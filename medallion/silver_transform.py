"""
Silver Layer: Cleaning, deduplication, type casting, Great Expectations checks.
Production pattern — all transformations are idempotent merge-on-key writes.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Final

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DataType, DoubleType, IntegerType, LongType, StringType, TimestampType

logger = logging.getLogger(__name__)

# Map of common string patterns → target types for auto-casting
AUTO_CAST_RULES: Final[dict[str, DataType]] = {
    "_ts": TimestampType(),
    "_at": TimestampType(),
    "_date": TimestampType(),
    "_id": StringType(),
    "_count": LongType(),
    "_amount": DoubleType(),
    "_price": DoubleType(),
    "_qty": IntegerType(),
}


@dataclass(frozen=True)
class SilverTransformConfig:
    bronze_table: str
    silver_table: str
    dedup_key: str
    order_by_col: str
    merge_key: str
    null_threshold: float = 0.20  # Fail GX check if any column exceeds 20% nulls
    row_count_min: int = 1
    expected_columns: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Cleaning
# ---------------------------------------------------------------------------


def drop_duplicates_keep_latest(df: DataFrame, dedup_key: str, order_by_col: str) -> DataFrame:
    """Keep the most recent record per dedup_key. Deterministic via dense_rank."""
    window = F.expr(f"ROW_NUMBER() OVER (PARTITION BY {dedup_key} ORDER BY {order_by_col} DESC)")
    return df.withColumn("_rn", window).filter(F.col("_rn") == 1).drop("_rn")


def apply_auto_casts(df: DataFrame) -> DataFrame:
    """Attempt type coercion on columns matching suffix patterns. Silently skip if cast fails."""
    for col_name in df.columns:
        for suffix, target_type in AUTO_CAST_RULES.items():
            if col_name.lower().endswith(suffix):
                df = df.withColumn(col_name, F.col(col_name).cast(target_type))
                break
    return df


def normalize_strings(df: DataFrame, columns: list[str]) -> DataFrame:
    """Trim whitespace, lowercase, replace empty string with NULL."""
    for col_name in columns:
        if col_name in df.columns:
            df = df.withColumn(
                col_name,
                F.when(F.trim(F.col(col_name)) == "", None).otherwise(F.lower(F.trim(F.col(col_name)))),
            )
    return df


def add_silver_metadata(df: DataFrame) -> DataFrame:
    return df.withColumn("_silver_processed_at", F.current_timestamp()).withColumn("_silver_version", F.lit(1).cast(IntegerType()))


# ---------------------------------------------------------------------------
# Great Expectations-style checks (lightweight, no GX daemon required)
# ---------------------------------------------------------------------------


def validate_not_null(df: DataFrame, column: str) -> dict[str, Any]:
    total = df.count()
    null_count = df.filter(F.col(column).isNull()).count()
    null_pct = null_count / total if total > 0 else 0.0
    return {
        "expectation": "expect_column_values_to_not_be_null",
        "column": column,
        "null_count": null_count,
        "null_pct": round(null_pct, 4),
        "passed": null_pct < 0.01,  # Silver must be <1% null on key columns
    }


def validate_row_count(df: DataFrame, minimum: int) -> dict[str, Any]:
    count = df.count()
    return {
        "expectation": "expect_table_row_count_to_be_between",
        "row_count": count,
        "minimum": minimum,
        "passed": count >= minimum,
    }


def validate_columns_exist(df: DataFrame, expected: list[str]) -> dict[str, Any]:
    missing = [c for c in expected if c not in df.columns]
    return {
        "expectation": "expect_table_columns_to_match_set",
        "missing_columns": missing,
        "passed": len(missing) == 0,
    }


def run_great_expectations_suite(df: DataFrame, config: SilverTransformConfig) -> list[dict[str, Any]]:
    """Run all GX-style checks. Raises on any failure so the pipeline halts before writing bad data."""
    results: list[dict[str, Any]] = []

    results.append(validate_row_count(df, config.row_count_min))

    if config.expected_columns:
        results.append(validate_columns_exist(df, config.expected_columns))

    # Null threshold check on all columns
    total = df.count()
    for col_name in df.columns:
        if col_name.startswith("_"):
            continue  # Skip metadata columns
        null_count = df.filter(F.col(col_name).isNull()).count()
        null_pct = null_count / total if total > 0 else 0.0
        result = {
            "expectation": "expect_column_null_pct_below_threshold",
            "column": col_name,
            "null_pct": round(null_pct, 4),
            "threshold": config.null_threshold,
            "passed": null_pct <= config.null_threshold,
        }
        results.append(result)

    failed = [r for r in results if not r["passed"]]
    if failed:
        raise ValueError(f"Great Expectations suite FAILED — {len(failed)} checks: {failed}")

    logger.info("GX suite passed — %d checks all green", len(results))
    return results


# ---------------------------------------------------------------------------
# Write
# ---------------------------------------------------------------------------


def upsert_to_silver(df: DataFrame, config: SilverTransformConfig, spark: SparkSession) -> None:
    """
    Idempotent merge: if the silver table exists, MERGE on merge_key.
    If the table does not exist yet, CREATE it.
    """
    if DeltaTable.isDeltaTable(spark, config.silver_table):
        silver_dt = DeltaTable.forName(spark, config.silver_table)
        silver_dt.alias("target").merge(
            df.alias("source"),
            f"target.{config.merge_key} = source.{config.merge_key}",
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        logger.info("MERGE into silver table %s complete", config.silver_table)
    else:
        df.write.format("delta").mode("overwrite").saveAsTable(config.silver_table)
        logger.info("Created silver table %s", config.silver_table)


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


def run_silver_transform(config: SilverTransformConfig, spark: SparkSession | None = None) -> dict[str, Any]:
    spark = spark or SparkSession.getActiveSession()
    if spark is None:
        raise RuntimeError("No active SparkSession — call build_spark_session() first")

    raw_df = spark.read.format("delta").table(config.bronze_table)

    deduped_df = drop_duplicates_keep_latest(raw_df, config.dedup_key, config.order_by_col)
    cast_df = apply_auto_casts(deduped_df)
    string_cols = [f.name for f in cast_df.schema.fields if isinstance(f.dataType, StringType) and not f.name.startswith("_")]
    cleaned_df = normalize_strings(cast_df, string_cols)
    final_df = add_silver_metadata(cleaned_df)

    gx_results = run_great_expectations_suite(final_df, config)

    upsert_to_silver(final_df, config, spark)

    return {"gx_checks": len(gx_results), "row_count": final_df.count()}
