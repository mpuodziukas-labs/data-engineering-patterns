"""
Dead Letter Queue (DLQ): quarantine bad records + reprocess after fix.
Production pattern — no data loss, full audit trail, idempotent reprocessing.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

logger = logging.getLogger(__name__)


class DlqStatus(str, Enum):
    QUARANTINED = "QUARANTINED"
    REPROCESSING = "REPROCESSING"
    RESOLVED = "RESOLVED"
    UNRESOLVABLE = "UNRESOLVABLE"


DLQ_SCHEMA_EXTENSION = StructType(
    [
        StructField("dlq_id", StringType(), nullable=False),
        StructField("dlq_reason", StringType(), nullable=False),
        StructField("dlq_status", StringType(), nullable=False),
        StructField("dlq_quarantined_at", TimestampType(), nullable=False),
        StructField("dlq_resolved_at", TimestampType(), nullable=True),
        StructField("dlq_source_table", StringType(), nullable=False),
        StructField("dlq_batch_id", StringType(), nullable=True),
        StructField("dlq_reprocess_count", StringType(), nullable=False),  # StringType for Delta compatibility
    ]
)


@dataclass(frozen=True)
class DlqConfig:
    dlq_table: str
    source_table: str
    resolved_table: str
    max_reprocess_attempts: int = 3


# ---------------------------------------------------------------------------
# Quarantine
# ---------------------------------------------------------------------------


def quarantine_records(
    df: DataFrame,
    reason: str,
    source_table: str,
    batch_id: str | None = None,
) -> DataFrame:
    """
    Stamp records with DLQ metadata before writing to the quarantine table.
    Uses UUID per-row so each bad record has a traceable identity.
    """
    return (
        df.withColumn("dlq_id", F.expr("uuid()"))
        .withColumn("dlq_reason", F.lit(reason))
        .withColumn("dlq_status", F.lit(DlqStatus.QUARANTINED.value))
        .withColumn("dlq_quarantined_at", F.current_timestamp())
        .withColumn("dlq_resolved_at", F.lit(None).cast(TimestampType()))
        .withColumn("dlq_source_table", F.lit(source_table))
        .withColumn("dlq_batch_id", F.lit(batch_id or "unknown"))
        .withColumn("dlq_reprocess_count", F.lit("0"))
    )


def write_to_dlq(df: DataFrame, config: DlqConfig) -> int:
    """Write records to the DLQ table. Returns the count of quarantined records."""
    stamped_df = quarantine_records(df, reason="PIPELINE_REJECTION", source_table=config.source_table)
    count = stamped_df.count()
    if count > 0:
        stamped_df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(config.dlq_table)
        logger.warning("DLQ: quarantined %d records into %s", count, config.dlq_table)
    return count


# ---------------------------------------------------------------------------
# Reprocessing
# ---------------------------------------------------------------------------


def load_quarantined_records(spark: SparkSession, config: DlqConfig, limit: int | None = None) -> DataFrame:
    """
    Load records in QUARANTINED status for reprocessing.
    Excludes records that have exceeded max retry attempts.
    """
    df = (
        spark.read.format("delta")
        .table(config.dlq_table)
        .filter(F.col("dlq_status") == DlqStatus.QUARANTINED.value)
        .filter(F.col("dlq_reprocess_count").cast("int") < config.max_reprocess_attempts)
    )
    if limit is not None:
        df = df.limit(limit)
    return df


def mark_as_reprocessing(spark: SparkSession, config: DlqConfig, dlq_ids: list[str]) -> None:
    """Mark a set of DLQ records as currently being reprocessed (optimistic locking)."""
    from delta.tables import DeltaTable

    id_list = ", ".join(f"'{id_}'" for id_ in dlq_ids)
    dlq_dt = DeltaTable.forName(spark, config.dlq_table)
    dlq_dt.update(
        condition=F.expr(f"dlq_id IN ({id_list})"),
        set={"dlq_status": F.lit(DlqStatus.REPROCESSING.value)},
    )


def mark_as_resolved(spark: SparkSession, config: DlqConfig, dlq_ids: list[str]) -> None:
    from delta.tables import DeltaTable

    id_list = ", ".join(f"'{id_}'" for id_ in dlq_ids)
    dlq_dt = DeltaTable.forName(spark, config.dlq_table)
    dlq_dt.update(
        condition=F.expr(f"dlq_id IN ({id_list})"),
        set={
            "dlq_status": F.lit(DlqStatus.RESOLVED.value),
            "dlq_resolved_at": F.current_timestamp(),
        },
    )


def mark_as_unresolvable(spark: SparkSession, config: DlqConfig, dlq_ids: list[str]) -> None:
    from delta.tables import DeltaTable

    id_list = ", ".join(f"'{id_}'" for id_ in dlq_ids)
    dlq_dt = DeltaTable.forName(spark, config.dlq_table)
    dlq_dt.update(
        condition=F.expr(f"dlq_id IN ({id_list})"),
        set={"dlq_status": F.lit(DlqStatus.UNRESOLVABLE.value)},
    )


def increment_reprocess_count(spark: SparkSession, config: DlqConfig, dlq_ids: list[str]) -> None:
    from delta.tables import DeltaTable

    id_list = ", ".join(f"'{id_}'" for id_ in dlq_ids)
    dlq_dt = DeltaTable.forName(spark, config.dlq_table)
    dlq_dt.update(
        condition=F.expr(f"dlq_id IN ({id_list})"),
        set={"dlq_reprocess_count": F.expr("CAST(CAST(dlq_reprocess_count AS INT) + 1 AS STRING)")},
    )


# ---------------------------------------------------------------------------
# Reprocess pipeline
# ---------------------------------------------------------------------------


def reprocess_dlq(
    spark: SparkSession,
    config: DlqConfig,
    fix_fn: Callable[[DataFrame], DataFrame],
    batch_size: int = 1000,
) -> dict[str, Any]:
    """
    Reprocess DLQ records using a user-provided fix function.

    fix_fn: takes a DataFrame of bad records and returns a fixed DataFrame.
            Records that cannot be fixed should be excluded (filter them out).
            The returned DataFrame is written to config.resolved_table.

    Returns summary: {attempted, resolved, unresolvable}.
    """
    quarantined_df = load_quarantined_records(spark, config, limit=batch_size)
    total = quarantined_df.count()

    if total == 0:
        logger.info("DLQ: no records to reprocess")
        return {"attempted": 0, "resolved": 0, "unresolvable": 0}

    dlq_ids = [row["dlq_id"] for row in quarantined_df.select("dlq_id").collect()]
    mark_as_reprocessing(spark, config, dlq_ids)
    increment_reprocess_count(spark, config, dlq_ids)

    try:
        fixed_df = fix_fn(quarantined_df)
        resolved_ids = [row["dlq_id"] for row in fixed_df.select("dlq_id").collect()]
        failed_ids = [id_ for id_ in dlq_ids if id_ not in set(resolved_ids)]

        if resolved_ids:
            # Strip DLQ metadata and write resolved records to target table
            clean_df = fixed_df.drop(
                "dlq_id", "dlq_reason", "dlq_status", "dlq_quarantined_at",
                "dlq_resolved_at", "dlq_source_table", "dlq_batch_id", "dlq_reprocess_count"
            )
            clean_df.write.format("delta").mode("append").saveAsTable(config.resolved_table)
            mark_as_resolved(spark, config, resolved_ids)

        if failed_ids:
            # Check if they've exceeded max attempts
            exceeded_df = (
                quarantined_df.filter(F.col("dlq_id").isin(failed_ids))
                .filter(F.col("dlq_reprocess_count").cast("int") >= config.max_reprocess_attempts)
            )
            exceeded_ids = [row["dlq_id"] for row in exceeded_df.select("dlq_id").collect()]
            if exceeded_ids:
                mark_as_unresolvable(spark, config, exceeded_ids)
            else:
                # Reset to QUARANTINED for retry next cycle
                from delta.tables import DeltaTable
                id_list = ", ".join(f"'{id_}'" for id_ in failed_ids)
                dlq_dt = DeltaTable.forName(spark, config.dlq_table)
                dlq_dt.update(
                    condition=F.expr(f"dlq_id IN ({id_list})"),
                    set={"dlq_status": F.lit(DlqStatus.QUARANTINED.value)},
                )

        result = {
            "attempted": total,
            "resolved": len(resolved_ids),
            "unresolvable": len([id_ for id_ in failed_ids if id_ in (
                exceeded_ids if failed_ids else []
            )]),
        }
        logger.info("DLQ reprocessing: %s", result)
        return result

    except Exception as exc:
        # Revert status to QUARANTINED on unexpected failure
        logger.error("DLQ reprocessing failed: %s", exc, exc_info=True)
        mark_as_reprocessing(spark, config, dlq_ids)  # Will be retried next cycle
        raise


# ---------------------------------------------------------------------------
# DLQ stats
# ---------------------------------------------------------------------------


def get_dlq_stats(spark: SparkSession, config: DlqConfig) -> dict[str, Any]:
    """Summary statistics for the DLQ table — useful for monitoring dashboards."""
    df = spark.read.format("delta").table(config.dlq_table)
    stats = df.groupBy("dlq_status", "dlq_reason").count().collect()
    return {
        "dlq_table": config.dlq_table,
        "breakdown": [{"status": r["dlq_status"], "reason": r["dlq_reason"], "count": r["count"]} for r in stats],
        "total": df.count(),
    }
