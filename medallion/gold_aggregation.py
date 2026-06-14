"""
Gold Layer: Business-ready aggregates, SLA tracking, dbt-style final layer.
Production pattern — complete overwrite with SLA breach detection and alerting.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Final

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType

logger = logging.getLogger(__name__)

# SLA thresholds per metric family (seconds)
SLA_THRESHOLDS_SECONDS: Final[dict[str, float]] = {
    "daily_metrics": 900.0,   # 15 min
    "user_cohorts": 600.0,    # 10 min
    "revenue_summary": 300.0, # 5 min
}


@dataclass(frozen=True)
class GoldAggregationConfig:
    silver_table: str
    gold_table: str
    metric_family: str
    partition_by: list[str] = field(default_factory=lambda: ["event_date"])
    z_order_by: list[str] = field(default_factory=list)
    write_mode: str = "overwrite"  # or "merge" for incremental gold


@dataclass(frozen=True)
class SlaResult:
    metric_family: str
    started_at: datetime
    finished_at: datetime
    duration_seconds: float
    row_count: int
    sla_threshold_seconds: float
    breached: bool
    breach_margin_seconds: float


# ---------------------------------------------------------------------------
# Aggregation logic (dbt-style layered transforms)
# ---------------------------------------------------------------------------


def build_daily_metrics(df: DataFrame) -> DataFrame:
    """
    dbt-equivalent of mart_daily_metrics.sql.
    Aggregates events by date, user, and event type.
    """
    return (
        df.withColumn("event_date", F.to_date(F.col("event_ts")))
        .groupBy("event_date", "user_id", "event_type")
        .agg(
            F.count("*").alias("event_count").cast(LongType()),
            F.sum("revenue_amount").alias("total_revenue").cast(DoubleType()),
            F.avg("session_duration_seconds").alias("avg_session_duration_seconds").cast(DoubleType()),
            F.countDistinct("session_id").alias("unique_sessions").cast(LongType()),
            F.min("event_ts").alias("first_event_ts"),
            F.max("event_ts").alias("last_event_ts"),
        )
        .withColumn("_gold_processed_at", F.current_timestamp())
        .withColumn("_metric_family", F.lit("daily_metrics"))
    )


def build_user_cohorts(df: DataFrame) -> DataFrame:
    """
    User cohort aggregation: acquisition date × 7d / 30d retention signals.
    """
    first_seen = df.groupBy("user_id").agg(F.min("event_ts").alias("acquisition_ts"))
    first_seen = first_seen.withColumn("acquisition_date", F.to_date("acquisition_ts"))

    enriched = df.join(first_seen, on="user_id", how="left")

    return (
        enriched.withColumn("days_since_acquisition", F.datediff(F.to_date("event_ts"), F.col("acquisition_date")))
        .withColumn(
            "cohort_bucket",
            F.when(F.col("days_since_acquisition") == 0, "day_0")
            .when(F.col("days_since_acquisition").between(1, 7), "day_1_7")
            .when(F.col("days_since_acquisition").between(8, 30), "day_8_30")
            .otherwise("day_30_plus"),
        )
        .groupBy("acquisition_date", "cohort_bucket")
        .agg(
            F.countDistinct("user_id").alias("unique_users").cast(LongType()),
            F.sum("revenue_amount").alias("cohort_revenue").cast(DoubleType()),
        )
        .withColumn("_gold_processed_at", F.current_timestamp())
        .withColumn("_metric_family", F.lit("user_cohorts"))
    )


def build_revenue_summary(df: DataFrame) -> DataFrame:
    """
    High-level revenue summary — drives executive dashboards.
    """
    return (
        df.withColumn("event_date", F.to_date("event_ts"))
        .withColumn(
            "revenue_tier",
            F.when(F.col("revenue_amount") >= 1000, "enterprise")
            .when(F.col("revenue_amount") >= 100, "mid_market")
            .otherwise("smb"),
        )
        .groupBy("event_date", "revenue_tier", "product_line")
        .agg(
            F.sum("revenue_amount").alias("gross_revenue").cast(DoubleType()),
            F.count("*").alias("transaction_count").cast(LongType()),
            F.avg("revenue_amount").alias("avg_transaction_value").cast(DoubleType()),
            F.percentile_approx("revenue_amount", 0.5).alias("median_transaction_value").cast(DoubleType()),
            F.percentile_approx("revenue_amount", 0.95).alias("p95_transaction_value").cast(DoubleType()),
        )
        .withColumn("_gold_processed_at", F.current_timestamp())
        .withColumn("_metric_family", F.lit("revenue_summary"))
    )


AGGREGATION_REGISTRY: Final[dict[str, Any]] = {
    "daily_metrics": build_daily_metrics,
    "user_cohorts": build_user_cohorts,
    "revenue_summary": build_revenue_summary,
}


# ---------------------------------------------------------------------------
# SLA tracking
# ---------------------------------------------------------------------------


def track_sla(metric_family: str, start_ts: float, row_count: int) -> SlaResult:
    finished = time.time()
    duration = finished - start_ts
    threshold = SLA_THRESHOLDS_SECONDS.get(metric_family, 1800.0)
    breached = duration > threshold
    result = SlaResult(
        metric_family=metric_family,
        started_at=datetime.fromtimestamp(start_ts, tz=timezone.utc),
        finished_at=datetime.fromtimestamp(finished, tz=timezone.utc),
        duration_seconds=round(duration, 2),
        row_count=row_count,
        sla_threshold_seconds=threshold,
        breached=breached,
        breach_margin_seconds=round(duration - threshold, 2),
    )
    if breached:
        logger.error(
            "SLA BREACH — %s took %.1fs (threshold %.1fs, over by %.1fs)",
            metric_family,
            duration,
            threshold,
            duration - threshold,
        )
    else:
        logger.info(
            "SLA OK — %s completed in %.1fs / %.1fs threshold",
            metric_family,
            duration,
            threshold,
        )
    return result


# ---------------------------------------------------------------------------
# Write
# ---------------------------------------------------------------------------


def write_gold_table(df: DataFrame, config: GoldAggregationConfig) -> None:
    writer = df.write.format("delta").mode(config.write_mode).option("overwriteSchema", "true")

    if config.partition_by:
        writer = writer.partitionBy(*config.partition_by)

    writer.saveAsTable(config.gold_table)

    if config.z_order_by:
        z_cols = ", ".join(config.z_order_by)
        df.sparkSession.sql(f"OPTIMIZE {config.gold_table} ZORDER BY ({z_cols})")

    logger.info("Gold table %s written, partitions=%s, zorder=%s", config.gold_table, config.partition_by, config.z_order_by)


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


def run_gold_aggregation(config: GoldAggregationConfig, spark: SparkSession | None = None) -> dict[str, Any]:
    spark = spark or SparkSession.getActiveSession()
    if spark is None:
        raise RuntimeError("No active SparkSession")

    if config.metric_family not in AGGREGATION_REGISTRY:
        raise ValueError(f"Unknown metric_family {config.metric_family!r}. Valid: {list(AGGREGATION_REGISTRY)}")

    silver_df = spark.read.format("delta").table(config.silver_table)

    start_ts = time.time()
    agg_fn = AGGREGATION_REGISTRY[config.metric_family]
    gold_df = agg_fn(silver_df)

    row_count = gold_df.count()
    write_gold_table(gold_df, config)

    sla = track_sla(config.metric_family, start_ts, row_count)

    return {
        "metric_family": config.metric_family,
        "row_count": row_count,
        "duration_seconds": sla.duration_seconds,
        "sla_breached": sla.breached,
    }
