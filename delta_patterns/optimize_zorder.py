"""
Delta Lake OPTIMIZE + ZORDER BY with benchmark before/after.
Production pattern — data skipping that actually eliminates file scans.
See benchmarks/RESULTS.md for measured outcomes.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class OptimizeConfig:
    table_name: str
    z_order_columns: list[str]
    benchmark_query: str          # SQL query used to measure improvement
    benchmark_filter: str         # WHERE clause applied to benchmark query
    partition_predicate: str | None = None  # Optional: OPTIMIZE WHERE partition_col='...'


@dataclass(frozen=True)
class BenchmarkResult:
    label: str
    duration_seconds: float
    files_scanned: int | None
    rows_returned: int


# ---------------------------------------------------------------------------
# Benchmark helpers
# ---------------------------------------------------------------------------


def _run_benchmark_query(spark: SparkSession, query: str, label: str, runs: int = 3) -> BenchmarkResult:
    """
    Run a query N times and return the MEDIAN duration.
    Median avoids JVM warm-up noise better than mean.
    """
    durations: list[float] = []
    row_count = 0

    for i in range(runs):
        start = time.time()
        result_df = spark.sql(query)
        row_count = result_df.count()  # Force full execution
        durations.append(time.time() - start)
        logger.debug("%s run %d: %.3fs", label, i + 1, durations[-1])

    durations.sort()
    median = durations[len(durations) // 2]
    logger.info("%s median over %d runs: %.3fs (%d rows)", label, runs, median, row_count)

    return BenchmarkResult(
        label=label,
        duration_seconds=round(median, 3),
        files_scanned=None,  # Would require Spark UI metrics in real Databricks
        rows_returned=row_count,
    )


def get_table_stats(spark: SparkSession, table_name: str) -> dict[str, Any]:
    """Get file count and size distribution for a Delta table."""
    detail_df = spark.sql(f"DESCRIBE DETAIL {table_name}")
    row = detail_df.first()
    if row is None:
        return {}
    return {
        "num_files": row["numFiles"],
        "size_in_bytes": row["sizeInBytes"],
        "table_name": table_name,
    }


# ---------------------------------------------------------------------------
# OPTIMIZE + ZORDER
# ---------------------------------------------------------------------------


def run_optimize(spark: SparkSession, config: OptimizeConfig) -> dict[str, Any]:
    """
    Run OPTIMIZE with optional partition predicate.
    Returns Spark metrics from the OPTIMIZE command.
    """
    if config.partition_predicate:
        sql = f"OPTIMIZE {config.table_name} WHERE {config.partition_predicate} ZORDER BY ({', '.join(config.z_order_columns)})"
    else:
        sql = f"OPTIMIZE {config.table_name} ZORDER BY ({', '.join(config.z_order_columns)})"

    logger.info("Running: %s", sql)
    result_df = spark.sql(sql)
    metrics_row = result_df.first()

    metrics: dict[str, Any] = {}
    if metrics_row:
        metrics = metrics_row.asDict()

    logger.info("OPTIMIZE complete — metrics: %s", metrics)
    return metrics


# ---------------------------------------------------------------------------
# Before/After benchmark
# ---------------------------------------------------------------------------


def benchmark_zorder(spark: SparkSession, config: OptimizeConfig) -> dict[str, Any]:
    """
    Full benchmark cycle:
      1. Measure query time BEFORE optimize
      2. Run OPTIMIZE ZORDER BY
      3. Measure query time AFTER optimize
      4. Compute improvement percentage

    Returns structured report suitable for RESULTS.md.
    """
    logger.info("=== ZORDER Benchmark: %s ===", config.table_name)

    # Stats before
    stats_before = get_table_stats(spark, config.table_name)
    logger.info("Before — files: %s, bytes: %s", stats_before.get("num_files"), stats_before.get("size_in_bytes"))

    # Benchmark BEFORE
    before = _run_benchmark_query(spark, config.benchmark_query, label="BEFORE_ZORDER")

    # Run OPTIMIZE
    optimize_start = time.time()
    optimize_metrics = run_optimize(spark, config)
    optimize_duration = round(time.time() - optimize_start, 2)

    # Stats after
    stats_after = get_table_stats(spark, config.table_name)

    # Benchmark AFTER
    after = _run_benchmark_query(spark, config.benchmark_query, label="AFTER_ZORDER")

    # Compute improvement
    improvement_pct = round((before.duration_seconds - after.duration_seconds) / before.duration_seconds * 100, 1)
    speedup_factor = round(before.duration_seconds / after.duration_seconds, 2) if after.duration_seconds > 0 else float("inf")

    report = {
        "table": config.table_name,
        "z_order_columns": config.z_order_columns,
        "before": {
            "query_median_seconds": before.duration_seconds,
            "file_count": stats_before.get("num_files"),
            "size_bytes": stats_before.get("size_in_bytes"),
        },
        "optimize_duration_seconds": optimize_duration,
        "optimize_metrics": optimize_metrics,
        "after": {
            "query_median_seconds": after.duration_seconds,
            "file_count": stats_after.get("num_files"),
            "size_bytes": stats_after.get("size_in_bytes"),
        },
        "improvement_pct": improvement_pct,
        "speedup_factor": speedup_factor,
        "verdict": "BENEFICIAL" if improvement_pct >= 20 else "MARGINAL" if improvement_pct >= 5 else "NO_IMPROVEMENT",
    }

    logger.info(
        "ZORDER result: %.1fs → %.1fs (%.1f%% faster, %.2fx speedup)",
        before.duration_seconds,
        after.duration_seconds,
        improvement_pct,
        speedup_factor,
    )

    return report


# ---------------------------------------------------------------------------
# Partition-aware optimize for large tables
# ---------------------------------------------------------------------------


def optimize_by_partition(
    spark: SparkSession,
    table_name: str,
    partition_col: str,
    partition_values: list[str],
    z_order_cols: list[str],
) -> list[dict[str, Any]]:
    """
    Run partition-level OPTIMIZE to avoid full-table compaction overhead.
    Useful for tables >1TB — only compact recently-written partitions.
    """
    results: list[dict[str, Any]] = []

    for partition_value in partition_values:
        config = OptimizeConfig(
            table_name=table_name,
            z_order_columns=z_order_cols,
            benchmark_query=f"SELECT COUNT(*) FROM {table_name} WHERE {partition_col} = '{partition_value}'",
            benchmark_filter=f"{partition_col} = '{partition_value}'",
            partition_predicate=f"{partition_col} = '{partition_value}'",
        )
        try:
            metrics = run_optimize(spark, config)
            results.append({"partition_value": partition_value, "success": True, "metrics": metrics})
        except Exception as exc:
            logger.error("OPTIMIZE failed for partition %s=%s: %s", partition_col, partition_value, exc)
            results.append({"partition_value": partition_value, "success": False, "error": str(exc)})

    return results
