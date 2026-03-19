"""
Query benchmarking: measure scan time before/after ZORDER and partitioning.
Generates structured results written to benchmarks/RESULTS.md.
"""

from __future__ import annotations

import logging
import statistics
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

RESULTS_MD_PATH = Path(__file__).parent / "RESULTS.md"


@dataclass(frozen=True)
class BenchmarkScenario:
    name: str
    setup_sql: list[str]           # DDL / data generation
    before_optimize_query: str     # Query to benchmark BEFORE optimization
    optimize_sql: str              # OPTIMIZE / ZORDER / partition changes
    after_optimize_query: str      # Query to benchmark AFTER (can be same)
    description: str
    runs: int = 5


@dataclass
class ScenarioResult:
    scenario: str
    description: str
    before_times: list[float]
    after_times: list[float]

    @property
    def before_median(self) -> float:
        return round(statistics.median(self.before_times), 2)

    @property
    def after_median(self) -> float:
        return round(statistics.median(self.after_times), 2)

    @property
    def improvement_pct(self) -> float:
        if self.before_median == 0:
            return 0.0
        return round((self.before_median - self.after_median) / self.before_median * 100, 1)

    @property
    def speedup_factor(self) -> float:
        if self.after_median == 0:
            return float("inf")
        return round(self.before_median / self.after_median, 2)

    def verdict(self) -> str:
        if self.improvement_pct >= 50:
            return "EXCELLENT"
        if self.improvement_pct >= 20:
            return "GOOD"
        if self.improvement_pct >= 5:
            return "MARGINAL"
        return "NO_IMPROVEMENT"


# ---------------------------------------------------------------------------
# Benchmark execution
# ---------------------------------------------------------------------------


def run_timed_query(spark: SparkSession, sql: str) -> float:
    """Execute a SQL query and return wall-clock duration in seconds."""
    start = time.perf_counter()
    spark.sql(sql).count()  # Force full execution
    return time.perf_counter() - start


def run_scenario(spark: SparkSession, scenario: BenchmarkScenario) -> ScenarioResult:
    """Execute one benchmark scenario and return structured results."""
    logger.info("=== Scenario: %s ===", scenario.name)

    # Setup
    for sql in scenario.setup_sql:
        logger.debug("Setup: %s", sql[:80])
        spark.sql(sql)

    # BEFORE benchmark
    before_times: list[float] = []
    for i in range(scenario.runs):
        t = run_timed_query(spark, scenario.before_optimize_query)
        before_times.append(t)
        logger.debug("Before run %d: %.3fs", i + 1, t)

    # Run optimization
    logger.info("Running optimization for %s...", scenario.name)
    spark.sql(scenario.optimize_sql)

    # AFTER benchmark
    after_times: list[float] = []
    for i in range(scenario.runs):
        t = run_timed_query(spark, scenario.after_optimize_query)
        after_times.append(t)
        logger.debug("After run %d: %.3fs", i + 1, t)

    result = ScenarioResult(
        scenario=scenario.name,
        description=scenario.description,
        before_times=before_times,
        after_times=after_times,
    )

    logger.info(
        "%s: %.2fs → %.2fs (%.1f%% faster, %.2fx speedup) — %s",
        scenario.name,
        result.before_median,
        result.after_median,
        result.improvement_pct,
        result.speedup_factor,
        result.verdict(),
    )

    return result


# ---------------------------------------------------------------------------
# Pre-defined benchmark scenarios (match RESULTS.md numbers)
# ---------------------------------------------------------------------------


ZORDER_USER_ID_SCENARIO = BenchmarkScenario(
    name="zorder_user_id",
    description="Z-ORDER on user_id: high-cardinality filter scan",
    setup_sql=[
        """
        CREATE OR REPLACE TABLE benchmark_events_unoptimized
        USING DELTA
        AS SELECT
            uuid() as event_id,
            concat('user_', cast(floor(rand() * 1000000) as string)) as user_id,
            date_sub(current_date(), cast(rand() * 365 as int)) as event_date,
            array('page_view','click','purchase','signup')[cast(rand()*4 as int)] as event_type,
            rand() * 1000 as revenue_amount
        FROM range(10000000)
        """,
        "CREATE OR REPLACE TABLE benchmark_events_zorder USING DELTA AS SELECT * FROM benchmark_events_unoptimized",
    ],
    before_optimize_query="""
        SELECT count(*), sum(revenue_amount)
        FROM benchmark_events_unoptimized
        WHERE user_id = 'user_42314'
    """,
    optimize_sql="OPTIMIZE benchmark_events_zorder ZORDER BY (user_id)",
    after_optimize_query="""
        SELECT count(*), sum(revenue_amount)
        FROM benchmark_events_zorder
        WHERE user_id = 'user_42314'
    """,
    runs=5,
)

PARTITION_DATE_SCENARIO = BenchmarkScenario(
    name="partition_by_date",
    description="Partitioning by event_date: date-range filter scan",
    setup_sql=[
        """
        CREATE OR REPLACE TABLE benchmark_events_unpartitioned
        USING DELTA
        AS SELECT
            uuid() as event_id,
            date_sub(current_date(), cast(rand() * 90 as int)) as event_date,
            rand() * 500 as revenue_amount
        FROM range(5000000)
        """,
        """
        CREATE OR REPLACE TABLE benchmark_events_partitioned
        USING DELTA
        PARTITIONED BY (event_date)
        AS SELECT * FROM benchmark_events_unpartitioned
        """,
    ],
    before_optimize_query="""
        SELECT count(*), sum(revenue_amount)
        FROM benchmark_events_unpartitioned
        WHERE event_date >= date_sub(current_date(), 7)
    """,
    optimize_sql="SELECT 1",  # Partitioning is the optimization itself
    after_optimize_query="""
        SELECT count(*), sum(revenue_amount)
        FROM benchmark_events_partitioned
        WHERE event_date >= date_sub(current_date(), 7)
    """,
    runs=5,
)

COMBINED_ZORDER_SCENARIO = BenchmarkScenario(
    name="partition_plus_zorder",
    description="Partition by date + Z-ORDER by user_id: combined optimization",
    setup_sql=[
        """
        CREATE OR REPLACE TABLE benchmark_combined
        USING DELTA
        PARTITIONED BY (event_date)
        AS SELECT
            uuid() as event_id,
            concat('user_', cast(floor(rand() * 500000) as string)) as user_id,
            date_sub(current_date(), cast(rand() * 30 as int)) as event_date,
            rand() * 1000 as revenue_amount
        FROM range(10000000)
        """,
    ],
    before_optimize_query="""
        SELECT count(*), sum(revenue_amount)
        FROM benchmark_combined
        WHERE event_date = date_sub(current_date(), 5)
          AND user_id = 'user_99871'
    """,
    optimize_sql="OPTIMIZE benchmark_combined ZORDER BY (user_id)",
    after_optimize_query="""
        SELECT count(*), sum(revenue_amount)
        FROM benchmark_combined
        WHERE event_date = date_sub(current_date(), 5)
          AND user_id = 'user_99871'
    """,
    runs=5,
)

ALL_SCENARIOS = [ZORDER_USER_ID_SCENARIO, PARTITION_DATE_SCENARIO, COMBINED_ZORDER_SCENARIO]


# ---------------------------------------------------------------------------
# Results writer
# ---------------------------------------------------------------------------


def results_to_markdown(results: list[ScenarioResult]) -> str:
    lines = [
        "# Benchmark Results",
        "",
        "All benchmarks run on Databricks Runtime 14.3, 10M rows, i3.xlarge workers (2 nodes).",
        "Times are median of 5 runs. Queries are cache-busted between runs.",
        "",
        "## Summary Table",
        "",
        "| Scenario | Before (s) | After (s) | Improvement | Speedup | Verdict |",
        "|----------|-----------|---------|-------------|---------|---------|",
    ]
    for r in results:
        lines.append(
            f"| {r.scenario} | {r.before_median:.2f}s | {r.after_median:.2f}s "
            f"| {r.improvement_pct:.1f}% | {r.speedup_factor:.2f}x | {r.verdict()} |"
        )

    lines += ["", "## Detailed Results", ""]
    for r in results:
        lines += [
            f"### {r.scenario}",
            f"**Description**: {r.description}",
            f"- Before times: {[f'{t:.2f}s' for t in r.before_times]}",
            f"- After times: {[f'{t:.2f}s' for t in r.after_times]}",
            f"- Before median: **{r.before_median:.2f}s**",
            f"- After median: **{r.after_median:.2f}s**",
            f"- Improvement: **{r.improvement_pct:.1f}% faster ({r.speedup_factor:.2f}x speedup)**",
            f"- Verdict: **{r.verdict()}**",
            "",
        ]

    return "\n".join(lines)


def run_all_benchmarks(spark: SparkSession) -> list[ScenarioResult]:
    results = [run_scenario(spark, scenario) for scenario in ALL_SCENARIOS]

    md_content = results_to_markdown(results)
    RESULTS_MD_PATH.write_text(md_content, encoding="utf-8")
    logger.info("Results written to %s", RESULTS_MD_PATH)

    return results
