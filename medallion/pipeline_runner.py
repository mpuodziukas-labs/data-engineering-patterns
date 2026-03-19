"""
Pipeline Runner: Orchestrates Bronze → Silver → Gold with retry logic.
Production pattern — exponential backoff, stage-level isolation, metric emission.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any, Callable
from uuid import uuid4

from pyspark.sql import SparkSession

from medallion.bronze_ingestion import BronzeIngestionConfig, run_bronze_batch
from medallion.gold_aggregation import GoldAggregationConfig, run_gold_aggregation
from medallion.silver_transform import SilverTransformConfig, run_silver_transform

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int = 3
    base_delay_seconds: float = 5.0
    exponential_base: float = 2.0
    max_delay_seconds: float = 120.0


@dataclass(frozen=True)
class PipelineRunConfig:
    bronze: BronzeIngestionConfig
    silver: SilverTransformConfig
    gold: GoldAggregationConfig
    retry_policy: RetryPolicy = field(default_factory=RetryPolicy)
    pipeline_id: str = field(default_factory=lambda: str(uuid4()))


@dataclass
class StageResult:
    stage: str
    success: bool
    attempt: int
    duration_seconds: float
    metrics: dict[str, Any]
    error: str | None = None


def _compute_backoff(attempt: int, policy: RetryPolicy) -> float:
    delay = policy.base_delay_seconds * (policy.exponential_base ** (attempt - 1))
    return min(delay, policy.max_delay_seconds)


def run_with_retry(
    fn: Callable[[], dict[str, Any]],
    stage_name: str,
    policy: RetryPolicy,
) -> StageResult:
    """Run a stage function with exponential backoff retry. Returns StageResult regardless of outcome."""
    last_error: Exception | None = None

    for attempt in range(1, policy.max_attempts + 1):
        start = time.time()
        try:
            metrics = fn()
            duration = time.time() - start
            logger.info("[%s] attempt=%d SUCCESS in %.2fs", stage_name, attempt, duration)
            return StageResult(
                stage=stage_name,
                success=True,
                attempt=attempt,
                duration_seconds=round(duration, 2),
                metrics=metrics,
            )
        except Exception as exc:
            duration = time.time() - start
            last_error = exc
            logger.warning(
                "[%s] attempt=%d FAILED in %.2fs — %s",
                stage_name,
                attempt,
                duration,
                exc,
                exc_info=True,
            )
            if attempt < policy.max_attempts:
                backoff = _compute_backoff(attempt, policy)
                logger.info("[%s] retrying in %.1fs (backoff attempt %d/%d)", stage_name, backoff, attempt, policy.max_attempts)
                time.sleep(backoff)

    return StageResult(
        stage=stage_name,
        success=False,
        attempt=policy.max_attempts,
        duration_seconds=0.0,
        metrics={},
        error=str(last_error),
    )


def run_pipeline(config: PipelineRunConfig, spark: SparkSession | None = None) -> dict[str, Any]:
    """
    Execute Bronze → Silver → Gold in sequence.
    Each stage uses the retry policy.
    If any stage fails all retries, the pipeline halts and emits a structured failure report.
    """
    spark = spark or _build_spark()
    policy = config.retry_policy
    pipeline_start = time.time()

    logger.info("Pipeline %s started", config.pipeline_id)

    results: list[StageResult] = []

    # --- Bronze ---
    bronze_result = run_with_retry(
        lambda: run_bronze_batch(config.bronze, spark),
        "bronze",
        policy,
    )
    results.append(bronze_result)

    if not bronze_result.success:
        return _failure_report(config.pipeline_id, results, pipeline_start)

    # --- Silver ---
    silver_result = run_with_retry(
        lambda: run_silver_transform(config.silver, spark),
        "silver",
        policy,
    )
    results.append(silver_result)

    if not silver_result.success:
        return _failure_report(config.pipeline_id, results, pipeline_start)

    # --- Gold ---
    gold_result = run_with_retry(
        lambda: run_gold_aggregation(config.gold, spark),
        "gold",
        policy,
    )
    results.append(gold_result)

    total_duration = time.time() - pipeline_start

    if not gold_result.success:
        return _failure_report(config.pipeline_id, results, pipeline_start)

    report = {
        "pipeline_id": config.pipeline_id,
        "status": "SUCCESS",
        "total_duration_seconds": round(total_duration, 2),
        "stages": [
            {
                "stage": r.stage,
                "attempt": r.attempt,
                "duration_seconds": r.duration_seconds,
                "metrics": r.metrics,
            }
            for r in results
        ],
    }
    logger.info("Pipeline %s completed in %.2fs", config.pipeline_id, total_duration)
    return report


def _failure_report(pipeline_id: str, results: list[StageResult], start: float) -> dict[str, Any]:
    failed_stage = next((r for r in results if not r.success), None)
    return {
        "pipeline_id": pipeline_id,
        "status": "FAILED",
        "total_duration_seconds": round(time.time() - start, 2),
        "failed_stage": failed_stage.stage if failed_stage else "unknown",
        "error": failed_stage.error if failed_stage else "unknown",
        "stages": [
            {
                "stage": r.stage,
                "success": r.success,
                "attempt": r.attempt,
                "error": r.error,
            }
            for r in results
        ],
    }


def _build_spark() -> SparkSession:
    from medallion.bronze_ingestion import build_spark_session
    return build_spark_session("pipeline_runner")
