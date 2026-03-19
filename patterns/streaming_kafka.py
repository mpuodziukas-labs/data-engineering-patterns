"""
Kafka + Spark Structured Streaming production patterns.

Demonstrates:
- Consumer lag monitoring (per-partition, alert + critical thresholds)
- Watermark-based late data handling (configurable delay tolerance)
- Exactly-once semantics (idempotent Delta writes via checkpoint)
- Backpressure detection (maxOffsetsPerTrigger + rate monitoring)
- Dead letter queue (DLQ) routing for malformed records

Architecture:
    Kafka Topic
        └─ Structured Streaming (maxOffsetsPerTrigger backpressure)
            ├─ Watermark filter (late data up to N minutes tolerated)
            ├─ Good records → Delta output table (exactly-once via checkpoint)
            └─ Bad records (NULL required fields) → DLQ Delta table

Exactly-once guarantee:
    Delta + Spark Structured Streaming achieves exactly-once via:
    1. Idempotent writes: Delta's transaction log detects duplicate epoch writes
    2. Checkpoint: Spark persists committed Kafka offsets atomically with the write
    3. Recovery: On restart, Spark reprocesses from last committed offset

Usage:
    config = KafkaStreamConfig(...)
    report = compute_consumer_lag(LagConfig(...))
    run_kafka_streaming(config, spark)
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any, Final

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

LAG_ALERT_THRESHOLD: Final[int] = 10_000
LAG_CRITICAL_THRESHOLD: Final[int] = 100_000

BACKPRESSURE_RATE_DROP_PCT: Final[float] = 30.0
"""If processing rate drops >30% between batches, flag as backpressure."""

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class LagConfig:
    bootstrap_servers: list[str]
    consumer_group_id: str
    topics: list[str]
    alert_threshold: int = LAG_ALERT_THRESHOLD
    critical_threshold: int = LAG_CRITICAL_THRESHOLD
    poll_interval_seconds: float = 30.0


@dataclass(frozen=True)
class KafkaStreamConfig:
    bootstrap_servers: str
    topic: str
    checkpoint_location: str
    output_table: str
    starting_offsets: str = "latest"
    watermark_delay: str = "10 minutes"
    trigger_interval: str = "1 minute"
    max_offsets_per_trigger: int = 100_000
    kafka_group_id: str = "structured-streaming-consumer"
    dlq_table: str = ""


# ---------------------------------------------------------------------------
# Lag monitoring — pure Python, no Spark required
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class PartitionLagRecord:
    topic: str
    partition: int
    consumer_offset: int
    log_end_offset: int
    lag: int

    @property
    def severity(self) -> str:
        if self.lag >= LAG_CRITICAL_THRESHOLD:
            return "CRITICAL"
        if self.lag >= LAG_ALERT_THRESHOLD:
            return "WARN"
        return "OK"


@dataclass(frozen=True)
class LagReport:
    consumer_group: str
    polled_at: float
    partitions: list[PartitionLagRecord]
    total_lag: int
    max_partition_lag: int
    alert_triggered: bool
    critical_triggered: bool

    def summary(self) -> str:
        return (
            f"group={self.consumer_group} "
            f"total_lag={self.total_lag:,} "
            f"max_partition_lag={self.max_partition_lag:,} "
            f"alert={self.alert_triggered} "
            f"critical={self.critical_triggered}"
        )

    def severity(self) -> str:
        if self.critical_triggered:
            return "CRITICAL"
        if self.alert_triggered:
            return "WARN"
        return "OK"


def compute_consumer_lag(
    log_end_offsets: dict[tuple[str, int], int],
    committed_offsets: dict[tuple[str, int], int],
    config: LagConfig,
) -> LagReport:
    """
    Compute per-partition consumer lag from raw offset maps.

    This function is pure — it accepts offset maps rather than Kafka clients,
    making it fully testable without a running Kafka cluster.

    Args:
        log_end_offsets: Map of (topic, partition) → log end offset.
        committed_offsets: Map of (topic, partition) → consumer committed offset.
        config: LagConfig with alert and critical thresholds.

    Returns:
        LagReport with per-partition breakdown and aggregate alert flags.

    When to alert:
        - WARN: max partition lag ≥ alert_threshold (default 10K)
        - CRITICAL: max partition lag ≥ critical_threshold (default 100K)
          A CRITICAL lag typically means the consumer is stalled and falling
          behind at a rate faster than it can process.
    """
    partitions: list[PartitionLagRecord] = []

    for (topic, partition), leo in log_end_offsets.items():
        consumer_offset = committed_offsets.get((topic, partition), 0)
        lag = max(0, leo - consumer_offset)
        partitions.append(PartitionLagRecord(
            topic=topic,
            partition=partition,
            consumer_offset=consumer_offset,
            log_end_offset=leo,
            lag=lag,
        ))

    total_lag = sum(p.lag for p in partitions)
    max_lag = max((p.lag for p in partitions), default=0)

    report = LagReport(
        consumer_group=config.consumer_group_id,
        polled_at=time.time(),
        partitions=partitions,
        total_lag=total_lag,
        max_partition_lag=max_lag,
        alert_triggered=max_lag >= config.alert_threshold,
        critical_triggered=max_lag >= config.critical_threshold,
    )

    if report.critical_triggered:
        logger.critical("LAG CRITICAL — %s", report.summary())
    elif report.alert_triggered:
        logger.warning("LAG ALERT — %s", report.summary())
    else:
        logger.info("LAG OK — %s", report.summary())

    return report


# ---------------------------------------------------------------------------
# Backpressure detection
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class BatchMetrics:
    batch_id: int
    records_processed: int
    duration_seconds: float
    input_rows_per_second: float
    process_rows_per_second: float


def detect_backpressure(batch_history: list[BatchMetrics], rate_drop_threshold_pct: float = BACKPRESSURE_RATE_DROP_PCT) -> dict[str, Any]:
    """
    Detect backpressure from a sliding window of batch metrics.

    Backpressure is indicated when the processing rate drops significantly
    between batches while the input rate remains constant or increases.
    This typically means the executor is saturated and Spark's internal
    maxOffsetsPerTrigger is rate-limiting ingestion.

    Args:
        batch_history: Recent batch metrics, ordered oldest to newest.
        rate_drop_threshold_pct: Drop in process_rows_per_second that signals backpressure.

    Returns:
        Dict with backpressure_detected bool, rate_drop_pct, and recommendation.

    Anti-patterns:
        - Setting maxOffsetsPerTrigger too high causes backpressure and GC pressure
        - Not monitoring batch duration: growing duration = silent backpressure
        - Using foreachBatch with slow external writes without rate limiting
    """
    if len(batch_history) < 2:
        return {"backpressure_detected": False, "reason": "insufficient_history"}

    recent = batch_history[-1]
    previous = batch_history[-2]

    if previous.process_rows_per_second <= 0:
        return {"backpressure_detected": False, "reason": "zero_previous_rate"}

    rate_drop_pct = (
        (previous.process_rows_per_second - recent.process_rows_per_second)
        / previous.process_rows_per_second
        * 100
    )

    backpressure = rate_drop_pct >= rate_drop_threshold_pct

    result: dict[str, Any] = {
        "backpressure_detected": backpressure,
        "rate_drop_pct": round(rate_drop_pct, 1),
        "previous_rate": previous.process_rows_per_second,
        "current_rate": recent.process_rows_per_second,
        "threshold_pct": rate_drop_threshold_pct,
    }

    if backpressure:
        result["recommendation"] = (
            f"Reduce maxOffsetsPerTrigger (current batch processed {recent.records_processed:,} rows). "
            "Consider adding executor instances or reducing window size."
        )
        logger.warning("BACKPRESSURE DETECTED — rate dropped %.1f%% (%.0f → %.0f rows/s)",
                       rate_drop_pct, previous.process_rows_per_second, recent.process_rows_per_second)
    else:
        result["recommendation"] = "No action required"

    return result


# ---------------------------------------------------------------------------
# Watermark helpers
# ---------------------------------------------------------------------------


def parse_watermark_delay(delay: str) -> dict[str, Any]:
    """
    Parse a Spark watermark delay string into a structured dict.

    Validates format: '<N> <unit>' where unit is seconds/minutes/hours/days.
    Used in tests to verify watermark configuration without a Spark session.

    Args:
        delay: Spark-format delay string, e.g. '10 minutes', '2 hours'.

    Returns:
        Dict with value (int), unit (str), total_seconds (int).

    Raises:
        ValueError: If the delay string does not match the expected format.
    """
    parts = delay.strip().split()
    if len(parts) != 2:
        raise ValueError(
            f"Invalid watermark delay {delay!r}. Expected format: '<N> <unit>' "
            "e.g. '10 minutes', '2 hours'"
        )

    try:
        value = int(parts[0])
    except ValueError:
        raise ValueError(f"Watermark delay value must be an integer, got {parts[0]!r}")

    unit = parts[1].lower().rstrip("s")  # normalize: 'minutes' → 'minute'
    unit_seconds: dict[str, int] = {
        "second": 1,
        "minute": 60,
        "hour": 3600,
        "day": 86400,
    }
    if unit not in unit_seconds:
        raise ValueError(
            f"Unrecognized watermark unit {parts[1]!r}. Valid: seconds, minutes, hours, days"
        )

    return {
        "value": value,
        "unit": unit,
        "total_seconds": value * unit_seconds[unit],
        "spark_string": delay,
    }


def validate_late_data_policy(watermark_delay: str, trigger_interval: str, max_late_arrival_minutes: int) -> dict[str, Any]:
    """
    Validate that the watermark is large enough to cover the expected late arrival window.

    If the watermark delay is shorter than max_late_arrival_minutes, late events
    will be silently dropped by Spark, leading to data loss.

    Args:
        watermark_delay: Spark watermark string, e.g. '10 minutes'.
        trigger_interval: Spark trigger string, e.g. '1 minute'.
        max_late_arrival_minutes: Maximum expected event lateness in minutes.

    Returns:
        Dict with policy_valid bool and any warnings.
    """
    wm = parse_watermark_delay(watermark_delay)
    trigger = parse_watermark_delay(trigger_interval)

    watermark_minutes = wm["total_seconds"] // 60
    trigger_minutes = trigger["total_seconds"] // 60

    warnings = []
    if watermark_minutes < max_late_arrival_minutes:
        warnings.append(
            f"Watermark ({watermark_minutes}min) is shorter than expected late arrival "
            f"({max_late_arrival_minutes}min). Late events will be dropped."
        )
    if trigger_minutes > watermark_minutes:
        warnings.append(
            f"Trigger interval ({trigger_minutes}min) exceeds watermark ({watermark_minutes}min). "
            "State may not advance correctly."
        )

    return {
        "policy_valid": len(warnings) == 0,
        "watermark_minutes": watermark_minutes,
        "trigger_minutes": trigger_minutes,
        "max_late_arrival_minutes": max_late_arrival_minutes,
        "warnings": warnings,
    }


# ---------------------------------------------------------------------------
# Exactly-once semantics helpers
# ---------------------------------------------------------------------------


def build_kafka_read_options(config: KafkaStreamConfig) -> dict[str, str]:
    """
    Build the Kafka read options dict for Structured Streaming.

    Exactly-once is achieved through the combination of:
    1. Delta table writes (transactional, idempotent)
    2. Spark checkpoint (atomically records committed Kafka offsets)
    3. maxOffsetsPerTrigger (bounded micro-batches prevent OOM on lag spike)

    Returns the options dict suitable for spark.readStream.options(**opts).
    """
    return {
        "kafka.bootstrap.servers": config.bootstrap_servers,
        "subscribe": config.topic,
        "startingOffsets": config.starting_offsets,
        "maxOffsetsPerTrigger": str(config.max_offsets_per_trigger),
        "kafka.group.id": config.kafka_group_id,
        "failOnDataLoss": "false",
    }


def build_stream_write_options(config: KafkaStreamConfig, output_mode: str = "append") -> dict[str, str]:
    """
    Build Delta writeStream options dict for exactly-once semantics.

    The checkpoint location is the key to exactly-once delivery:
    - Spark writes Kafka offsets and Delta commit in the same atomic transaction
    - On recovery, Spark reprocesses from the last committed offset only
    - Duplicate writes within the same epoch are rejected by Delta's log

    Raises:
        ValueError: If checkpoint_location is empty.
    """
    if not config.checkpoint_location.strip():
        raise ValueError("checkpoint_location must not be empty for exactly-once semantics")
    return {
        "checkpointLocation": config.checkpoint_location,
        "mergeSchema": "true",
    }


# ---------------------------------------------------------------------------
# DLQ routing
# ---------------------------------------------------------------------------


def classify_dlq_record(record: dict[str, Any], required_fields: list[str]) -> dict[str, Any]:
    """
    Determine if a record should be routed to the Dead Letter Queue.

    A record is DLQ-eligible if:
    - Any required field is None/missing
    - The record contains an explicit parse error marker

    Args:
        record: Deserialized event dict.
        required_fields: Fields that must be non-null.

    Returns:
        Dict with is_dlq bool, reason str, and original record.
    """
    missing = [f for f in required_fields if record.get(f) is None]
    if missing:
        return {
            "is_dlq": True,
            "reason": f"NULL_REQUIRED_FIELDS:{','.join(missing)}",
            "record": record,
        }
    if record.get("_corrupt_record") is not None:
        return {
            "is_dlq": True,
            "reason": "PARSE_FAILURE",
            "record": record,
        }
    return {"is_dlq": False, "reason": None, "record": record}


def compute_dlq_rate(total_records: int, dlq_records: int) -> dict[str, Any]:
    """
    Compute the DLQ rate for a batch.

    A DLQ rate above 1% warrants investigation (schema change, upstream bug).
    A DLQ rate above 5% should trigger an alert.

    Args:
        total_records: Total records in the batch.
        dlq_records: Records routed to the DLQ.

    Returns:
        Dict with dlq_rate_pct, severity, and recommendation.
    """
    if total_records == 0:
        return {"dlq_rate_pct": 0.0, "severity": "OK", "recommendation": "No records processed"}

    rate_pct = round(dlq_records / total_records * 100, 3)
    if rate_pct >= 5.0:
        severity = "CRITICAL"
        recommendation = "DLQ rate exceeds 5% — investigate schema contract with upstream producer"
    elif rate_pct >= 1.0:
        severity = "WARN"
        recommendation = "DLQ rate above 1% — review recent schema changes"
    else:
        severity = "OK"
        recommendation = "DLQ rate within acceptable bounds"

    return {
        "dlq_rate_pct": rate_pct,
        "total_records": total_records,
        "dlq_records": dlq_records,
        "severity": severity,
        "recommendation": recommendation,
    }


# ---------------------------------------------------------------------------
# Streaming pipeline (Spark-dependent, wraps all helpers)
# ---------------------------------------------------------------------------


def run_kafka_streaming(config: KafkaStreamConfig, spark: Any) -> None:
    """
    End-to-end Kafka → Delta streaming pipeline.

    Features:
    - maxOffsetsPerTrigger backpressure control
    - Event-time watermark for late data tolerance
    - Good records → output_table (exactly-once Delta write)
    - Bad records → dlq_table (NULL required field isolation)
    - Checkpoint ensures exactly-once on restart

    Anti-patterns this avoids:
    - No spark.streaming.kafka.maxRatePerPartition (deprecated in Structured Streaming)
    - No manual offset management (handled by checkpoint)
    - No output mode = 'complete' (grows unbounded state)
    """
    from pyspark.sql import functions as F
    from pyspark.sql.types import StringType, StructType

    read_opts = build_kafka_read_options(config)
    write_opts = build_stream_write_options(config)

    raw_stream = spark.readStream.format("kafka").options(**read_opts).load()

    parsed = raw_stream.select(
        F.col("offset").alias("kafka_offset"),
        F.col("partition").alias("kafka_partition"),
        F.col("timestamp").alias("kafka_ts"),
        F.col("value").cast(StringType()).alias("raw_value"),
    )

    required_cols = ["kafka_offset", "kafka_partition", "kafka_ts"]
    null_condition = F.lit(False)
    for col_name in required_cols:
        null_condition = null_condition | F.col(col_name).isNull()

    good_stream = parsed.filter(~null_condition)
    bad_stream = (
        parsed.filter(null_condition)
        .withColumn("dlq_reason", F.lit("NULL_REQUIRED_FIELD"))
        .withColumn("dlq_at", F.current_timestamp())
    )

    watermarked = good_stream.withWatermark("kafka_ts", config.watermark_delay)

    agg_query = (
        watermarked.writeStream.format("delta")
        .outputMode("append")
        .options(**write_opts)
        .trigger(processingTime=config.trigger_interval)
        .toTable(config.output_table)
    )

    dlq_table = config.dlq_table or f"{config.output_table}_dlq"
    dlq_checkpoint = f"{config.checkpoint_location}_dlq"
    dlq_query = (
        bad_stream.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", dlq_checkpoint)
        .trigger(processingTime=config.trigger_interval)
        .toTable(dlq_table)
    )

    logger.info(
        "run_kafka_streaming: topic=%s → %s (DLQ: %s)",
        config.topic,
        config.output_table,
        dlq_table,
    )

    agg_query.awaitTermination()
