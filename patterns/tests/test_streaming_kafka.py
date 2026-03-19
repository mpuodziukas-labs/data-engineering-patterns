"""
Tests for streaming_kafka.py — 15 tests.

All tests are pure Python — no Spark, no Kafka broker required.
They verify lag computation, backpressure detection, watermark validation,
DLQ routing, and exactly-once option builders.
"""

from __future__ import annotations

import pytest

from patterns.streaming_kafka import (
    LAG_ALERT_THRESHOLD,
    LAG_CRITICAL_THRESHOLD,
    BatchMetrics,
    KafkaStreamConfig,
    LagConfig,
    PartitionLagRecord,
    build_kafka_read_options,
    build_stream_write_options,
    classify_dlq_record,
    compute_consumer_lag,
    compute_dlq_rate,
    detect_backpressure,
    parse_watermark_delay,
    validate_late_data_policy,
)


# ---------------------------------------------------------------------------
# Consumer lag computation
# ---------------------------------------------------------------------------


class TestComputeConsumerLag:
    def _config(self) -> LagConfig:
        return LagConfig(
            bootstrap_servers=["localhost:9092"],
            consumer_group_id="test-group",
            topics=["events"],
        )

    def test_zero_lag(self) -> None:
        log_end = {("events", 0): 1000, ("events", 1): 2000}
        committed = {("events", 0): 1000, ("events", 1): 2000}
        report = compute_consumer_lag(log_end, committed, self._config())
        assert report.total_lag == 0
        assert report.alert_triggered is False
        assert report.critical_triggered is False

    def test_alert_triggered(self) -> None:
        log_end = {("events", 0): LAG_ALERT_THRESHOLD + 1}
        committed = {("events", 0): 0}
        report = compute_consumer_lag(log_end, committed, self._config())
        assert report.alert_triggered is True
        assert report.critical_triggered is False

    def test_critical_triggered(self) -> None:
        log_end = {("events", 0): LAG_CRITICAL_THRESHOLD + 1}
        committed = {("events", 0): 0}
        report = compute_consumer_lag(log_end, committed, self._config())
        assert report.critical_triggered is True
        assert report.alert_triggered is True  # critical implies alert

    def test_missing_committed_offset_treated_as_zero(self) -> None:
        log_end = {("events", 0): 500}
        committed: dict[tuple[str, int], int] = {}  # no committed offset
        report = compute_consumer_lag(log_end, committed, self._config())
        assert report.total_lag == 500

    def test_consumer_ahead_of_log_end_clamped_to_zero(self) -> None:
        """Consumer offset > log end should never produce negative lag."""
        log_end = {("events", 0): 100}
        committed = {("events", 0): 200}  # consumer ahead (e.g. after reset)
        report = compute_consumer_lag(log_end, committed, self._config())
        assert report.total_lag == 0

    def test_multi_partition_aggregate_lag(self) -> None:
        log_end = {("events", 0): 1000, ("events", 1): 2000, ("events", 2): 3000}
        committed = {("events", 0): 900, ("events", 1): 1900, ("events", 2): 2900}
        report = compute_consumer_lag(log_end, committed, self._config())
        assert report.total_lag == 300
        assert report.max_partition_lag == 100

    def test_summary_contains_group_id(self) -> None:
        log_end = {("events", 0): 100}
        committed = {("events", 0): 100}
        report = compute_consumer_lag(log_end, committed, self._config())
        assert "test-group" in report.summary()

    def test_partition_severity_ok(self) -> None:
        p = PartitionLagRecord(topic="t", partition=0, consumer_offset=100, log_end_offset=100, lag=0)
        assert p.severity == "OK"

    def test_partition_severity_warn(self) -> None:
        p = PartitionLagRecord(topic="t", partition=0, consumer_offset=0, log_end_offset=LAG_ALERT_THRESHOLD, lag=LAG_ALERT_THRESHOLD)
        assert p.severity == "WARN"

    def test_partition_severity_critical(self) -> None:
        p = PartitionLagRecord(topic="t", partition=0, consumer_offset=0, log_end_offset=LAG_CRITICAL_THRESHOLD, lag=LAG_CRITICAL_THRESHOLD)
        assert p.severity == "CRITICAL"


# ---------------------------------------------------------------------------
# Backpressure detection
# ---------------------------------------------------------------------------


class TestDetectBackpressure:
    def _batch(self, batch_id: int, rate: float) -> BatchMetrics:
        return BatchMetrics(
            batch_id=batch_id,
            records_processed=int(rate * 60),
            duration_seconds=60.0,
            input_rows_per_second=rate,
            process_rows_per_second=rate,
        )

    def test_no_backpressure_stable_rate(self) -> None:
        history = [self._batch(0, 1000.0), self._batch(1, 950.0)]
        result = detect_backpressure(history)
        assert result["backpressure_detected"] is False

    def test_backpressure_detected_on_rate_drop(self) -> None:
        history = [self._batch(0, 1000.0), self._batch(1, 500.0)]
        result = detect_backpressure(history)
        assert result["backpressure_detected"] is True
        assert result["rate_drop_pct"] == pytest.approx(50.0)

    def test_insufficient_history(self) -> None:
        result = detect_backpressure([self._batch(0, 1000.0)])
        assert result["backpressure_detected"] is False
        assert result["reason"] == "insufficient_history"

    def test_recommendation_present_on_backpressure(self) -> None:
        history = [self._batch(0, 1000.0), self._batch(1, 100.0)]
        result = detect_backpressure(history)
        assert "recommendation" in result
        assert "maxOffsetsPerTrigger" in result["recommendation"]


# ---------------------------------------------------------------------------
# Watermark parsing
# ---------------------------------------------------------------------------


class TestParseWatermarkDelay:
    def test_minutes(self) -> None:
        result = parse_watermark_delay("10 minutes")
        assert result["value"] == 10
        assert result["total_seconds"] == 600

    def test_hours(self) -> None:
        result = parse_watermark_delay("2 hours")
        assert result["total_seconds"] == 7200

    def test_seconds(self) -> None:
        result = parse_watermark_delay("30 seconds")
        assert result["total_seconds"] == 30

    def test_invalid_format_raises(self) -> None:
        with pytest.raises(ValueError, match="Invalid watermark delay"):
            parse_watermark_delay("10min")

    def test_invalid_unit_raises(self) -> None:
        with pytest.raises(ValueError, match="Unrecognized watermark unit"):
            parse_watermark_delay("10 weeks")


class TestValidateLateDataPolicy:
    def test_valid_policy(self) -> None:
        result = validate_late_data_policy("30 minutes", "1 minute", max_late_arrival_minutes=15)
        assert result["policy_valid"] is True
        assert len(result["warnings"]) == 0

    def test_watermark_too_short(self) -> None:
        result = validate_late_data_policy("5 minutes", "1 minute", max_late_arrival_minutes=30)
        assert result["policy_valid"] is False
        assert any("will be dropped" in w for w in result["warnings"])


# ---------------------------------------------------------------------------
# DLQ routing
# ---------------------------------------------------------------------------


class TestClassifyDlqRecord:
    def test_good_record(self) -> None:
        record = {"event_id": "abc", "user_id": "u1", "event_type": "purchase"}
        result = classify_dlq_record(record, ["event_id", "user_id", "event_type"])
        assert result["is_dlq"] is False

    def test_missing_required_field(self) -> None:
        record = {"event_id": "abc", "user_id": None, "event_type": "purchase"}
        result = classify_dlq_record(record, ["event_id", "user_id", "event_type"])
        assert result["is_dlq"] is True
        assert "user_id" in result["reason"]

    def test_corrupt_record_marker(self) -> None:
        record = {"_corrupt_record": '{"bad json', "event_id": None}
        result = classify_dlq_record(record, ["event_id"])
        assert result["is_dlq"] is True


class TestComputeDlqRate:
    def test_acceptable_rate(self) -> None:
        result = compute_dlq_rate(1000, 5)
        assert result["severity"] == "OK"
        assert result["dlq_rate_pct"] == pytest.approx(0.5)

    def test_warn_rate(self) -> None:
        result = compute_dlq_rate(1000, 20)
        assert result["severity"] == "WARN"

    def test_critical_rate(self) -> None:
        result = compute_dlq_rate(1000, 60)
        assert result["severity"] == "CRITICAL"

    def test_zero_total_records(self) -> None:
        result = compute_dlq_rate(0, 0)
        assert result["dlq_rate_pct"] == 0.0


# ---------------------------------------------------------------------------
# Exactly-once option builders
# ---------------------------------------------------------------------------


class TestBuildKafkaReadOptions:
    def test_required_keys_present(self) -> None:
        config = KafkaStreamConfig(
            bootstrap_servers="broker:9092",
            topic="events",
            checkpoint_location="/chk",
            output_table="silver.events",
        )
        opts = build_kafka_read_options(config)
        assert "kafka.bootstrap.servers" in opts
        assert opts["subscribe"] == "events"
        assert "maxOffsetsPerTrigger" in opts
        assert opts["failOnDataLoss"] == "false"


class TestBuildStreamWriteOptions:
    def test_checkpoint_present(self) -> None:
        config = KafkaStreamConfig(
            bootstrap_servers="broker:9092",
            topic="events",
            checkpoint_location="/chk/events",
            output_table="silver.events",
        )
        opts = build_stream_write_options(config)
        assert opts["checkpointLocation"] == "/chk/events"

    def test_empty_checkpoint_raises(self) -> None:
        config = KafkaStreamConfig(
            bootstrap_servers="broker:9092",
            topic="events",
            checkpoint_location="",
            output_table="silver.events",
        )
        with pytest.raises(ValueError, match="checkpoint_location must not be empty"):
            build_stream_write_options(config)
