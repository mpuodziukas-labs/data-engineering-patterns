"""
Test suite for data engineering patterns.
12 tests covering medallion logic, Delta patterns, streaming utilities, and DLQ.
No Spark required for unit tests — all Spark-dependent tests mock SparkSession.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any
from unittest.mock import MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Helper stubs (avoid SparkSession in unit tests)
# ---------------------------------------------------------------------------


class MockColumn:
    def __init__(self, name: str):
        self._name = name

    def isNull(self) -> "MockColumn":
        return MockColumn(f"{self._name}.isNull()")

    def __eq__(self, other: object) -> "MockColumn":
        return MockColumn(f"{self._name}=={other}")

    def __or__(self, other: object) -> "MockColumn":
        return MockColumn(f"({self._name}|{other})")


class MockDataFrame:
    def __init__(self, rows: list[dict[str, Any]], columns: list[str] | None = None):
        self._rows = rows
        self.columns = columns or (list(rows[0].keys()) if rows else [])
        self.schema = MagicMock()
        self.schema.fields = [MagicMock(name=c) for c in self.columns]

    def count(self) -> int:
        return len(self._rows)

    def isEmpty(self) -> bool:
        return len(self._rows) == 0

    def filter(self, condition: Any) -> "MockDataFrame":
        return self  # No-op in unit tests

    def withColumn(self, name: str, value: Any) -> "MockDataFrame":
        return self

    def select(self, *args: Any) -> "MockDataFrame":
        return self

    def drop(self, *args: Any) -> "MockDataFrame":
        return self

    def first(self) -> dict[str, Any] | None:
        return self._rows[0] if self._rows else None

    def collect(self) -> list[dict[str, Any]]:
        return self._rows


# ---------------------------------------------------------------------------
# Test 1: Bronze — bad records quarantine logic
# ---------------------------------------------------------------------------


def test_bronze_quarantine_logic_skips_when_no_bad_records(tmp_path: Any) -> None:
    """
    If the bad_records_path directory is empty, quarantine should return 0
    without attempting any Spark operations.
    """
    # Empty directory
    bad_path = tmp_path / "bad_records"
    bad_path.mkdir()

    mock_spark = MagicMock()

    with patch("medallion.bronze_ingestion.Path") as mock_path_cls:
        mock_path_obj = MagicMock()
        mock_path_cls.return_value = mock_path_obj
        mock_path_obj.exists.return_value = True
        mock_path_obj.__iter__ = MagicMock(return_value=iter([]))  # Empty directory

        from medallion.bronze_ingestion import quarantine_bad_records

        result = quarantine_bad_records(mock_spark, str(bad_path), "batch-001", "/data/source", "bronze.quarantine")

    assert result == 0
    mock_spark.read.assert_not_called()


# ---------------------------------------------------------------------------
# Test 2: Bronze — metadata stamping
# ---------------------------------------------------------------------------


def test_bronze_metadata_columns_added() -> None:
    """add_bronze_metadata should add exactly 4 audit columns."""
    mock_df = MockDataFrame([{"event_id": "e1", "user_id": "u1"}])
    call_log: list[str] = []

    original_with_column = mock_df.withColumn

    def tracking_with_column(name: str, value: Any) -> MockDataFrame:
        call_log.append(name)
        return mock_df

    mock_df.withColumn = tracking_with_column  # type: ignore[method-assign]

    with patch("medallion.bronze_ingestion.F"):
        from medallion.bronze_ingestion import add_bronze_metadata

        add_bronze_metadata(mock_df, "batch-001", "/source/path")

    assert "_ingested_at" in call_log
    assert "_source_path" in call_log
    assert "_batch_id" in call_log
    assert "_record_hash" in call_log


# ---------------------------------------------------------------------------
# Test 3: Silver — dedup logic
# ---------------------------------------------------------------------------


def test_silver_dedup_removes_duplicates() -> None:
    """
    drop_duplicates_keep_latest should invoke ROW_NUMBER window function.
    The resulting filter on _rn == 1 ensures only latest per key survives.
    """
    with patch("medallion.silver_transform.F") as mock_f:
        mock_f.expr.return_value = MagicMock()
        mock_f.col.return_value = MockColumn("_rn")

        mock_df = MockDataFrame([
            {"user_id": "u1", "updated_at": "2026-01-02", "_rn": 1},
            {"user_id": "u1", "updated_at": "2026-01-01", "_rn": 2},
        ])
        mock_df.withColumn = MagicMock(return_value=mock_df)
        mock_df.filter = MagicMock(return_value=mock_df)
        mock_df.drop = MagicMock(return_value=mock_df)

        from medallion.silver_transform import drop_duplicates_keep_latest

        result = drop_duplicates_keep_latest(mock_df, "user_id", "updated_at")

    # Verify ROW_NUMBER expression was constructed
    mock_f.expr.assert_called_once()
    expr_call = mock_f.expr.call_args[0][0]
    assert "ROW_NUMBER" in expr_call
    assert "user_id" in expr_call
    assert "updated_at" in expr_call


# ---------------------------------------------------------------------------
# Test 4: Silver — GX row count validation
# ---------------------------------------------------------------------------


def test_gx_row_count_check_fails_on_empty_dataframe() -> None:
    """validate_row_count should return passed=False for empty DataFrames."""
    mock_df = MockDataFrame([])

    from medallion.silver_transform import validate_row_count

    result = validate_row_count(mock_df, minimum=1)

    assert result["passed"] is False
    assert result["row_count"] == 0
    assert result["minimum"] == 1


def test_gx_row_count_check_passes_with_data() -> None:
    """validate_row_count should return passed=True when count >= minimum."""
    mock_df = MockDataFrame([{"a": 1}, {"a": 2}, {"a": 3}])

    from medallion.silver_transform import validate_row_count

    result = validate_row_count(mock_df, minimum=3)

    assert result["passed"] is True
    assert result["row_count"] == 3


# ---------------------------------------------------------------------------
# Test 5: Silver — schema column validation
# ---------------------------------------------------------------------------


def test_gx_columns_exist_catches_missing() -> None:
    """validate_columns_exist should catch columns in expected list that are absent."""
    mock_df = MockDataFrame([{"event_id": "e1", "user_id": "u1"}])

    from medallion.silver_transform import validate_columns_exist

    result = validate_columns_exist(mock_df, expected=["event_id", "user_id", "event_ts"])

    assert result["passed"] is False
    assert "event_ts" in result["missing_columns"]


# ---------------------------------------------------------------------------
# Test 6: Gold — SLA tracking
# ---------------------------------------------------------------------------


def test_sla_tracking_detects_breach() -> None:
    """track_sla should set breached=True when duration exceeds threshold."""
    from medallion.gold_aggregation import SLA_THRESHOLDS_SECONDS, track_sla

    # Simulate a job that took longer than the SLA
    threshold = SLA_THRESHOLDS_SECONDS.get("daily_metrics", 900.0)
    simulated_start = time.time() - (threshold + 60)  # 60 seconds over SLA

    result = track_sla("daily_metrics", simulated_start, row_count=1000)

    assert result.breached is True
    assert result.breach_margin_seconds > 0


def test_sla_tracking_passes_within_threshold() -> None:
    """track_sla should set breached=False when duration is well within threshold."""
    from medallion.gold_aggregation import track_sla

    simulated_start = time.time() - 5.0  # 5 seconds ago

    result = track_sla("daily_metrics", simulated_start, row_count=500)

    assert result.breached is False
    assert result.duration_seconds < 900.0


# ---------------------------------------------------------------------------
# Test 7: Pipeline runner — retry policy backoff
# ---------------------------------------------------------------------------


def test_retry_backoff_is_exponential() -> None:
    """Backoff delay should grow exponentially with each attempt."""
    from medallion.pipeline_runner import RetryPolicy, _compute_backoff

    policy = RetryPolicy(base_delay_seconds=2.0, exponential_base=2.0, max_delay_seconds=1000.0)

    delay_1 = _compute_backoff(1, policy)
    delay_2 = _compute_backoff(2, policy)
    delay_3 = _compute_backoff(3, policy)

    assert delay_1 == 2.0    # 2 * 2^0
    assert delay_2 == 4.0    # 2 * 2^1
    assert delay_3 == 8.0    # 2 * 2^2


def test_retry_backoff_capped_at_max() -> None:
    """Backoff should never exceed max_delay_seconds."""
    from medallion.pipeline_runner import RetryPolicy, _compute_backoff

    policy = RetryPolicy(base_delay_seconds=10.0, exponential_base=10.0, max_delay_seconds=50.0)

    delay = _compute_backoff(5, policy)  # 10 * 10^4 = 100,000 — would be huge uncapped

    assert delay == 50.0


# ---------------------------------------------------------------------------
# Test 8: Pipeline runner — retry on failure
# ---------------------------------------------------------------------------


def test_run_with_retry_succeeds_on_second_attempt() -> None:
    """run_with_retry should succeed if the function passes on attempt 2."""
    from medallion.pipeline_runner import RetryPolicy, run_with_retry

    attempt_counter = {"count": 0}

    def flaky_fn() -> dict[str, int]:
        attempt_counter["count"] += 1
        if attempt_counter["count"] < 2:
            raise RuntimeError("Simulated transient failure")
        return {"rows": 100}

    policy = RetryPolicy(max_attempts=3, base_delay_seconds=0.001, exponential_base=2.0, max_delay_seconds=0.01)

    result = run_with_retry(flaky_fn, "test_stage", policy)

    assert result.success is True
    assert result.attempt == 2
    assert result.metrics == {"rows": 100}


# ---------------------------------------------------------------------------
# Test 9: Delta — VACUUM retain hours guard
# ---------------------------------------------------------------------------


def test_vacuum_enforces_minimum_retain_hours() -> None:
    """vacuum_table should override retain_hours < 168 to 168."""
    mock_spark = MagicMock()
    config = MagicMock()
    config.table_name = "test_table"
    config.spark = mock_spark

    with patch("delta_patterns.time_travel.TimeTravelConfig"):
        from delta_patterns.time_travel import TimeTravelConfig, vacuum_table

        cfg = TimeTravelConfig(table_name="test_table", spark=mock_spark)
        vacuum_table(cfg, retain_hours=1)  # Dangerously low — should be overridden

    # Verify SQL used 168, not 1
    call_args = mock_spark.sql.call_args[0][0]
    assert "168" in call_args
    assert "1 HOURS" not in call_args


# ---------------------------------------------------------------------------
# Test 10: Schema evolution — diff_schemas detects changes
# ---------------------------------------------------------------------------


def test_schema_diff_detects_added_and_removed_columns() -> None:
    """diff_schemas should correctly identify added and removed columns."""
    from pyspark.sql.types import IntegerType, StringType, StructField, StructType

    schema_a = StructType([
        StructField("event_id", StringType()),
        StructField("user_id", StringType()),
        StructField("old_col", IntegerType()),
    ])
    schema_b = StructType([
        StructField("event_id", StringType()),
        StructField("user_id", StringType()),
        StructField("new_col", IntegerType()),
    ])

    from delta_patterns.schema_evolution import diff_schemas

    diff = diff_schemas(schema_a, schema_b)

    assert "new_col" in diff["added_columns"]
    assert "old_col" in diff["removed_columns"]
    assert diff["is_backward_compatible"] is False


# ---------------------------------------------------------------------------
# Test 11: DLQ — quarantine stamping
# ---------------------------------------------------------------------------


def test_dlq_quarantine_adds_required_metadata_columns() -> None:
    """quarantine_records should add all required DLQ metadata columns."""
    stamped_columns: list[str] = []

    mock_df = MockDataFrame([{"event_id": "e1"}])
    mock_df.withColumn = lambda name, val: (stamped_columns.append(name) or mock_df)  # type: ignore[method-assign]

    with patch("streaming.dlq_handler.F"):
        from streaming.dlq_handler import quarantine_records

        quarantine_records(mock_df, reason="TEST_REASON", source_table="raw.events", batch_id="b001")

    required_cols = {"dlq_id", "dlq_reason", "dlq_status", "dlq_quarantined_at", "dlq_source_table", "dlq_batch_id", "dlq_reprocess_count"}
    assert required_cols.issubset(set(stamped_columns))


# ---------------------------------------------------------------------------
# Test 12: Benchmark result statistics
# ---------------------------------------------------------------------------


def test_scenario_result_improvement_calculation() -> None:
    """ScenarioResult should compute improvement_pct and speedup_factor correctly."""
    from benchmarks.query_benchmark import ScenarioResult

    result = ScenarioResult(
        scenario="test",
        description="unit test",
        before_times=[45.0, 45.0, 45.0, 45.0, 45.0],  # Median = 45.0
        after_times=[8.0, 8.0, 8.0, 8.0, 8.0],          # Median = 8.0
    )

    assert result.before_median == 45.0
    assert result.after_median == 8.0
    assert result.improvement_pct == pytest.approx(82.2, abs=0.1)
    assert result.speedup_factor == pytest.approx(5.625, abs=0.01)
    assert result.verdict() == "EXCELLENT"
