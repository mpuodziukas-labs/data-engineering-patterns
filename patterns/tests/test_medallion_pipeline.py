"""
Tests for medallion_pipeline.py — 15 tests.

All tests are pure Python — no Spark, no Delta Lake runtime required.
They verify the business logic, SQL generation, and metric computation
that surrounds the Spark calls.
"""

from __future__ import annotations

import pytest

from patterns.medallion_pipeline import (
    ZORDER_BENCHMARK_RESULT,
    MedallionPipelineConfig,
    BronzeConfig,
    SilverConfig,
    GoldConfig,
    OptimizeConfig,
    build_optimize_sql,
    build_schema_evolution_sql,
    build_vacuum_sql,
    classify_benchmark_verdict,
    compute_improvement_pct,
    diff_schemas,
    extract_medallion_metrics,
    validate_vacuum_retention,
)


# ---------------------------------------------------------------------------
# Z-ORDER benchmark math
# ---------------------------------------------------------------------------


class TestComputeImprovementPct:
    def test_documented_82_percent_improvement(self) -> None:
        """Verify the documented benchmark result is mathematically consistent."""
        pct = compute_improvement_pct(14.3, 2.6)
        assert pct == pytest.approx(81.8, abs=0.2)

    def test_zero_improvement(self) -> None:
        pct = compute_improvement_pct(10.0, 10.0)
        assert pct == 0.0

    def test_full_improvement(self) -> None:
        """100% improvement = after_seconds is 0."""
        pct = compute_improvement_pct(10.0, 0.0)
        assert pct == 100.0

    def test_negative_improvement_allowed(self) -> None:
        """after > before = regression, negative pct."""
        pct = compute_improvement_pct(5.0, 10.0)
        assert pct == -100.0

    def test_before_zero_raises(self) -> None:
        with pytest.raises(ValueError, match="before_seconds must be > 0"):
            compute_improvement_pct(0.0, 5.0)

    def test_negative_after_raises(self) -> None:
        with pytest.raises(ValueError, match="after_seconds must be >= 0"):
            compute_improvement_pct(5.0, -1.0)


class TestClassifyBenchmarkVerdict:
    def test_beneficial(self) -> None:
        assert classify_benchmark_verdict(82.0) == "BENEFICIAL"

    def test_exactly_20_pct_is_beneficial(self) -> None:
        assert classify_benchmark_verdict(20.0) == "BENEFICIAL"

    def test_marginal(self) -> None:
        assert classify_benchmark_verdict(10.0) == "MARGINAL"

    def test_exactly_5_pct_is_marginal(self) -> None:
        assert classify_benchmark_verdict(5.0) == "MARGINAL"

    def test_no_improvement(self) -> None:
        assert classify_benchmark_verdict(0.0) == "NO_IMPROVEMENT"

    def test_negative_is_no_improvement(self) -> None:
        assert classify_benchmark_verdict(-5.0) == "NO_IMPROVEMENT"

    def test_documented_result_is_beneficial(self) -> None:
        """Validate the module-level ZORDER_BENCHMARK_RESULT constant is self-consistent."""
        pct = compute_improvement_pct(
            ZORDER_BENCHMARK_RESULT["before_median_seconds"],
            ZORDER_BENCHMARK_RESULT["after_median_seconds"],
        )
        verdict = classify_benchmark_verdict(pct)
        assert verdict == "BENEFICIAL"
        assert pct >= 80.0, f"Expected ≥80% improvement, got {pct}%"


# ---------------------------------------------------------------------------
# SQL builders
# ---------------------------------------------------------------------------


class TestBuildOptimizeSql:
    def test_basic_zorder(self) -> None:
        sql = build_optimize_sql("silver.events", ["user_id", "event_type"])
        assert sql == "OPTIMIZE silver.events ZORDER BY (user_id, event_type)"

    def test_with_partition_predicate(self) -> None:
        sql = build_optimize_sql("silver.events", ["user_id"], partition_predicate="event_date='2024-01-01'")
        assert "WHERE event_date='2024-01-01'" in sql
        assert "ZORDER BY (user_id)" in sql

    def test_single_column(self) -> None:
        sql = build_optimize_sql("gold.revenue", ["product_id"])
        assert "ZORDER BY (product_id)" in sql

    def test_empty_table_name_raises(self) -> None:
        with pytest.raises(ValueError, match="table_name must not be empty"):
            build_optimize_sql("", ["user_id"])

    def test_empty_columns_raises(self) -> None:
        with pytest.raises(ValueError, match="z_order_columns must not be empty"):
            build_optimize_sql("silver.events", [])


class TestBuildVacuumSql:
    def test_default_retention(self) -> None:
        sql = build_vacuum_sql("silver.events", 168)
        assert sql == "VACUUM silver.events RETAIN 168 HOURS"

    def test_dry_run(self) -> None:
        sql = build_vacuum_sql("silver.events", 168, dry_run=True)
        assert sql.endswith("DRY RUN")

    def test_non_default_retention(self) -> None:
        sql = build_vacuum_sql("gold.metrics", 720)
        assert "RETAIN 720 HOURS" in sql

    def test_empty_table_raises(self) -> None:
        with pytest.raises(ValueError, match="table_name must not be empty"):
            build_vacuum_sql("", 168)

    def test_zero_retention_raises(self) -> None:
        with pytest.raises(ValueError, match="retention_hours must be >= 1"):
            build_vacuum_sql("silver.events", 0)


class TestBuildSchemaEvolutionSql:
    def test_add_column(self) -> None:
        sql = build_schema_evolution_sql("add", "silver.events", "session_revenue", "DOUBLE")
        assert sql == "ALTER TABLE silver.events ADD COLUMN session_revenue DOUBLE"

    def test_rename_column(self) -> None:
        sql = build_schema_evolution_sql("rename", "silver.events", "user_id", new_column_name="customer_id")
        assert sql == "ALTER TABLE silver.events RENAME COLUMN user_id TO customer_id"

    def test_drop_column(self) -> None:
        sql = build_schema_evolution_sql("drop", "silver.events", "legacy_field")
        assert sql == "ALTER TABLE silver.events DROP COLUMN legacy_field"

    def test_unsupported_operation_raises(self) -> None:
        with pytest.raises(ValueError, match="Unsupported operation"):
            build_schema_evolution_sql("modify", "silver.events", "col")

    def test_add_without_type_raises(self) -> None:
        with pytest.raises(ValueError, match="column_type required"):
            build_schema_evolution_sql("add", "silver.events", "new_col")

    def test_rename_without_new_name_raises(self) -> None:
        with pytest.raises(ValueError, match="new_column_name required"):
            build_schema_evolution_sql("rename", "silver.events", "old_col")


# ---------------------------------------------------------------------------
# Vacuum validation
# ---------------------------------------------------------------------------


class TestValidateVacuumRetention:
    def test_safe_retention(self) -> None:
        result = validate_vacuum_retention(168)
        assert result["valid"] is True
        assert result["warning"] is None

    def test_below_threshold_warns(self) -> None:
        result = validate_vacuum_retention(24)
        assert result["valid"] is True
        assert result["warning"] is not None
        assert "7-day" in result["warning"]

    def test_zero_is_invalid(self) -> None:
        result = validate_vacuum_retention(0)
        assert result["valid"] is False

    def test_negative_is_invalid(self) -> None:
        result = validate_vacuum_retention(-1)
        assert result["valid"] is False

    def test_one_hour_is_valid_but_warns(self) -> None:
        result = validate_vacuum_retention(1)
        assert result["valid"] is True
        assert result["warning"] is not None


# ---------------------------------------------------------------------------
# Schema diff
# ---------------------------------------------------------------------------


class TestDiffSchemas:
    """Test diff_schemas with mock StructType-like objects."""

    class _MockField:
        def __init__(self, name: str, data_type: str) -> None:
            self.name = name
            self.dataType = data_type  # noqa: N815

        def __str__(self) -> str:
            return self.dataType

    class _MockSchema:
        def __init__(self, fields: list) -> None:
            self.fields = fields

    def test_backward_compatible_add_column(self) -> None:
        schema_a = self._MockSchema([self._MockField("id", "StringType"), self._MockField("ts", "TimestampType")])
        schema_b = self._MockSchema([self._MockField("id", "StringType"), self._MockField("ts", "TimestampType"), self._MockField("revenue", "DoubleType")])
        result = diff_schemas(schema_a, schema_b)
        assert result["is_backward_compatible"] is True
        assert "revenue" in result["added_columns"]
        assert len(result["removed_columns"]) == 0

    def test_removed_column_is_incompatible(self) -> None:
        schema_a = self._MockSchema([self._MockField("id", "StringType"), self._MockField("legacy", "StringType")])
        schema_b = self._MockSchema([self._MockField("id", "StringType")])
        result = diff_schemas(schema_a, schema_b)
        assert result["is_backward_compatible"] is False
        assert "legacy" in result["removed_columns"]

    def test_type_change_is_incompatible(self) -> None:
        schema_a = self._MockSchema([self._MockField("amount", "IntegerType")])
        schema_b = self._MockSchema([self._MockField("amount", "DoubleType")])
        result = diff_schemas(schema_a, schema_b)
        assert result["is_backward_compatible"] is False
        assert "amount" in result["type_changes"]

    def test_identical_schemas(self) -> None:
        schema_a = self._MockSchema([self._MockField("id", "StringType")])
        schema_b = self._MockSchema([self._MockField("id", "StringType")])
        result = diff_schemas(schema_a, schema_b)
        assert result["is_backward_compatible"] is True
        assert len(result["added_columns"]) == 0
        assert len(result["removed_columns"]) == 0


# ---------------------------------------------------------------------------
# Pipeline metrics aggregation
# ---------------------------------------------------------------------------


class TestExtractMedallionMetrics:
    def test_basic_aggregation(self) -> None:
        bronze = {"good_records": 1000, "bad_records": 10}
        silver = {"row_count": 990, "gx_checks": 5}
        gold = {"row_count": 50, "sla_breached": False, "duration_seconds": 120.0}
        metrics = extract_medallion_metrics(bronze, silver, gold)
        assert metrics["bronze_good_records"] == 1000
        assert metrics["bronze_bad_records"] == 10
        assert metrics["silver_row_count"] == 990
        assert metrics["gold_sla_breached"] is False

    def test_quarantine_rate_calculation(self) -> None:
        bronze = {"good_records": 900, "bad_records": 100}
        silver = {"row_count": 900, "gx_checks": 3}
        gold = {"row_count": 10, "sla_breached": False, "duration_seconds": 60.0}
        metrics = extract_medallion_metrics(bronze, silver, gold)
        assert metrics["bronze_quarantine_rate"] == pytest.approx(0.1)

    def test_zero_records_no_division_error(self) -> None:
        bronze = {"good_records": 0, "bad_records": 0}
        silver = {"row_count": 0, "gx_checks": 0}
        gold = {"row_count": 0, "sla_breached": False, "duration_seconds": 0.0}
        metrics = extract_medallion_metrics(bronze, silver, gold)
        assert metrics["bronze_quarantine_rate"] == 0.0

    def test_sla_breach_propagated(self) -> None:
        bronze = {"good_records": 500, "bad_records": 0}
        silver = {"row_count": 500, "gx_checks": 2}
        gold = {"row_count": 20, "sla_breached": True, "duration_seconds": 1200.0}
        metrics = extract_medallion_metrics(bronze, silver, gold)
        assert metrics["gold_sla_breached"] is True
