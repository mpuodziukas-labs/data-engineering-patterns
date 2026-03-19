"""
Tests for dbt_transformations.py — 10 tests.

All tests are pure Python — no dbt CLI, no database connection required.
They verify freshness assertion logic, lineage tracking, test coverage
calculation, and dependency graph validation.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from patterns.dbt_transformations import (
    ColumnLineageTracker,
    DependencyGraph,
    FreshnessThreshold,
    check_all_sources,
    check_source_freshness,
    compute_project_coverage,
    compute_test_coverage,
)


# ---------------------------------------------------------------------------
# Source freshness
# ---------------------------------------------------------------------------


class TestCheckSourceFreshness:
    def _threshold(self) -> FreshnessThreshold:
        return FreshnessThreshold(
            warn_hours=12,
            error_hours=24,
            source_name="raw",
            table_name="events",
        )

    def test_fresh_source_passes(self) -> None:
        now = datetime(2024, 3, 1, 12, 0, 0, tzinfo=timezone.utc)
        last_loaded = now - timedelta(hours=6)
        result = check_source_freshness(self._threshold(), last_loaded, as_of=now)
        assert result.status == "pass"

    def test_stale_warn(self) -> None:
        now = datetime(2024, 3, 1, 12, 0, 0, tzinfo=timezone.utc)
        last_loaded = now - timedelta(hours=18)
        result = check_source_freshness(self._threshold(), last_loaded, as_of=now)
        assert result.status == "warn"

    def test_stale_error(self) -> None:
        now = datetime(2024, 3, 1, 12, 0, 0, tzinfo=timezone.utc)
        last_loaded = now - timedelta(hours=30)
        result = check_source_freshness(self._threshold(), last_loaded, as_of=now)
        assert result.status == "error"

    def test_none_last_loaded_is_unknown(self) -> None:
        result = check_source_freshness(self._threshold(), None)
        assert result.status == "unknown"
        assert result.age_hours is None

    def test_warn_error_parity_raises(self) -> None:
        with pytest.raises(ValueError, match="warn_hours"):
            FreshnessThreshold(warn_hours=24, error_hours=24, source_name="s", table_name="t")

    def test_naive_datetime_handled(self) -> None:
        """Naive datetimes (no tz) should be treated as UTC, not raise."""
        now = datetime(2024, 3, 1, 12, 0, 0, tzinfo=timezone.utc)
        last_loaded = datetime(2024, 3, 1, 6, 0, 0)  # naive
        result = check_source_freshness(self._threshold(), last_loaded, as_of=now)
        assert result.status in ("pass", "warn", "error")

    def test_check_all_sources_overall_status(self) -> None:
        thresholds = [
            FreshnessThreshold(warn_hours=12, error_hours=24, source_name="raw", table_name="events"),
            FreshnessThreshold(warn_hours=6, error_hours=12, source_name="raw", table_name="users"),
        ]
        now = datetime(2024, 3, 1, 12, 0, 0, tzinfo=timezone.utc)
        last_loaded_map = {
            "raw.events": now - timedelta(hours=1),
            "raw.users": now - timedelta(hours=20),  # error
        }
        summary = check_all_sources(thresholds, last_loaded_map, as_of=now)
        assert summary["overall_status"] == "error"
        assert summary["error_count"] == 1
        assert summary["pass_count"] == 1


# ---------------------------------------------------------------------------
# Test coverage
# ---------------------------------------------------------------------------


class TestComputeTestCoverage:
    def test_fully_tested_model(self) -> None:
        model = {
            "name": "stg_events",
            "columns": [
                {"name": "event_id", "tests": ["unique", "not_null"]},
                {"name": "user_id", "tests": ["not_null"]},
            ],
        }
        result = compute_test_coverage(model)
        assert result.coverage_pct == 100.0
        assert result.tested_columns == 2
        assert len(result.untested_columns) == 0

    def test_partial_coverage(self) -> None:
        model = {
            "name": "int_sessions",
            "columns": [
                {"name": "session_id", "tests": ["unique", "not_null"]},
                {"name": "revenue"},  # no tests
                {"name": "user_id"},  # no tests
            ],
        }
        result = compute_test_coverage(model)
        assert result.coverage_pct == pytest.approx(33.3, abs=0.1)
        assert result.tested_columns == 1
        assert "revenue" in result.untested_columns
        assert "user_id" in result.untested_columns

    def test_project_coverage_aggregation(self) -> None:
        models = [
            {"name": "stg_a", "columns": [{"name": "id", "tests": ["not_null"]}, {"name": "val"}]},
            {"name": "stg_b", "columns": [{"name": "id", "tests": ["not_null"]}, {"name": "ts", "tests": ["not_null"]}]},
        ]
        report = compute_project_coverage(models)
        # 3 tested out of 4 total = 75%
        assert report["project_coverage_pct"] == pytest.approx(75.0)
        assert report["total_columns"] == 4
        assert report["total_tested_columns"] == 3


# ---------------------------------------------------------------------------
# Dependency graph
# ---------------------------------------------------------------------------


class TestDependencyGraph:
    def test_no_cycles_valid_graph(self) -> None:
        deps = {
            "stg_events": [],
            "int_sessions": ["stg_events"],
            "mart_daily": ["int_sessions", "stg_events"],
        }
        graph = DependencyGraph(deps)
        issues = graph.validate()
        cycle_issues = [i for i in issues if i.issue_type == "cycle"]
        assert len(cycle_issues) == 0

    def test_cycle_detected(self) -> None:
        deps = {
            "model_a": ["model_b"],
            "model_b": ["model_c"],
            "model_c": ["model_a"],  # cycle
        }
        graph = DependencyGraph(deps)
        cycles = graph.detect_cycles()
        assert len(cycles) > 0
        assert all(i.issue_type == "cycle" for i in cycles)

    def test_topological_sort_order(self) -> None:
        deps = {
            "stg_events": [],
            "int_sessions": ["stg_events"],
            "mart_daily": ["int_sessions"],
        }
        graph = DependencyGraph(deps)
        order = graph.topological_sort()
        # stg_events must come before int_sessions which must come before mart_daily
        assert order.index("stg_events") < order.index("int_sessions")
        assert order.index("int_sessions") < order.index("mart_daily")

    def test_topological_sort_raises_on_cycle(self) -> None:
        deps = {"a": ["b"], "b": ["a"]}
        graph = DependencyGraph(deps)
        with pytest.raises(ValueError, match="Cycle detected"):
            graph.topological_sort()


# ---------------------------------------------------------------------------
# Column lineage
# ---------------------------------------------------------------------------


class TestColumnLineageTracker:
    def _build_tracker(self) -> ColumnLineageTracker:
        tracker = ColumnLineageTracker()
        tracker.register_model(
            "raw_events",
            layer="source",
            columns=["event_id", "user_id", "event_type", "event_ts"],
        )
        tracker.register_model(
            "stg_events",
            layer="staging",
            columns=["event_id", "user_id", "event_type", "event_ts"],
            upstream_models=["raw_events"],
        )
        tracker.register_model(
            "int_sessions",
            layer="intermediate",
            columns=["session_id", "user_id", "event_count"],
            upstream_models=["stg_events"],
        )
        tracker.register_model(
            "mart_daily",
            layer="mart",
            columns=["event_date", "user_id", "event_count", "revenue"],
            upstream_models=["int_sessions", "stg_events"],
        )
        return tracker

    def test_lineage_traces_to_source(self) -> None:
        tracker = self._build_tracker()
        lineage = tracker.get_column_lineage("mart_daily", "user_id")
        layers = [step["layer"] for step in lineage]
        assert "source" in layers
        assert layers[0] == "source"
        assert layers[-1] == "mart"

    def test_unknown_model_returns_empty(self) -> None:
        tracker = self._build_tracker()
        lineage = tracker.get_column_lineage("nonexistent_model", "event_id")
        assert lineage == []

    def test_upstream_chain(self) -> None:
        tracker = self._build_tracker()
        chain = tracker.get_upstream_chain("mart_daily")
        assert "stg_events" in chain
        assert "int_sessions" in chain
