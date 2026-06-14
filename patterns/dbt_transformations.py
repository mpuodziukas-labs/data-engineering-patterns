"""
dbt model quality gates — pure Python implementation.

Demonstrates:
- Source freshness assertions (configurable warn/error thresholds)
- Column-level lineage tracking (source → staging → intermediate → mart)
- Test coverage percentage calculator (columns tested vs total)
- Model dependency graph validator (cycle detection, orphan detection)

Design philosophy:
    All functions here are pure Python — no dbt CLI or runtime required.
    They parse dbt schema.yml structures (as Python dicts) and produce
    structured reports suitable for CI gates, Slack alerts, or dashboards.

    In production, these integrate with dbt's JSON artifacts:
    - manifest.json → model dependency graph
    - sources.json → source freshness checks
    - run_results.json → test coverage

Usage:
    # Source freshness
    result = check_source_freshness(source_meta, last_loaded_at)

    # Lineage
    tracker = ColumnLineageTracker()
    tracker.register_model("stg_events", inputs=["raw.events"], columns=["event_id", "user_id"])
    lineage = tracker.get_column_lineage("mart_daily_metrics", "event_id")

    # Coverage
    report = compute_test_coverage(manifest_models)

    # Dependency graph
    graph = DependencyGraph(models)
    issues = graph.validate()
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Source freshness
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class FreshnessThreshold:
    warn_hours: int
    error_hours: int
    source_name: str
    table_name: str
    loaded_at_field: str = "_ingested_at"

    def __post_init__(self) -> None:
        if self.warn_hours >= self.error_hours:
            raise ValueError(
                f"warn_hours ({self.warn_hours}) must be < error_hours ({self.error_hours})"
            )
        if self.warn_hours < 1:
            raise ValueError("warn_hours must be >= 1")


@dataclass(frozen=True)
class FreshnessResult:
    source_name: str
    table_name: str
    last_loaded_at: datetime | None
    age_hours: float | None
    status: str  # 'pass', 'warn', 'error', 'unknown'
    threshold: FreshnessThreshold
    message: str


def check_source_freshness(
    threshold: FreshnessThreshold,
    last_loaded_at: datetime | None,
    as_of: datetime | None = None,
) -> FreshnessResult:
    """
    Assert that a source table was loaded within the configured freshness window.

    This mirrors dbt's source freshness check behavior:
    - age < warn_hours → pass
    - warn_hours ≤ age < error_hours → warn
    - age ≥ error_hours → error
    - last_loaded_at is None → unknown (treat as error in CI)

    Args:
        threshold: Configured freshness thresholds for this source.
        last_loaded_at: Timestamp of the most recent record in the source table.
            Typically MAX(_ingested_at) from the Bronze layer.
        as_of: Reference timestamp for age calculation. Defaults to now (UTC).

    Returns:
        FreshnessResult with status, age_hours, and human-readable message.

    When to use:
        - In CI pipelines before running dbt models that depend on this source
        - In Airflow sensors to gate downstream DAG tasks
        - In dbt's source block under 'freshness:' key

    Anti-patterns:
        - Using wall-clock time instead of MAX(loaded_at) — skewed by DST or NTP drift
        - Setting error_hours too high (>48h) — silently stale sources go undetected
        - Not checking freshness for high-value tables (revenue, orders)
    """
    now = as_of or datetime.now(timezone.utc)

    if last_loaded_at is None:
        return FreshnessResult(
            source_name=threshold.source_name,
            table_name=threshold.table_name,
            last_loaded_at=None,
            age_hours=None,
            status="unknown",
            threshold=threshold,
            message=f"{threshold.source_name}.{threshold.table_name}: no data found — source may be empty",
        )

    # Ensure timezone-aware comparison
    if last_loaded_at.tzinfo is None:
        last_loaded_at = last_loaded_at.replace(tzinfo=timezone.utc)

    age = now - last_loaded_at
    age_hours = age.total_seconds() / 3600.0

    if age_hours < threshold.warn_hours:
        status = "pass"
        message = (
            f"{threshold.source_name}.{threshold.table_name}: "
            f"fresh ({age_hours:.1f}h < {threshold.warn_hours}h warn threshold)"
        )
    elif age_hours < threshold.error_hours:
        status = "warn"
        message = (
            f"{threshold.source_name}.{threshold.table_name}: "
            f"stale ({age_hours:.1f}h ≥ {threshold.warn_hours}h warn threshold, "
            f"< {threshold.error_hours}h error threshold)"
        )
    else:
        status = "error"
        message = (
            f"{threshold.source_name}.{threshold.table_name}: "
            f"STALE ERROR ({age_hours:.1f}h ≥ {threshold.error_hours}h error threshold)"
        )

    logger.log(
        logging.ERROR if status == "error" else logging.WARNING if status == "warn" else logging.INFO,
        "source_freshness: %s",
        message,
    )

    return FreshnessResult(
        source_name=threshold.source_name,
        table_name=threshold.table_name,
        last_loaded_at=last_loaded_at,
        age_hours=round(age_hours, 3),
        status=status,
        threshold=threshold,
        message=message,
    )


def check_all_sources(
    thresholds: list[FreshnessThreshold],
    last_loaded_map: dict[str, datetime | None],
    as_of: datetime | None = None,
) -> dict[str, Any]:
    """
    Run freshness checks for all configured sources.

    Args:
        thresholds: List of FreshnessThreshold configs.
        last_loaded_map: Map of '<source>.<table>' → last loaded timestamp.
        as_of: Reference time for age calculation.

    Returns:
        Dict with results list, summary counts, and overall_status.
        overall_status is the worst status across all sources.
    """
    results = []
    for t in thresholds:
        key = f"{t.source_name}.{t.table_name}"
        last_loaded = last_loaded_map.get(key)
        results.append(check_source_freshness(t, last_loaded, as_of))

    status_priority = {"error": 3, "unknown": 2, "warn": 1, "pass": 0}
    overall = max(results, key=lambda r: status_priority.get(r.status, 0), default=None)

    return {
        "results": results,
        "total": len(results),
        "pass_count": sum(1 for r in results if r.status == "pass"),
        "warn_count": sum(1 for r in results if r.status == "warn"),
        "error_count": sum(1 for r in results if r.status == "error"),
        "unknown_count": sum(1 for r in results if r.status == "unknown"),
        "overall_status": overall.status if overall else "pass",
    }


# ---------------------------------------------------------------------------
# Column-level lineage
# ---------------------------------------------------------------------------


@dataclass
class ModelLineageNode:
    model_name: str
    layer: str  # 'source', 'staging', 'intermediate', 'mart'
    columns: list[str] = field(default_factory=list)
    upstream_models: list[str] = field(default_factory=list)
    sql_path: str = ""


class ColumnLineageTracker:
    """
    Track column-level lineage across dbt model layers.

    Lineage is stored as a directed graph: column in model A flows to model B
    if model B selects that column from model A (directly or via *).

    This is a lightweight implementation — for production use, dbt's
    column-level lineage is best captured via dbt-column-lineage or
    Elementary's artifact parsing.
    """

    def __init__(self) -> None:
        self._models: dict[str, ModelLineageNode] = {}
        self._column_sources: dict[str, dict[str, str]] = {}
        # _column_sources[model_name][column_name] = source_model_name

    def register_model(
        self,
        model_name: str,
        layer: str,
        columns: list[str],
        upstream_models: list[str] | None = None,
        sql_path: str = "",
    ) -> None:
        """
        Register a dbt model with its columns and upstream dependencies.

        Args:
            model_name: dbt model name (matches schema.yml key).
            layer: Medallion layer: 'source', 'staging', 'intermediate', or 'mart'.
            columns: List of output column names.
            upstream_models: List of upstream model names this model depends on.
            sql_path: Path to the .sql file (optional, for display).
        """
        self._models[model_name] = ModelLineageNode(
            model_name=model_name,
            layer=layer,
            columns=columns,
            upstream_models=upstream_models or [],
            sql_path=sql_path,
        )

    def get_column_lineage(self, model_name: str, column_name: str) -> list[dict[str, str]]:
        """
        Trace a column back through all upstream models to its original source.

        Returns a list of lineage steps from source to the target model,
        ordered from earliest (source table) to latest (target model).

        Args:
            model_name: Target model name.
            column_name: Column to trace.

        Returns:
            List of dicts with model_name, layer, column_name at each hop.
        """
        if model_name not in self._models:
            return []

        lineage: list[dict[str, str]] = []
        visited: set[str] = set()

        def _trace(current_model: str, col: str) -> None:
            if current_model in visited:
                return
            visited.add(current_model)

            node = self._models.get(current_model)
            if node is None:
                return

            if col in node.columns:
                lineage.append({
                    "model": current_model,
                    "layer": node.layer,
                    "column": col,
                })
                for upstream in node.upstream_models:
                    _trace(upstream, col)

        _trace(model_name, column_name)
        lineage.reverse()  # Source → target order
        return lineage

    def get_model_columns(self, model_name: str) -> list[str]:
        node = self._models.get(model_name)
        return node.columns if node else []

    def list_models(self) -> list[str]:
        return list(self._models.keys())

    def get_upstream_chain(self, model_name: str) -> list[str]:
        """Return all transitive upstream models for a given model."""
        visited: set[str] = set()
        chain: list[str] = []

        def _walk(name: str) -> None:
            if name in visited:
                return
            visited.add(name)
            node = self._models.get(name)
            if node:
                for upstream in node.upstream_models:
                    _walk(upstream)
                    chain.append(upstream)

        _walk(model_name)
        return chain


# ---------------------------------------------------------------------------
# Test coverage calculator
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ColumnTestCoverage:
    model_name: str
    total_columns: int
    tested_columns: int
    coverage_pct: float
    untested_columns: list[str]
    test_counts: dict[str, int]  # column → number of tests


def compute_test_coverage(schema_model: dict[str, Any]) -> ColumnTestCoverage:
    """
    Compute test coverage percentage for a single dbt model from its schema.yml entry.

    A column is "tested" if it has at least one test defined (unique, not_null,
    accepted_values, dbt_utils.accepted_range, or custom).

    Args:
        schema_model: A dict matching the dbt schema.yml model entry:
            {
                "name": "stg_events",
                "columns": [
                    {"name": "event_id", "tests": ["unique", "not_null"]},
                    {"name": "user_id", "tests": ["not_null"]},
                    {"name": "revenue_amount"},  # no tests
                ]
            }

    Returns:
        ColumnTestCoverage with coverage_pct and list of untested columns.

    When to use:
        - In CI to enforce minimum test coverage (e.g., ≥80% of columns tested)
        - In dbt PR reviews to flag models with no tests on critical columns
        - In data quality dashboards to track coverage trend over time

    Anti-patterns:
        - Counting model-level tests (source freshness) as column tests
        - Treating 'not_null' on a nullable column as meaningful coverage
        - High coverage % with only trivial tests (all not_null, no uniqueness)
    """
    columns: list[dict[str, Any]] = schema_model.get("columns", [])
    model_name: str = schema_model.get("name", "unknown")

    if not columns:
        return ColumnTestCoverage(
            model_name=model_name,
            total_columns=0,
            tested_columns=0,
            coverage_pct=0.0,
            untested_columns=[],
            test_counts={},
        )

    tested_columns: list[str] = []
    untested_columns: list[str] = []
    test_counts: dict[str, int] = {}

    for col in columns:
        col_name: str = col.get("name", "")
        tests: list[Any] = col.get("tests", [])
        test_count = len(tests)
        test_counts[col_name] = test_count
        if test_count > 0:
            tested_columns.append(col_name)
        else:
            untested_columns.append(col_name)

    coverage_pct = round(len(tested_columns) / len(columns) * 100, 1) if columns else 0.0

    return ColumnTestCoverage(
        model_name=model_name,
        total_columns=len(columns),
        tested_columns=len(tested_columns),
        coverage_pct=coverage_pct,
        untested_columns=untested_columns,
        test_counts=test_counts,
    )


def compute_project_coverage(schema_models: list[dict[str, Any]]) -> dict[str, Any]:
    """
    Compute test coverage across all models in a dbt project.

    Args:
        schema_models: List of dbt schema.yml model entries.

    Returns:
        Dict with per_model results, project_coverage_pct, and models_below_threshold.
    """
    results = [compute_test_coverage(m) for m in schema_models]

    total_cols = sum(r.total_columns for r in results)
    total_tested = sum(r.tested_columns for r in results)
    project_pct = round(total_tested / total_cols * 100, 1) if total_cols > 0 else 0.0

    return {
        "per_model": [
            {
                "model": r.model_name,
                "coverage_pct": r.coverage_pct,
                "total_columns": r.total_columns,
                "tested_columns": r.tested_columns,
                "untested_columns": r.untested_columns,
            }
            for r in results
        ],
        "project_coverage_pct": project_pct,
        "total_columns": total_cols,
        "total_tested_columns": total_tested,
    }


# ---------------------------------------------------------------------------
# Model dependency graph validator
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class DependencyIssue:
    issue_type: str  # 'cycle', 'missing_dependency', 'orphan'
    model_name: str
    detail: str


class DependencyGraph:
    """
    Validate a dbt model dependency graph for structural issues.

    Detects:
    - Cycles: model A depends on B which depends on A (would deadlock dbt run)
    - Missing dependencies: model references an upstream that is not defined
    - Orphans: model with no consumers and no downstream models

    In production, the dependency graph is built from dbt's manifest.json:
        manifest = json.load(open('target/manifest.json'))
        nodes = manifest['nodes']
        graph = DependencyGraph({
            k: v['depends_on']['nodes'] for k, v in nodes.items()
        })
    """

    def __init__(self, dependencies: dict[str, list[str]]) -> None:
        """
        Args:
            dependencies: Map of model_name → list of upstream model names.
                Example: {"mart_daily": ["int_sessions", "stg_events"], "int_sessions": ["stg_events"]}
        """
        self._dependencies = {k: list(v) for k, v in dependencies.items()}

    def detect_cycles(self) -> list[DependencyIssue]:
        """
        Detect cycles using DFS with three-color marking.

        WHITE (0) = unvisited, GRAY (1) = in current DFS path, BLACK (2) = done.
        A back edge (node with GRAY ancestor) indicates a cycle.
        """
        WHITE, GRAY, BLACK = 0, 1, 2
        color: dict[str, int] = {m: WHITE for m in self._dependencies}
        issues: list[DependencyIssue] = []

        def _dfs(node: str, path: list[str]) -> None:
            color[node] = GRAY
            for upstream in self._dependencies.get(node, []):
                if upstream not in color:
                    continue  # External / source — skip
                if color[upstream] == GRAY:
                    cycle_path = path + [upstream]
                    issues.append(DependencyIssue(
                        issue_type="cycle",
                        model_name=node,
                        detail=f"Cycle detected: {' → '.join(cycle_path)}",
                    ))
                elif color[upstream] == WHITE:
                    _dfs(upstream, path + [upstream])
            color[node] = BLACK

        for model in list(self._dependencies):
            if color[model] == WHITE:
                _dfs(model, [model])

        return issues

    def detect_missing_dependencies(self) -> list[DependencyIssue]:
        """
        Detect references to models that are not defined in the graph.

        A model that references an undefined upstream will cause a dbt compile error.
        """
        defined = set(self._dependencies.keys())
        issues: list[DependencyIssue] = []

        for model, upstreams in self._dependencies.items():
            for upstream in upstreams:
                if upstream not in defined:
                    issues.append(DependencyIssue(
                        issue_type="missing_dependency",
                        model_name=model,
                        detail=f"Model {model!r} depends on {upstream!r} which is not defined",
                    ))

        return issues

    def detect_orphans(self) -> list[DependencyIssue]:
        """
        Detect models that have no consumers (no model depends on them).

        Orphan models may indicate dead code — models that are no longer
        used by any downstream mart or dashboard.

        Note: mart / final layer models are expected to have no consumers
        and should be excluded from orphan detection in practice.
        """
        all_upstreams: set[str] = set()
        for upstreams in self._dependencies.values():
            all_upstreams.update(upstreams)

        issues: list[DependencyIssue] = []
        for model in self._dependencies:
            if model not in all_upstreams:
                issues.append(DependencyIssue(
                    issue_type="orphan",
                    model_name=model,
                    detail=f"Model {model!r} has no downstream consumers (may be dead code)",
                ))

        return issues

    def validate(self, check_orphans: bool = False) -> list[DependencyIssue]:
        """
        Run all validation checks and return aggregated issues.

        Args:
            check_orphans: Include orphan detection (default False — mart models are orphans by design).

        Returns:
            List of DependencyIssue. Empty list = graph is valid.
        """
        issues = self.detect_cycles() + self.detect_missing_dependencies()
        if check_orphans:
            issues += self.detect_orphans()
        return issues

    def topological_sort(self) -> list[str]:
        """
        Return models in dependency order (sources before dependents).

        Uses Kahn's algorithm (BFS-based topo sort).

        Raises:
            ValueError: If the graph contains a cycle.
        """
        in_degree: dict[str, int] = {m: 0 for m in self._dependencies}
        for upstreams in self._dependencies.values():
            for upstream in upstreams:
                if upstream in in_degree:
                    in_degree[upstream] += 0  # upstream is a predecessor, don't add to its in_degree

        # Recalculate: in_degree[m] = number of models that depend on m
        in_degree = {m: 0 for m in self._dependencies}
        for model, upstreams in self._dependencies.items():
            for upstream in upstreams:
                if upstream in in_degree:
                    pass  # upstream has no additional in-degree from this edge

        # Standard Kahn's: count how many things point to each node
        reverse: dict[str, list[str]] = {m: [] for m in self._dependencies}
        for model, upstreams in self._dependencies.items():
            for upstream in upstreams:
                if upstream in reverse:
                    reverse[upstream].append(model)

        in_deg: dict[str, int] = {m: len(self._dependencies.get(m, [])) for m in self._dependencies}
        queue = [m for m, d in in_deg.items() if d == 0]
        result: list[str] = []

        while queue:
            node = queue.pop(0)
            result.append(node)
            for downstream in reverse.get(node, []):
                in_deg[downstream] -= 1
                if in_deg[downstream] == 0:
                    queue.append(downstream)

        if len(result) != len(self._dependencies):
            raise ValueError("Cycle detected — topological sort impossible")

        return result
