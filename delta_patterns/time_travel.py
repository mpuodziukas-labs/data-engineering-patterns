"""
Delta Lake Time Travel: VERSION AS OF, TIMESTAMP AS OF, RESTORE TABLE, DESCRIBE HISTORY.
Production pattern — audit trail queries, point-in-time recovery, compliance snapshots.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class TimeTravelConfig:
    table_name: str
    spark: SparkSession


# ---------------------------------------------------------------------------
# Time Travel Reads
# ---------------------------------------------------------------------------


def read_version(config: TimeTravelConfig, version: int) -> DataFrame:
    """
    Read a specific Delta table version.
    Equivalent to: SELECT * FROM table VERSION AS OF <version>
    """
    df = config.spark.read.format("delta").option("versionAsOf", version).table(config.table_name)
    logger.info("Read table %s at VERSION %d — %d rows", config.table_name, version, df.count())
    return df


def read_timestamp(config: TimeTravelConfig, timestamp: datetime) -> DataFrame:
    """
    Read the table as it existed at a specific point in time.
    Equivalent to: SELECT * FROM table TIMESTAMP AS OF '<timestamp>'
    """
    ts_str = timestamp.strftime("%Y-%m-%d %H:%M:%S")
    df = (
        config.spark.read.format("delta")
        .option("timestampAsOf", ts_str)
        .table(config.table_name)
    )
    logger.info("Read table %s at TIMESTAMP %s — %d rows", config.table_name, ts_str, df.count())
    return df


def read_version_sql(config: TimeTravelConfig, version: int) -> DataFrame:
    """SQL-syntax time travel — works in Databricks notebooks naturally."""
    return config.spark.sql(f"SELECT * FROM {config.table_name} VERSION AS OF {version}")


def read_timestamp_sql(config: TimeTravelConfig, timestamp: datetime) -> DataFrame:
    ts_str = timestamp.strftime("%Y-%m-%d %H:%M:%S")
    return config.spark.sql(f"SELECT * FROM {config.table_name} TIMESTAMP AS OF '{ts_str}'")


# ---------------------------------------------------------------------------
# History & Audit
# ---------------------------------------------------------------------------


def describe_history(config: TimeTravelConfig, limit: int = 20) -> DataFrame:
    """
    Show operation history for the table.
    Returns columns: version, timestamp, userId, userName, operation, operationParameters, etc.
    """
    history_df = config.spark.sql(f"DESCRIBE HISTORY {config.table_name} LIMIT {limit}")
    history_df.show(truncate=False)
    return history_df


def get_latest_version(config: TimeTravelConfig) -> int:
    """Return the current (latest) version number of the Delta table."""
    history_df = config.spark.sql(f"DESCRIBE HISTORY {config.table_name} LIMIT 1")
    row = history_df.first()
    if row is None:
        raise RuntimeError(f"Table {config.table_name!r} has no history — does it exist?")
    version: int = row["version"]
    return version


def diff_versions(config: TimeTravelConfig, version_a: int, version_b: int) -> dict[str, Any]:
    """
    Compute a row-count and schema diff between two table versions.
    Useful for change auditing and data drift detection.
    """
    df_a = read_version(config, version_a)
    df_b = read_version(config, version_b)

    count_a = df_a.count()
    count_b = df_b.count()

    cols_a = set(df_a.columns)
    cols_b = set(df_b.columns)

    return {
        "version_a": version_a,
        "version_b": version_b,
        "row_count_a": count_a,
        "row_count_b": count_b,
        "row_delta": count_b - count_a,
        "added_columns": sorted(cols_b - cols_a),
        "removed_columns": sorted(cols_a - cols_b),
    }


# ---------------------------------------------------------------------------
# RESTORE TABLE
# ---------------------------------------------------------------------------


def restore_to_version(config: TimeTravelConfig, target_version: int) -> None:
    """
    Restore the Delta table to a prior version.
    WARNING: This modifies the table in-place. Always verify the target version first.
    """
    current = get_latest_version(config)
    if target_version >= current:
        raise ValueError(
            f"Cannot restore to version {target_version} — current version is {current}. "
            "Target must be strictly less than current."
        )
    logger.warning(
        "RESTORING %s from version %d to version %d",
        config.table_name,
        current,
        target_version,
    )
    config.spark.sql(f"RESTORE TABLE {config.table_name} TO VERSION AS OF {target_version}")
    logger.info("Restore complete — table is now at version %d", target_version)


def restore_to_timestamp(config: TimeTravelConfig, target_timestamp: datetime) -> None:
    """Restore the table to the state at a given timestamp."""
    ts_str = target_timestamp.strftime("%Y-%m-%d %H:%M:%S")
    logger.warning("RESTORING %s to TIMESTAMP %s", config.table_name, ts_str)
    config.spark.sql(f"RESTORE TABLE {config.table_name} TO TIMESTAMP AS OF '{ts_str}'")
    logger.info("Restore to timestamp %s complete", ts_str)


# ---------------------------------------------------------------------------
# VACUUM (with safety guard)
# ---------------------------------------------------------------------------


def vacuum_table(config: TimeTravelConfig, retain_hours: int = 168) -> None:
    """
    Run VACUUM to remove files older than retain_hours (default 7 days).
    Enforces the Databricks minimum of 168 hours unless explicitly overridden.
    """
    if retain_hours < 168:
        logger.warning(
            "retain_hours=%d is below the safe minimum of 168. "
            "Overriding to 168 to protect time travel history.",
            retain_hours,
        )
        retain_hours = 168

    config.spark.sql(f"VACUUM {config.table_name} RETAIN {retain_hours} HOURS")
    logger.info("VACUUM complete on %s (retain %d hours)", config.table_name, retain_hours)
