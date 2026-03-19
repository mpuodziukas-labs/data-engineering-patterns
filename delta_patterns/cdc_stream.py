"""
Delta Lake Change Data Feed (CDF): consumer, downstream fan-out.
Production pattern — exactly-once downstream propagation using CDF + streaming.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Final

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)

# CDF adds these system columns to every changed row
CDF_COLUMNS: Final[list[str]] = [
    "_change_type",       # insert | update_preimage | update_postimage | delete
    "_commit_version",    # Delta table version of the change
    "_commit_timestamp",  # Wall-clock time of the commit
]

# Only postimages and inserts represent "new truth" for downstream systems
FORWARD_CHANGE_TYPES: Final[set[str]] = {"insert", "update_postimage"}


@dataclass(frozen=True)
class CdcConsumerConfig:
    source_table: str
    checkpoint_location: str
    starting_version: int = 0
    # Downstream fan-out targets: table_name → filter_expression (or None for all)
    downstream_targets: dict[str, str | None] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Read CDF changes
# ---------------------------------------------------------------------------


def read_cdf_batch(spark: SparkSession, table: str, starting_version: int, ending_version: int | None = None) -> DataFrame:
    """
    Read a bounded batch of changes from a CDF-enabled Delta table.
    Returns all change types: insert, update_preimage, update_postimage, delete.
    """
    reader = (
        spark.read.format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", starting_version)
    )

    if ending_version is not None:
        reader = reader.option("endingVersion", ending_version)

    df = reader.table(table)
    change_count = df.count()
    logger.info(
        "CDF batch read from %s vStart=%d vEnd=%s — %d change rows",
        table,
        starting_version,
        ending_version,
        change_count,
    )
    return df


def read_cdf_stream(spark: SparkSession, table: str, starting_version: int) -> DataFrame:
    """
    Open a streaming CDF reader — new commits are pushed as micro-batches.
    Use with writeStream for continuous downstream propagation.
    """
    return (
        spark.readStream.format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", starting_version)
        .table(table)
    )


# ---------------------------------------------------------------------------
# Change processing
# ---------------------------------------------------------------------------


def filter_forward_changes(df: DataFrame) -> DataFrame:
    """
    Keep only inserts and update postimages — the rows that represent current truth.
    Drops preimages and deletes (handled separately by delete propagation if needed).
    """
    return df.filter(F.col("_change_type").isin(list(FORWARD_CHANGE_TYPES)))


def extract_deletes(df: DataFrame) -> DataFrame:
    """Extract delete records for tombstone propagation to downstream systems."""
    return df.filter(F.col("_change_type") == "delete")


def strip_cdf_metadata(df: DataFrame) -> DataFrame:
    """Remove CDF system columns before writing to downstream tables."""
    cols_to_drop = [c for c in CDF_COLUMNS if c in df.columns]
    return df.drop(*cols_to_drop)


def add_cdc_lineage(df: DataFrame, source_table: str) -> DataFrame:
    """Stamp downstream rows with CDC lineage for debugging."""
    return (
        df.withColumn("_cdc_source_table", F.lit(source_table))
        .withColumn("_cdc_propagated_at", F.current_timestamp())
    )


# ---------------------------------------------------------------------------
# Downstream fan-out
# ---------------------------------------------------------------------------


def fanout_to_targets(
    df: DataFrame,
    targets: dict[str, str | None],
    source_table: str,
    spark: SparkSession,
) -> dict[str, int]:
    """
    Write changed rows to each downstream target.
    Each target can specify an optional SQL filter expression.
    Returns: {target_table: row_count_written}
    """
    results: dict[str, int] = {}

    for target_table, filter_expr in targets.items():
        target_df = df if filter_expr is None else df.filter(filter_expr)
        target_df = strip_cdf_metadata(target_df)
        target_df = add_cdc_lineage(target_df, source_table)

        row_count = target_df.count()
        if row_count == 0:
            logger.debug("No changes to propagate to %s (filter: %s)", target_table, filter_expr)
            results[target_table] = 0
            continue

        target_df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(target_table)
        logger.info("CDC fan-out → %s: %d rows", target_table, row_count)
        results[target_table] = row_count

    return results


# ---------------------------------------------------------------------------
# Streaming micro-batch handler
# ---------------------------------------------------------------------------


def process_micro_batch(
    batch_df: DataFrame,
    batch_id: int,
    config: CdcConsumerConfig,
    spark: SparkSession,
) -> None:
    """
    Structured Streaming foreachBatch handler.
    Called once per micro-batch with all CDC changes.
    """
    if batch_df.isEmpty():
        logger.debug("Batch %d: no changes", batch_id)
        return

    forward_df = filter_forward_changes(batch_df)
    delete_df = extract_deletes(batch_df)

    if not forward_df.isEmpty():
        fanout_to_targets(forward_df, config.downstream_targets, config.source_table, spark)

    delete_count = delete_df.count()
    if delete_count > 0:
        logger.info("Batch %d: %d deletes detected (tombstone propagation not configured)", batch_id, delete_count)


def run_cdc_streaming(config: CdcConsumerConfig, spark: SparkSession) -> None:
    """
    Launch streaming CDC consumer. Blocks until stream is terminated.
    """
    change_stream = read_cdf_stream(spark, config.source_table, config.starting_version)

    query = (
        change_stream.writeStream.foreachBatch(
            lambda batch_df, batch_id: process_micro_batch(batch_df, batch_id, config, spark)
        )
        .option("checkpointLocation", config.checkpoint_location)
        .trigger(processingTime="30 seconds")
        .start()
    )

    logger.info("CDC streaming consumer started for table %s", config.source_table)
    query.awaitTermination()


# ---------------------------------------------------------------------------
# Batch CDC run (for scheduled jobs / backfill)
# ---------------------------------------------------------------------------


def run_cdc_batch(
    config: CdcConsumerConfig,
    ending_version: int,
    spark: SparkSession,
) -> dict[str, Any]:
    """
    Bounded batch CDC run. Reads [starting_version, ending_version] and fans out.
    Returns a summary report.
    """
    changes_df = read_cdf_batch(spark, config.source_table, config.starting_version, ending_version)
    forward_df = filter_forward_changes(changes_df)
    delete_df = extract_deletes(changes_df)

    fan_out_results = fanout_to_targets(forward_df, config.downstream_targets, config.source_table, spark)

    return {
        "source_table": config.source_table,
        "version_range": f"{config.starting_version}..{ending_version}",
        "total_changes": changes_df.count(),
        "forward_changes": forward_df.count(),
        "deletes": delete_df.count(),
        "downstream_rows_written": fan_out_results,
    }
