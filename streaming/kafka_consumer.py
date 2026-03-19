"""
Structured Streaming from Kafka: watermarks, late data handling, exactly-once semantics.
Production pattern — handles out-of-order events up to configurable lateness threshold.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Final

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

logger = logging.getLogger(__name__)

# Default event schema — override per use case
DEFAULT_EVENT_SCHEMA: Final[StructType] = StructType(
    [
        StructField("event_id", StringType(), nullable=False),
        StructField("user_id", StringType(), nullable=False),
        StructField("event_type", StringType(), nullable=False),
        StructField("event_ts", TimestampType(), nullable=False),
        StructField("session_id", StringType(), nullable=True),
        StructField("revenue_amount", DoubleType(), nullable=True),
        StructField("product_line", StringType(), nullable=True),
        StructField("is_paid", BooleanType(), nullable=True),
        StructField("metadata", StringType(), nullable=True),
    ]
)


@dataclass(frozen=True)
class KafkaConsumerConfig:
    bootstrap_servers: str
    topic: str
    checkpoint_location: str
    output_table: str
    starting_offsets: str = "latest"       # "earliest" for backfill, "latest" for production
    watermark_delay: str = "10 minutes"    # Tolerate up to N late data
    trigger_interval: str = "1 minute"    # Micro-batch frequency
    max_offsets_per_trigger: int = 100_000
    event_schema: StructType = field(default_factory=lambda: DEFAULT_EVENT_SCHEMA)
    # Set to a Kafka group ID for consumer group tracking
    kafka_group_id: str = "exo-structured-streaming"


# ---------------------------------------------------------------------------
# Kafka source
# ---------------------------------------------------------------------------


def build_kafka_source(spark: SparkSession, config: KafkaConsumerConfig) -> DataFrame:
    """
    Create a streaming DataFrame from a Kafka topic.
    Kafka messages are read as binary (key, value) + metadata columns.
    """
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config.bootstrap_servers)
        .option("subscribe", config.topic)
        .option("startingOffsets", config.starting_offsets)
        .option("maxOffsetsPerTrigger", config.max_offsets_per_trigger)
        .option("kafka.group.id", config.kafka_group_id)
        .option("failOnDataLoss", "false")  # Don't fail if Kafka retention expired old offsets
        .load()
    )


# ---------------------------------------------------------------------------
# Deserialization
# ---------------------------------------------------------------------------


def deserialize_json_payload(df: DataFrame, schema: StructType) -> DataFrame:
    """
    Parse JSON from the Kafka value column.
    Malformed records produce NULL fields — handled downstream by DLQ logic.
    """
    return (
        df.select(
            F.col("offset").alias("kafka_offset"),
            F.col("partition").alias("kafka_partition"),
            F.col("timestamp").alias("kafka_ts"),
            F.from_json(F.col("value").cast(StringType()), schema).alias("payload"),
        )
        .select("kafka_offset", "kafka_partition", "kafka_ts", "payload.*")
    )


# ---------------------------------------------------------------------------
# Watermark + windowing
# ---------------------------------------------------------------------------


def apply_watermark(df: DataFrame, event_time_col: str, delay: str) -> DataFrame:
    """
    Apply event-time watermark for late data tolerance.
    Spark will hold state for events up to `delay` after the current watermark.
    Records arriving later than the watermark threshold are DROPPED (by design).
    """
    return df.withWatermark(event_time_col, delay)


def window_aggregate(df: DataFrame, event_time_col: str, window_duration: str, slide_duration: str | None = None) -> DataFrame:
    """
    Sliding window aggregation on event time.
    window_duration="5 minutes", slide_duration="1 minute" → overlapping 5-minute windows.
    slide_duration=None → tumbling (non-overlapping) window.
    """
    if slide_duration:
        window_expr = F.window(event_time_col, window_duration, slide_duration)
    else:
        window_expr = F.window(event_time_col, window_duration)

    return (
        df.groupBy(window_expr, "event_type", "product_line")
        .agg(
            F.count("event_id").alias("event_count").cast(LongType()),
            F.sum("revenue_amount").alias("total_revenue").cast(DoubleType()),
            F.countDistinct("user_id").alias("unique_users").cast(LongType()),
            F.countDistinct("session_id").alias("unique_sessions").cast(LongType()),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "event_type",
            "product_line",
            "event_count",
            "total_revenue",
            "unique_users",
            "unique_sessions",
        )
    )


# ---------------------------------------------------------------------------
# Bad record detection
# ---------------------------------------------------------------------------


def split_good_bad(df: DataFrame, required_cols: list[str]) -> tuple[DataFrame, DataFrame]:
    """
    Split deserialized records into good records and bad records (DLQ candidates).
    A record is bad if any required column is NULL post-deserialization.
    """
    null_condition = F.lit(False)
    for col_name in required_cols:
        null_condition = null_condition | F.col(col_name).isNull()

    good_df = df.filter(~null_condition)
    bad_df = df.filter(null_condition).withColumn("dlq_reason", F.lit("NULL_REQUIRED_FIELD")).withColumn("dlq_at", F.current_timestamp())

    return good_df, bad_df


# ---------------------------------------------------------------------------
# Streaming write
# ---------------------------------------------------------------------------


def write_stream_delta(
    df: DataFrame,
    output_table: str,
    checkpoint_location: str,
    trigger_interval: str,
    output_mode: str = "append",
) -> Any:
    """Write a streaming DataFrame to a Delta table."""
    return (
        df.writeStream.format("delta")
        .outputMode(output_mode)
        .option("checkpointLocation", checkpoint_location)
        .trigger(processingTime=trigger_interval)
        .toTable(output_table)
    )


# ---------------------------------------------------------------------------
# Full pipeline
# ---------------------------------------------------------------------------


def run_kafka_to_delta(config: KafkaConsumerConfig, spark: SparkSession | None = None) -> None:
    """
    End-to-end Kafka → Delta pipeline with watermarks, bad record isolation, and
    windowed aggregation written to the output table.
    """
    spark = spark or SparkSession.getActiveSession()
    if spark is None:
        raise RuntimeError("No active SparkSession")

    raw_stream = build_kafka_source(spark, config)
    parsed_stream = deserialize_json_payload(raw_stream, config.event_schema)

    required_cols = ["event_id", "user_id", "event_type", "event_ts"]
    good_stream, bad_stream = split_good_bad(parsed_stream, required_cols)

    # Route bad records to DLQ table
    dlq_query = write_stream_delta(
        bad_stream,
        output_table=f"{config.output_table}_dlq",
        checkpoint_location=f"{config.checkpoint_location}_dlq",
        trigger_interval=config.trigger_interval,
    )

    # Apply watermark and 5-minute tumbling window aggregation
    watermarked = apply_watermark(good_stream, "event_ts", config.watermark_delay)
    windowed = window_aggregate(watermarked, "event_ts", window_duration="5 minutes")

    agg_query = write_stream_delta(
        windowed,
        output_table=config.output_table,
        checkpoint_location=config.checkpoint_location,
        trigger_interval=config.trigger_interval,
        output_mode="update",
    )

    logger.info(
        "Kafka → Delta pipeline started — topic=%s, output=%s",
        config.topic,
        config.output_table,
    )

    agg_query.awaitTermination()
