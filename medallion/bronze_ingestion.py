"""
Bronze Layer: Raw ingestion with schema inference, bad record quarantine.
Production pattern — tolerates malformed records without killing the pipeline.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Final

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

logger = logging.getLogger(__name__)

QUARANTINE_SCHEMA: Final[StructType] = StructType(
    [
        StructField("raw_value", StringType(), nullable=True),
        StructField("source_path", StringType(), nullable=False),
        StructField("error_message", StringType(), nullable=False),
        StructField("quarantined_at", TimestampType(), nullable=False),
        StructField("ingestion_batch_id", StringType(), nullable=False),
    ]
)


@dataclass(frozen=True)
class BronzeIngestionConfig:
    source_path: str
    bronze_table: str
    quarantine_table: str
    checkpoint_location: str
    batch_id: str
    format: str = "json"
    max_files_per_trigger: int = 100
    bad_records_path: str = "/tmp/bad_records"


def build_spark_session(app_name: str = "bronze_ingestion") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        .getOrCreate()
    )


def infer_and_validate_schema(spark: SparkSession, sample_path: str) -> StructType:
    """Infer schema from a sample of source data. Fails loudly if source is empty."""
    sample_df = spark.read.format("json").option("samplingRatio", "0.1").load(sample_path)
    if sample_df.isEmpty():
        raise ValueError(f"Source path {sample_path!r} produced zero records — aborting schema inference")
    logger.info("Inferred %d columns from sample", len(sample_df.schema.fields))
    return sample_df.schema


def add_bronze_metadata(df: DataFrame, batch_id: str, source_path: str) -> DataFrame:
    """Stamp every record with audit columns required by downstream Silver layer."""
    return df.withColumn("_ingested_at", F.current_timestamp()).withColumn("_source_path", F.lit(source_path)).withColumn("_batch_id", F.lit(batch_id)).withColumn("_record_hash", F.md5(F.to_json(F.struct(*df.columns))))


def quarantine_bad_records(spark: SparkSession, bad_records_path: str, batch_id: str, source_path: str, quarantine_table: str) -> int:
    """
    Read PERMISSIVE-mode corrupted records from the bad records path and write
    them to the quarantine Delta table. Returns the count of quarantined records.
    """
    bad_path = Path(bad_records_path)
    if not bad_path.exists() or not any(bad_path.iterdir()):
        logger.debug("No bad records found at %s", bad_records_path)
        return 0

    bad_df = spark.read.text(bad_records_path)
    quarantine_df = bad_df.select(
        F.col("value").alias("raw_value"),
        F.lit(source_path).alias("source_path"),
        F.lit("PERMISSIVE_PARSE_FAILURE").alias("error_message"),
        F.lit(datetime.now(timezone.utc).isoformat()).cast(TimestampType()).alias("quarantined_at"),
        F.lit(batch_id).alias("ingestion_batch_id"),
    )

    count = quarantine_df.count()
    if count > 0:
        quarantine_df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(quarantine_table)
        logger.warning("Quarantined %d bad records from batch %s", count, batch_id)

    return count


def run_bronze_batch(config: BronzeIngestionConfig, spark: SparkSession | None = None) -> dict[str, int]:
    """
    Full Bronze ingestion run:
      1. Infer schema
      2. Read with PERMISSIVE mode (bad records go to side-channel file)
      3. Add audit metadata
      4. Write to Delta bronze table (append-only, schema evolution enabled)
      5. Quarantine bad records
    Returns metrics dict: {good_records, bad_records}.
    """
    spark = spark or build_spark_session()

    inferred_schema = infer_and_validate_schema(spark, config.source_path)

    raw_df = (
        spark.read.format(config.format)
        .schema(inferred_schema)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .option("badRecordsPath", config.bad_records_path)
        .load(config.source_path)
    )

    good_df = raw_df.filter(F.col("_corrupt_record").isNull()).drop("_corrupt_record")
    stamped_df = add_bronze_metadata(good_df, config.batch_id, config.source_path)

    good_count = stamped_df.count()

    stamped_df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(config.bronze_table)

    bad_count = quarantine_bad_records(
        spark,
        config.bad_records_path,
        config.batch_id,
        config.source_path,
        config.quarantine_table,
    )

    logger.info(
        "Bronze ingestion complete — batch=%s good=%d bad=%d",
        config.batch_id,
        good_count,
        bad_count,
    )
    return {"good_records": good_count, "bad_records": bad_count}


def run_bronze_streaming(config: BronzeIngestionConfig, spark: SparkSession | None = None) -> None:
    """
    Streaming variant using Auto Loader (cloudFiles) for continuous ingestion.
    Writes micro-batches to bronze Delta table with exactly-once semantics.
    """
    spark = spark or build_spark_session()

    stream_df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", config.format)
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.maxFilesPerTrigger", str(config.max_files_per_trigger))
        .load(config.source_path)
    )

    stamped_df = add_bronze_metadata(stream_df, config.batch_id, config.source_path)

    query = (
        stamped_df.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", config.checkpoint_location)
        .option("mergeSchema", "true")
        .toTable(config.bronze_table)
    )

    logger.info("Streaming bronze pipeline started — table=%s", config.bronze_table)
    query.awaitTermination()
