"""
Delta Lake Schema Evolution: mergeSchema, column mapping, type widening.
Production pattern — safe schema changes without table recreation.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SchemaEvolutionConfig:
    table_name: str
    spark: SparkSession


# ---------------------------------------------------------------------------
# mergeSchema — add columns on write
# ---------------------------------------------------------------------------


def append_with_new_columns(config: SchemaEvolutionConfig, df: DataFrame) -> None:
    """
    Append data with new columns not present in the existing table schema.
    Delta will automatically extend the schema.
    Equivalent to setting: spark.databricks.delta.schema.autoMerge.enabled = true
    """
    df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(config.table_name)
    logger.info("Appended with mergeSchema=true to %s", config.table_name)


def overwrite_with_new_schema(config: SchemaEvolutionConfig, df: DataFrame) -> None:
    """
    Full overwrite where the new DataFrame has a different schema.
    overwriteSchema=true is required when columns are removed or types change.
    """
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(config.table_name)
    logger.info("Overwrote schema on %s", config.table_name)


# ---------------------------------------------------------------------------
# ALTER TABLE column operations
# ---------------------------------------------------------------------------


def add_column(config: SchemaEvolutionConfig, column_name: str, column_type: str, comment: str | None = None) -> None:
    """Add a new column with an optional comment."""
    sql = f"ALTER TABLE {config.table_name} ADD COLUMN {column_name} {column_type}"
    if comment:
        sql += f" COMMENT '{comment}'"
    config.spark.sql(sql)
    logger.info("Added column %s %s to %s", column_name, column_type, config.table_name)


def rename_column(config: SchemaEvolutionConfig, old_name: str, new_name: str) -> None:
    """
    Rename a column. Requires column mapping mode = 'name' (set once at table creation).
    Delta stores the physical name separately from the logical name.
    """
    _assert_column_mapping_enabled(config)
    config.spark.sql(f"ALTER TABLE {config.table_name} RENAME COLUMN {old_name} TO {new_name}")
    logger.info("Renamed column %s → %s on %s", old_name, new_name, config.table_name)


def drop_column(config: SchemaEvolutionConfig, column_name: str) -> None:
    """
    Drop a column. Requires column mapping mode = 'name'.
    The physical data is retained until the next REORG TABLE.
    """
    _assert_column_mapping_enabled(config)
    config.spark.sql(f"ALTER TABLE {config.table_name} DROP COLUMN {column_name}")
    logger.info("Dropped column %s from %s", column_name, config.table_name)


def change_column_comment(config: SchemaEvolutionConfig, column_name: str, comment: str) -> None:
    config.spark.sql(f"ALTER TABLE {config.table_name} ALTER COLUMN {column_name} COMMENT '{comment}'")


# ---------------------------------------------------------------------------
# Column Mapping Mode
# ---------------------------------------------------------------------------


def enable_column_mapping(config: SchemaEvolutionConfig) -> None:
    """
    Enable column mapping on an existing table (one-time, irreversible upgrade).
    Required before RENAME COLUMN or DROP COLUMN.
    Sets minReaderVersion=2, minWriterVersion=5.
    """
    config.spark.sql(f"""
        ALTER TABLE {config.table_name}
        SET TBLPROPERTIES (
            'delta.columnMapping.mode' = 'name',
            'delta.minReaderVersion' = '2',
            'delta.minWriterVersion' = '5'
        )
    """)
    logger.info("Column mapping enabled on %s", config.table_name)


def _assert_column_mapping_enabled(config: SchemaEvolutionConfig) -> None:
    """Raise if column mapping is not enabled — prevents cryptic Spark errors."""
    props_df = config.spark.sql(f"SHOW TBLPROPERTIES {config.table_name}")
    props = {row["key"]: row["value"] for row in props_df.collect()}
    mode = props.get("delta.columnMapping.mode", "none")
    if mode != "name":
        raise RuntimeError(
            f"Table {config.table_name!r} does not have column mapping enabled "
            f"(current mode: {mode!r}). "
            "Run enable_column_mapping() first."
        )


# ---------------------------------------------------------------------------
# Schema validation helpers
# ---------------------------------------------------------------------------


def get_current_schema(config: SchemaEvolutionConfig) -> StructType:
    return config.spark.read.format("delta").table(config.table_name).schema


def diff_schemas(schema_a: StructType, schema_b: StructType) -> dict[str, Any]:
    """Compute the diff between two StructType schemas."""
    fields_a = {f.name: f.dataType for f in schema_a.fields}
    fields_b = {f.name: f.dataType for f in schema_b.fields}

    added = {name: str(dtype) for name, dtype in fields_b.items() if name not in fields_a}
    removed = {name: str(dtype) for name, dtype in fields_a.items() if name not in fields_b}
    type_changed = {
        name: {"from": str(fields_a[name]), "to": str(fields_b[name])}
        for name in fields_a
        if name in fields_b and fields_a[name] != fields_b[name]
    }

    return {
        "added_columns": added,
        "removed_columns": removed,
        "type_changes": type_changed,
        "is_backward_compatible": len(removed) == 0 and len(type_changed) == 0,
    }


def validate_schema_compatibility(
    config: SchemaEvolutionConfig,
    new_df: DataFrame,
    strict: bool = False,
) -> dict[str, Any]:
    """
    Validate that new_df schema is compatible with the existing table schema.
    strict=True: raises on any incompatibility. strict=False: logs warnings only.
    """
    current_schema = get_current_schema(config)
    diff = diff_schemas(current_schema, new_df.schema)

    if not diff["is_backward_compatible"]:
        message = (
            f"Schema incompatibility detected for {config.table_name!r}: "
            f"removed={diff['removed_columns']}, type_changes={diff['type_changes']}"
        )
        if strict:
            raise ValueError(message)
        logger.warning(message)
    else:
        logger.info("Schema validation passed for %s — backward compatible", config.table_name)

    return diff


# ---------------------------------------------------------------------------
# Type widening (Delta 3.1+ / Databricks Runtime 14.3+)
# ---------------------------------------------------------------------------


def enable_type_widening(config: SchemaEvolutionConfig) -> None:
    """
    Enable automatic type widening (e.g., INT → LONG, FLOAT → DOUBLE).
    Requires Delta 3.1+ with minWriterVersion=7.
    """
    config.spark.sql(f"""
        ALTER TABLE {config.table_name}
        SET TBLPROPERTIES ('delta.enableTypeWidening' = 'true')
    """)
    logger.info("Type widening enabled on %s", config.table_name)
