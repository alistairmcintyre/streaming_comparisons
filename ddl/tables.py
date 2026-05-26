"""
Iceberg table schema and partition spec definitions.
Used by create_namespaces.py to create all 8 tables.
"""

from pyiceberg.schema import Schema
from pyiceberg.types import (
    LongType, IntegerType, StringType, TimestampType, DateType, DoubleType, NestedField
)
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import DayTransform, IdentityTransform

# ─── Shared table properties ──────────────────────────────────────────────

BRONZE_PROPERTIES = {
    "format-version": "2",
    "write.format.default": "parquet",
    "write.parquet.compression-codec": "zstd",
    "write.target-file-size-bytes": "134217728",
    "commit.retry.num-retries": "10",
    "commit.retry.min-wait-ms": "200",
}

SILVER_PROPERTIES = {
    **BRONZE_PROPERTIES,
    # merge-on-read: writes equality/position deletes instead of rewriting files.
    # Load-bearing for 20s MERGE cadence — without it, every batch rewrites all
    # affected data files and throughput collapses.
    "write.delete.mode": "merge-on-read",
    "write.update.mode": "merge-on-read",
    "write.merge.mode": "merge-on-read",
    "write.distribution-mode": "hash",
    "write.spark.fanout.enabled": "true",
    # Flink upsert sink: these two properties must be set on the table.
    # Missing either = Flink silently falls back to append mode (correctness bug).
    "write.upsert.enabled": "true",
}

# ─── Bronze schemas ────────────────────────────────────────────────────────

BRONZE_INVENTORY_SCHEMA = Schema(
    NestedField(1,  "op",               StringType(),    required=False),
    NestedField(2,  "item_id",          LongType(),      required=False),
    NestedField(3,  "qty_on_hand",      IntegerType(),   required=False),
    NestedField(4,  "location",         StringType(),    required=False),
    NestedField(5,  "source_updated_at",TimestampType(), required=False),
    NestedField(6,  "event_ts",         TimestampType(), required=False),
    NestedField(7,  "ingest_ts",        TimestampType(), required=False),
    NestedField(8,  "kafka_offset",     LongType(),      required=False),
    NestedField(9,  "kafka_partition",  IntegerType(),   required=False),
)

BRONZE_ATTRIBUTES_SCHEMA = Schema(
    NestedField(1,  "op",               StringType(),    required=False),
    NestedField(2,  "item_id",          LongType(),      required=False),
    NestedField(3,  "name",             StringType(),    required=False),
    NestedField(4,  "price",            DoubleType(),    required=False),
    NestedField(5,  "category",         StringType(),    required=False),
    NestedField(6,  "source_updated_at",TimestampType(), required=False),
    NestedField(7,  "event_ts",         TimestampType(), required=False),
    NestedField(8,  "ingest_ts",        TimestampType(), required=False),
    NestedField(9,  "kafka_offset",     LongType(),      required=False),
    NestedField(10, "kafka_partition",  IntegerType(),   required=False),
)

# Partition by day of event_ts (source event timestamp from Debezium source.ts_ms)
BRONZE_PARTITION_SPEC = PartitionSpec(
    PartitionField(source_id=6, field_id=1000, transform=DayTransform(), name="event_ts_day")
)

BRONZE_ATTRIBUTES_PARTITION_SPEC = PartitionSpec(
    PartitionField(source_id=7, field_id=1000, transform=DayTransform(), name="event_ts_day")
)

# ─── Silver schemas ────────────────────────────────────────────────────────

SILVER_INVENTORY_SCHEMA = Schema(
    NestedField(1,  "item_id",          LongType(),      required=True),
    NestedField(2,  "qty_on_hand",      IntegerType(),   required=False),
    NestedField(3,  "location",         StringType(),    required=False),
    NestedField(4,  "source_updated_at",TimestampType(), required=False),
    NestedField(5,  "event_ts",         TimestampType(), required=False),
    NestedField(6,  "event_date",       DateType(),      required=False),
    NestedField(7,  "ingest_ts",        TimestampType(), required=False),
    NestedField(8,  "commit_ts",        TimestampType(), required=False),
    identifier_field_ids=[1],  # Primary key — required for Flink upsert mode
)

SILVER_ATTRIBUTES_SCHEMA = Schema(
    NestedField(1,  "item_id",          LongType(),      required=True),
    NestedField(2,  "name",             StringType(),    required=False),
    NestedField(3,  "price",            DoubleType(),    required=False),
    NestedField(4,  "category",         StringType(),    required=False),
    NestedField(5,  "source_updated_at",TimestampType(), required=False),
    NestedField(6,  "event_ts",         TimestampType(), required=False),
    NestedField(7,  "event_date",       DateType(),      required=False),
    NestedField(8,  "ingest_ts",        TimestampType(), required=False),
    NestedField(9,  "commit_ts",        TimestampType(), required=False),
    identifier_field_ids=[1],
)

# Partition by event_date (identity transform on date column)
SILVER_PARTITION_SPEC = PartitionSpec(
    PartitionField(source_id=6, field_id=1000, transform=IdentityTransform(), name="event_date")
)

SILVER_ATTRIBUTES_PARTITION_SPEC = PartitionSpec(
    PartitionField(source_id=7, field_id=1000, transform=IdentityTransform(), name="event_date")
)

# ─── Table manifest ───────────────────────────────────────────────────────

# (namespace, table_name, schema, partition_spec, properties)
ALL_TABLES = [
    ("bronze", "item_inventory_spark",  BRONZE_INVENTORY_SCHEMA,   BRONZE_PARTITION_SPEC,            BRONZE_PROPERTIES),
    ("bronze", "item_inventory_flink",  BRONZE_INVENTORY_SCHEMA,   BRONZE_PARTITION_SPEC,            BRONZE_PROPERTIES),
    ("bronze", "item_attributes_spark", BRONZE_ATTRIBUTES_SCHEMA,  BRONZE_ATTRIBUTES_PARTITION_SPEC, BRONZE_PROPERTIES),
    ("bronze", "item_attributes_flink", BRONZE_ATTRIBUTES_SCHEMA,  BRONZE_ATTRIBUTES_PARTITION_SPEC, BRONZE_PROPERTIES),
    ("silver", "item_inventory_spark",  SILVER_INVENTORY_SCHEMA,   SILVER_PARTITION_SPEC,            SILVER_PROPERTIES),
    ("silver", "item_inventory_flink",  SILVER_INVENTORY_SCHEMA,   SILVER_PARTITION_SPEC,            SILVER_PROPERTIES),
    ("silver", "item_attributes_spark", SILVER_ATTRIBUTES_SCHEMA,  SILVER_ATTRIBUTES_PARTITION_SPEC, SILVER_PROPERTIES),
    ("silver", "item_attributes_flink", SILVER_ATTRIBUTES_SCHEMA,  SILVER_ATTRIBUTES_PARTITION_SPEC, SILVER_PROPERTIES),
]
