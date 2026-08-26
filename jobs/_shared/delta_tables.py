"""
Delta table DDL as importable, idempotent functions — run IN the pipeline.

Each streaming job calls ensure_all(spark) at startup (after building the session,
before streaming). createIfNotExists is create-once + idempotent, so every stage can
self-provision the tables it reads/writes with no separate ddl-init and no ordering
dependency. Compaction is in-pipeline (delta.autoOptimize.optimizeWrite/.autoCompact);
VACUUM is the only separate job. Warehouse base is env-driven (local MinIO / AWS S3).

NOTE: create-once semantics — changing a schema/props in code only lands on a fresh
table (fine for ephemeral benchmark runs; a long-lived table needs explicit migration).
"""
import os
from delta.tables import DeltaTable
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, IntegerType, DoubleType, TimestampType,
)

_BASE = os.environ.get("DELTA_WAREHOUSE", "s3a://warehouse/delta")


def create_bronze_trades(spark):
    schema = StructType([
        StructField("op", StringType()), StructField("trade_id", LongType()),
        StructField("account_id", LongType()), StructField("symbol", StringType()),
        StructField("side", StringType()), StructField("quantity", IntegerType()),
        StructField("price", DoubleType()), StructField("executed_at", TimestampType()),
        StructField("event_ts", TimestampType()), StructField("ingest_ts", TimestampType()),
        StructField("kafka_offset", LongType()), StructField("kafka_partition", IntegerType()),
    ])
    (DeltaTable.createIfNotExists(spark)
        .tableName("delta_bronze_trades").addColumns(schema)
        .location(f"{_BASE}/bronze/trades").clusterBy("executed_at")
        .property("delta.enableDeletionVectors", "false")
        .execute())


def create_silver_trades(spark):
    schema = StructType([
        StructField("trade_id", LongType(), False), StructField("account_id", LongType()),
        StructField("symbol", StringType()), StructField("side", StringType()),
        StructField("quantity", IntegerType()), StructField("price", DoubleType()),
        StructField("executed_at", TimestampType()), StructField("event_ts", TimestampType()),
        StructField("ingest_ts", TimestampType()),
    ])
    (DeltaTable.createIfNotExists(spark)
        .tableName("delta_silver_trades").addColumns(schema)
        .location(f"{_BASE}/silver/trades").clusterBy("executed_at")
        .property("delta.enableDeletionVectors", "false")
        .execute())


def create_silver_accounts(spark):
    schema = StructType([
        StructField("account_id", LongType(), False), StructField("name", StringType()),
        StructField("country", StringType()), StructField("tier", StringType()),
        StructField("source_updated_at", TimestampType()), StructField("event_ts", TimestampType()),
        StructField("commit_ts", TimestampType()),
    ])
    (DeltaTable.createIfNotExists(spark)
        .tableName("delta_silver_accounts").addColumns(schema)
        .location(f"{_BASE}/silver/accounts").clusterBy("account_id")
        .property("delta.enableDeletionVectors", "true")
        .execute())


def create_gold_open_positions(spark):
    schema = StructType([
        StructField("account_id", LongType(), False), StructField("symbol", StringType(), False),
        StructField("net_quantity", LongType()), StructField("net_notional", DoubleType()),
        StructField("trade_count", LongType()), StructField("status", StringType()),
        StructField("country", StringType()), StructField("tier", StringType()),
        StructField("commit_ts", TimestampType()),
    ])
    (DeltaTable.createIfNotExists(spark)
        .tableName("delta_gold_open_positions").addColumns(schema)
        .location(f"{_BASE}/gold/open_positions").clusterBy("symbol", "account_id")
        .property("delta.enableDeletionVectors", "true")
        .execute())


# Table properties we want to hold on EVERY layer. createIfNotExists() FAILS with
# DELTA_CREATE_TABLE_WITH_DIFFERENT_PROPERTY when a table already exists with a
# different property set, so changing this list would break every restart against
# tables created by an earlier build. Converge with ALTER TABLE instead: create is
# for new tables, ALTER makes existing ones match.
_TABLE_PROPERTIES = {
    "delta.autoOptimize.optimizeWrite": "true",
    "delta.autoOptimize.autoCompact": "true",
}

_TABLE_PATHS = ["bronze/trades", "silver/trades", "silver/accounts", "gold/open_positions"]


def _converge_properties(spark):
    """Make existing tables match _TABLE_PROPERTIES (idempotent, safe on new tables)."""
    props = ", ".join(f"'{k}' = '{v}'" for k, v in _TABLE_PROPERTIES.items())
    for rel in _TABLE_PATHS:
        try:
            spark.sql(f"ALTER TABLE delta.`{_BASE}/{rel}` SET TBLPROPERTIES ({props})")
        except Exception as e:  # table may not exist yet on a first run
            print(f"skip property converge for {rel}: {str(e)[:160]}", flush=True)


def ensure_all(spark):
    create_bronze_trades(spark)
    create_silver_trades(spark)
    create_silver_accounts(spark)
    create_gold_open_positions(spark)
    _converge_properties(spark)
