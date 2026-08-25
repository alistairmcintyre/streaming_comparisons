"""
Shared helpers for the Spark streaming jobs.

Mounted read-only at /opt/shared and put on PYTHONPATH (see docker-compose env
anchors), so any Spark job can `from cdc import ...`.

Only boilerplate that is *identical across engines* lives here — the Debezium
CDC envelope schema + parse, the latest-per-key dedup, Paimon Iceberg-compat
props, and the Kafka serving-sink. Engine-specific logic (MERGE, table DDL,
format options) stays inline in each job so the comparison stays readable in a
single file per engine.
"""

import os
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, from_json, coalesce, current_timestamp, to_timestamp, to_date,
    row_number, to_json, struct,
)
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, LongType


# ── Debezium CDC envelope for public.customers ─────────────────────────────
_PAYLOAD = StructType([
    StructField("customer_id", LongType(),   True),
    StructField("name",        StringType(), True),
    StructField("country",     StringType(), True),
    StructField("segment",     StringType(), True),
    StructField("updated_at",  StringType(), True),
])
_SOURCE = StructType([
    StructField("ts_ms", LongType(),   True),
    StructField("db",    StringType(), True),
    StructField("table", StringType(), True),
])
CUSTOMERS_ENVELOPE = StructType([
    StructField("op",     StringType(), True),
    StructField("before", _PAYLOAD,     True),
    StructField("after",  _PAYLOAD,     True),
    StructField("source", _SOURCE,      True),
    StructField("ts_ms",  LongType(),   True),
])


def parse_customers_cdc(raw: DataFrame) -> DataFrame:
    """Raw Kafka stream (Debezium JSON) → bronze customer columns."""
    return (
        raw.filter(col("value").isNotNull())
        .select(
            from_json(col("value").cast("string"), CUSTOMERS_ENVELOPE).alias("env"),
            col("offset").alias("kafka_offset"),
            col("partition").alias("kafka_partition"),
        )
        .filter(col("env.op").isNotNull())
        .select(
            col("env.op").alias("op"),
            coalesce(col("env.after.customer_id"), col("env.before.customer_id")).alias("customer_id"),
            col("env.after.name").alias("name"),
            col("env.after.country").alias("country"),
            col("env.after.segment").alias("segment"),
            to_timestamp(col("env.after.updated_at")).alias("source_updated_at"),
            (col("env.source.ts_ms") / 1000).cast("timestamp").alias("event_ts"),
            current_timestamp().alias("ingest_ts"),
            col("kafka_offset"),
            col("kafka_partition"),
        )
    )


def latest_per_customer(batch_df: DataFrame) -> DataFrame:
    """Dedup a micro-batch to the newest event per customer_id; add event_date/commit_ts."""
    # event_ts (Debezium source.ts_ms) is present + monotonic on deletes;
    # source_updated_at is NULL on a delete and would let an in-batch insert
    # mask the delete.
    w = Window.partitionBy("customer_id").orderBy(
        col("event_ts").desc(),
        col("kafka_offset").desc(),
    )
    return (
        batch_df
        .withColumn("_rn", row_number().over(w))
        .filter(col("_rn") == 1)
        .drop("_rn", "kafka_offset", "kafka_partition")
        .withColumn("event_date", to_date(col("event_ts")))
        .withColumn("commit_ts", current_timestamp())
    )


def iceberg_props() -> str:
    """Paimon Iceberg-compat TBLPROPERTIES fragment so Athena/Glue can read it.
    Local: hadoop-catalog (metadata to the warehouse); AWS: set
    PAIMON_ICEBERG_STORAGE=hive-catalog to register into Glue."""
    storage = os.environ.get("PAIMON_ICEBERG_STORAGE", "hadoop-catalog")
    if storage in ("", "disabled"):
        return ""
    props = [f"'metadata.iceberg.storage' = '{storage}'"]
    if storage == "hive-catalog":
        props += [
            "'metadata.iceberg.hive-client-class' = 'com.amazonaws.glue.catalog.metastore.AWSCatalogMetastoreClient'",
            "'metadata.iceberg.manifest-legacy-version' = 'true'",
        ]
    return ",\n            " + ",\n            ".join(props)


def to_serving_kafka(df: DataFrame, topic: str, key_col: str, brokers: str = None) -> None:
    """Publish a small serving DataFrame to a Kafka topic as JSON (one message
    per row, keyed by key_col). Gold jobs use this to feed the serving loader,
    which lands it in Postgres for Grafana. Decouples the pipelines from the
    serving store (swap Postgres → StarRocks/Doris by changing only the loader)."""
    brokers = brokers or os.environ.get("KAFKA_BROKERS", "kafka:9092")
    (
        df.select(
            col(key_col).cast("string").alias("key"),
            to_json(struct([col(c) for c in df.columns])).alias("value"),
        )
        .write
        .format("kafka")
        .option("kafka.bootstrap.servers", brokers)
        .option("topic", topic)
        .save()
    )
