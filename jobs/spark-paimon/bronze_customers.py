"""
Spark Structured Streaming: Kafka → Paimon bronze.customers

Paimon bronze is append-only. The Paimon catalog is named 'paimon' (separate
from spark_catalog), so table paths use paimon.<db>.<table> syntax.
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, coalesce, current_timestamp, to_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType
)

from cdc import iceberg_props  # shared helper (mounted at /opt/shared)

KAFKA_BROKERS   = os.environ.get("KAFKA_BROKERS", "kafka:9092")
TOPIC           = "app.public.customers"
CHECKPOINT_BASE = os.environ.get("CHECKPOINT_BASE", "s3a://warehouse/_chk")
CHECKPOINT_PATH = f"{CHECKPOINT_BASE}/bronze_customers_paimon"

PAYLOAD_SCHEMA = StructType([
    StructField("customer_id", LongType(),   True),
    StructField("name",        StringType(), True),
    StructField("country",     StringType(), True),
    StructField("segment",     StringType(), True),
    StructField("updated_at",  StringType(), True),
])

SOURCE_SCHEMA = StructType([
    StructField("ts_ms", LongType(),   True),
    StructField("db",    StringType(), True),
    StructField("table", StringType(), True),
])

ENVELOPE_SCHEMA = StructType([
    StructField("op",     StringType(),   True),
    StructField("before", PAYLOAD_SCHEMA, True),
    StructField("after",  PAYLOAD_SCHEMA, True),
    StructField("source", SOURCE_SCHEMA,  True),
    StructField("ts_ms",  LongType(),     True),
])


def write_bronze_batch(batch_df, batch_id):
    print(f"[bronze-customers-paimon] batch {batch_id}: {batch_df.count()} rows")
    if batch_df.rdd.isEmpty():
        return

    (
        batch_df.write
        .format("paimon")
        .mode("append")
        .saveAsTable("paimon.bronze.customers")
    )


def main():
    spark = (
        SparkSession.builder
        .appName("bronze-customers-paimon")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    spark.sql("CREATE DATABASE IF NOT EXISTS paimon.bronze")

    # Create bronze append table (no primary key → append-only)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS paimon.bronze.customers (
            op               STRING,
            customer_id      BIGINT,
            name             STRING,
            country          STRING,
            segment          STRING,
            source_updated_at TIMESTAMP,
            event_ts         TIMESTAMP,
            ingest_ts        TIMESTAMP,
            kafka_offset     BIGINT,
            kafka_partition  INT
        ) TBLPROPERTIES (
            'file.format' = 'parquet'{iceberg_props()}
        )
    """)

    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKERS)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .option("kafka.group.id", "paimon-bronze-customers")
        .load()
        .filter(col("value").isNotNull())
    )

    parsed = (
        raw
        .select(
            from_json(col("value").cast("string"), ENVELOPE_SCHEMA).alias("env"),
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

    query = (
        parsed.writeStream
        .foreachBatch(write_bronze_batch)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime="15 seconds")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
