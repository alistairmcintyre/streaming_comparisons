"""
Spark Structured Streaming: Kafka → Hudi bronze.item_attributes

Hudi bronze is append-only (COW, no deduplication). The full Debezium envelope
including op type is preserved so silver can handle inserts, updates, deletes.
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, coalesce, current_timestamp, to_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, IntegerType,
    TimestampType, DoubleType
)

KAFKA_BROKERS   = os.environ.get("KAFKA_BROKERS", "kafka:9092")
TOPIC           = "app.public.item_attributes"
CHECKPOINT_BASE = os.environ.get("CHECKPOINT_BASE", "s3a://warehouse/_chk")
CHECKPOINT_PATH = f"{CHECKPOINT_BASE}/bronze_item_attributes_hudi"
TABLE_PATH      = "s3a://warehouse/hudi/bronze/item_attributes"

PAYLOAD_SCHEMA = StructType([
    StructField("item_id",    LongType(),   True),
    StructField("name",       StringType(), True),
    StructField("price",      DoubleType(), True),
    StructField("category",   StringType(), True),
    StructField("updated_at", StringType(), True),
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
    print(f"[bronze-attr-hudi] batch {batch_id}: {batch_df.count()} rows")
    if batch_df.rdd.isEmpty():
        return

    (
        batch_df.write
        .format("hudi")
        .option("hoodie.table.name", "bronze_item_attributes")
        .option("hoodie.datasource.write.table.type", "COPY_ON_WRITE")
        .option("hoodie.datasource.write.operation", "insert")
        .option("hoodie.datasource.write.recordkey.field", "kafka_offset,kafka_partition")
        .option("hoodie.datasource.write.precombine.field", "kafka_offset")
        .option("hoodie.datasource.write.hive_style_partitioning", "true")
        .option("hoodie.datasource.write.partitionpath.field", "")
        # Disable metadata table for simpler local setup
        .option("hoodie.metadata.enable", "false")
        .mode("append")
        .save(TABLE_PATH)
    )


def main():
    spark = (
        SparkSession.builder
        .appName("bronze-item-attributes-hudi")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKERS)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .option("kafka.group.id", "hudi-bronze-attr")
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
            coalesce(col("env.after.item_id"), col("env.before.item_id")).alias("item_id"),
            col("env.after.name").alias("name"),
            col("env.after.price").alias("price"),
            col("env.after.category").alias("category"),
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
