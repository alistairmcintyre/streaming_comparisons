"""
Spark Structured Streaming: Kafka → Delta bronze.item_attributes
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
CHECKPOINT_PATH = f"{CHECKPOINT_BASE}/bronze_item_attributes_delta"
TABLE_PATH      = "s3a://warehouse/delta/bronze/item_attributes"

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


def main():
    spark = (
        SparkSession.builder
        .appName("bronze-item-attributes-delta")
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
        .option("kafka.group.id", "delta-bronze-attr")
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
        .format("delta")
        .outputMode("append")
        .option("path", TABLE_PATH)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime="15 seconds")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
