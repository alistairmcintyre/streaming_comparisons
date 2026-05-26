"""
Spark Structured Streaming: Kafka → bronze.item_sales_spark
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, coalesce, current_timestamp, to_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, IntegerType,
    TimestampType, DoubleType
)

KAFKA_BROKERS   = os.environ.get("KAFKA_BROKERS", "kafka:9092")
TOPIC           = "app.public.item_sales"
CHECKPOINT_BASE = os.environ.get("CHECKPOINT_BASE", "s3a://warehouse/_chk")
CHECKPOINT_PATH = f"{CHECKPOINT_BASE}/bronze_item_sales_spark"

PAYLOAD_SCHEMA = StructType([
    StructField("item_id",     LongType(),   True),
    StructField("quantity",    IntegerType(), True),
    StructField("total_price", DoubleType(), True),
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


def main():
    spark = (
        SparkSession.builder
        .appName("bronze-item-sales-spark")
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
        .option("kafka.group.id", "spark-bronze-sales")
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
            col("env.after.quantity").alias("quantity"),
            col("env.after.total_price").alias("total_price"),
            to_timestamp(col("env.after.updated_at")).alias("source_updated_at"),
            (col("env.source.ts_ms") / 1000).cast("timestamp").alias("event_ts"),
            current_timestamp().alias("ingest_ts"),
            col("kafka_offset"),
            col("kafka_partition"),
        )
    )

    query = (
        parsed.writeStream
        .format("iceberg")
        .outputMode("append")
        .option("path", "rest.bronze.item_sales_spark")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .option("fanout-enabled", "true")
        .trigger(processingTime="15 seconds")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
