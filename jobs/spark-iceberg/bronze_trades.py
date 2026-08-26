"""
Spark Structured Streaming: Kafka → Iceberg bronze.trades_spark (append fills).
Immutable executions → plain append; downstream reads it as an append stream.
"""
import os
from pyspark.sql import SparkSession
from iceberg_tables import ensure_all  # in-pipeline DDL
from pyspark.sql.functions import col, from_json, current_timestamp, to_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, IntegerType, DecimalType,
)

KAFKA_BROKERS   = os.environ.get("KAFKA_BROKERS", "kafka:9092")
TOPIC           = "app.public.trades"
CHECKPOINT_BASE = os.environ.get("CHECKPOINT_BASE", "s3a://warehouse/_chk")
CHECKPOINT_PATH = f"{CHECKPOINT_BASE}/bronze_trades_spark"
TABLE           = "rest.bronze.trades_spark"

PAYLOAD = StructType([
    StructField("trade_id",    LongType(),    True),
    StructField("account_id",  LongType(),    True),
    StructField("symbol",      StringType(),  True),
    StructField("side",        StringType(),  True),
    StructField("quantity",    IntegerType(), True),
    StructField("price",       StringType(),  True),   # exact decimal as string
    StructField("executed_at", StringType(),  True),
])
SOURCE = StructType([StructField("ts_ms", LongType(), True)])
ENVELOPE = StructType([
    StructField("op",     StringType(), True),
    StructField("after",  PAYLOAD,      True),
    StructField("source", SOURCE,       True),
])


def main():
    spark = SparkSession.builder.appName("bronze-trades-spark").getOrCreate()
    ensure_all(spark)  # idempotent create-if-not-exists; no separate ddl-init
    spark.sparkContext.setLogLevel("WARN")

    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", KAFKA_BROKERS)
           .option("subscribe", TOPIC)
           .option("startingOffsets", "earliest")
           .option("failOnDataLoss", "false")
           .option("kafka.group.id", "spark-bronze-trades")
           .option("maxOffsetsPerTrigger", "200000")
           .load()
           .filter(col("value").isNotNull()))

    parsed = (raw.select(
                from_json(col("value").cast("string"), ENVELOPE).alias("env"),
                col("offset").alias("kafka_offset"),
                col("partition").alias("kafka_partition"))
              .filter(col("env.after.trade_id").isNotNull())
              .select(
                col("env.op").alias("op"),
                col("env.after.trade_id").alias("trade_id"),
                col("env.after.account_id").alias("account_id"),
                col("env.after.symbol").alias("symbol"),
                col("env.after.side").alias("side"),
                col("env.after.quantity").alias("quantity"),
                col("env.after.price").cast(DecimalType(12, 4)).alias("price"),
                to_timestamp(col("env.after.executed_at")).alias("executed_at"),
                (col("env.source.ts_ms") / 1000).cast("timestamp").alias("event_ts"),
                current_timestamp().alias("ingest_ts"),
                col("kafka_offset"), col("kafka_partition")))

    (parsed.writeStream.format("iceberg").outputMode("append")
        .option("path", TABLE)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .option("fanout-enabled", "true")
        .trigger(processingTime="10 seconds")
        .start().awaitTermination())


if __name__ == "__main__":
    main()
