"""
Spark Structured Streaming: Kafka (Debezium) → Hudi silver.accounts

SCD1 dimension: latest row per account_id wins, which Hudi gives directly via upsert
with precombine on source_updated_at.
"""
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp, to_timestamp, coalesce
from pyspark.sql.types import StructType, StructField, StringType, LongType
from hudi_tables import SILVER_ACCOUNTS, silver_accounts_opts

KAFKA_BROKERS   = os.environ.get("KAFKA_BROKERS", "kafka:9092")
TOPIC           = "app.public.accounts"
CHECKPOINT_BASE = os.environ.get("CHECKPOINT_BASE", "s3a://warehouse/_chk")
CHECKPOINT_PATH = f"{CHECKPOINT_BASE}/silver_accounts_hudi"

PAYLOAD = StructType([
    StructField("account_id", LongType(),   True),
    StructField("name",       StringType(), True),
    StructField("country",    StringType(), True),
    StructField("tier",       StringType(), True),
    StructField("updated_at", StringType(), True),
])
SOURCE = StructType([StructField("ts_ms", LongType(), True)])
ENVELOPE = StructType([
    StructField("op",     StringType(), True),
    StructField("before", PAYLOAD,      True),
    StructField("after",  PAYLOAD,      True),
    StructField("source", SOURCE,       True),
])


def main():
    spark = SparkSession.builder.appName("silver-accounts-hudi").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", KAFKA_BROKERS)
           .option("subscribe", TOPIC)
           .option("startingOffsets", "earliest")
           .option("failOnDataLoss", "false")
           .option("kafka.group.id", "hudi-silver-accounts")
           .load()
           .filter(col("value").isNotNull()))

    # `after` is null on delete (op=d); fall back to `before` to keep the key.
    parsed = (raw.select(from_json(col("value").cast("string"), ENVELOPE).alias("env"))
              .select(
                col("env.op").alias("op"),
                coalesce(col("env.after.account_id"), col("env.before.account_id")).alias("account_id"),
                col("env.after.name").alias("name"),
                col("env.after.country").alias("country"),
                col("env.after.tier").alias("tier"),
                to_timestamp(col("env.after.updated_at")).alias("source_updated_at"),
                (col("env.source.ts_ms") / 1000).cast("timestamp").alias("event_ts"),
                current_timestamp().alias("commit_ts"))
              .filter(col("account_id").isNotNull() & (col("op") != "d")))

    (parsed.writeStream.format("hudi")
        .options(**silver_accounts_opts())
        .option("path", SILVER_ACCOUNTS)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .outputMode("append")
        .trigger(processingTime="15 seconds")
        .start().awaitTermination())


if __name__ == "__main__":
    main()
