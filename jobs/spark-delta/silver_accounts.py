"""
Spark Structured Streaming: Kafka accounts (Debezium) → Delta silver.accounts.

SCD1 current-view dimension — one row per account_id, latest wins. Reads the
Debezium changelog directly and MERGEs (this is the *dimension*; it's batch-read
downstream by the gold job for enrichment, so it does NOT need CDF).
"""
import os
from delta_tables import ensure_all  # in-pipeline DDL
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, from_json, current_timestamp, to_timestamp, coalesce, row_number,
)
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, LongType

KAFKA_BROKERS   = os.environ.get("KAFKA_BROKERS", "kafka:9092")
TOPIC           = "app.public.accounts"
CHECKPOINT_BASE = os.environ.get("CHECKPOINT_BASE", "s3a://warehouse/_chk")
CHECKPOINT_PATH = f"{CHECKPOINT_BASE}/silver_accounts_delta"
_BASE = os.environ.get("DELTA_WAREHOUSE", "s3a://warehouse/delta")
TABLE_PATH      = f"{_BASE}/silver/accounts"

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


def upsert(batch: DataFrame, batch_id: int):
    if batch.rdd.isEmpty():
        return
    spark = batch.sparkSession
    # latest change per account_id in this batch (event_ts present + monotonic on deletes)
    w = Window.partitionBy("account_id").orderBy(col("event_ts").desc(), col("kafka_offset").desc())
    latest = batch.withColumn("_rn", row_number().over(w)).filter(col("_rn") == 1).drop("_rn")
    latest.createOrReplaceTempView("_acct_updates")
    spark.sql(f"""
        MERGE INTO delta.`{TABLE_PATH}` AS t
        USING _acct_updates AS s
        ON t.account_id = s.account_id
        WHEN MATCHED AND s.op = 'd' THEN DELETE
        WHEN MATCHED AND s.op <> 'd' AND s.event_ts >= t.event_ts THEN UPDATE SET
            t.name=s.name, t.country=s.country, t.tier=s.tier,
            t.source_updated_at=s.source_updated_at, t.event_ts=s.event_ts, t.commit_ts=s.commit_ts
        WHEN NOT MATCHED AND s.op <> 'd' THEN INSERT
            (account_id, name, country, tier, source_updated_at, event_ts, commit_ts)
            VALUES (s.account_id, s.name, s.country, s.tier, s.source_updated_at, s.event_ts, s.commit_ts)
    """)


def main():
    spark = SparkSession.builder.appName("silver-accounts-delta").getOrCreate()
    ensure_all(spark)  # idempotent create-if-not-exists; no separate ddl-init
    spark.sparkContext.setLogLevel("WARN")

    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", KAFKA_BROKERS)
           .option("subscribe", TOPIC)
           .option("startingOffsets", "earliest")
           .option("failOnDataLoss", "false")
           .option("kafka.group.id", "delta-silver-accounts")
           .load()
           .filter(col("value").isNotNull()))

    parsed = (raw.select(
                from_json(col("value").cast("string"), ENVELOPE).alias("env"),
                col("offset").alias("kafka_offset"))
              .filter(col("env.op").isNotNull())
              .select(
                col("env.op").alias("op"),
                coalesce(col("env.after.account_id"), col("env.before.account_id")).alias("account_id"),
                col("env.after.name").alias("name"),
                col("env.after.country").alias("country"),
                col("env.after.tier").alias("tier"),
                to_timestamp(col("env.after.updated_at")).alias("source_updated_at"),
                (col("env.source.ts_ms") / 1000).cast("timestamp").alias("event_ts"),
                current_timestamp().alias("commit_ts"),
                col("kafka_offset")))

    (parsed.writeStream.foreachBatch(upsert)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime="15 seconds")
        .start().awaitTermination())


if __name__ == "__main__":
    main()
