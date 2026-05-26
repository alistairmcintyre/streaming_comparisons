"""
Spark Structured Streaming: bronze.item_inventory_spark → silver.item_inventory_spark

Strategy: foreachBatch + MERGE INTO (Iceberg v2 merge-on-read)

Per-batch deduplication: keep the latest row per item_id by source_updated_at
(ties broken by kafka_offset). This prevents out-of-order CDC events from
overwriting newer state with older state.

MERGE logic:
  - op='d'  → DELETE matching row
  - op c/u/r → UPDATE if source_updated_at >= existing (idempotent replay)
  - new key  → INSERT

Trigger: every 20 seconds
"""

import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, current_timestamp, to_date, row_number
from pyspark.sql.window import Window

CHECKPOINT_BASE = os.environ.get("CHECKPOINT_BASE", "s3a://warehouse/_chk")
CHECKPOINT_PATH = f"{CHECKPOINT_BASE}/silver_item_inventory_spark"


def upsert_to_silver(batch_df: DataFrame, batch_id: int):
    if batch_df.rdd.isEmpty():
        return

    spark = batch_df.sparkSession

    # Dedupe within this micro-batch: keep newest event per item_id.
    # source_updated_at is the ground truth; kafka_offset breaks ties.
    w = Window.partitionBy("item_id").orderBy(
        col("source_updated_at").desc(),
        col("kafka_offset").desc(),
    )
    latest = (
        batch_df
        .withColumn("_rn", row_number().over(w))
        .filter(col("_rn") == 1)
        .drop("_rn", "kafka_offset", "kafka_partition")  # not in silver schema
        .withColumn("event_date", to_date(col("event_ts")))
        .withColumn("commit_ts", current_timestamp())
    )

    latest.createOrReplaceTempView("_silver_updates")

    spark.sql("""
        MERGE INTO rest.silver.item_inventory_spark AS t
        USING _silver_updates AS s
        ON t.item_id = s.item_id
        WHEN MATCHED AND s.op = 'd'
            THEN DELETE
        WHEN MATCHED AND s.op <> 'd'
             AND s.source_updated_at >= t.source_updated_at
            THEN UPDATE SET
                t.qty_on_hand       = s.qty_on_hand,
                t.location          = s.location,
                t.source_updated_at = s.source_updated_at,
                t.event_ts          = s.event_ts,
                t.event_date        = s.event_date,
                t.ingest_ts         = s.ingest_ts,
                t.commit_ts         = s.commit_ts
        WHEN NOT MATCHED AND s.op <> 'd'
            THEN INSERT (item_id, qty_on_hand, location, source_updated_at,
                         event_ts, event_date, ingest_ts, commit_ts)
                 VALUES (s.item_id, s.qty_on_hand, s.location, s.source_updated_at,
                         s.event_ts, s.event_date, s.ingest_ts, s.commit_ts)
    """)


def main():
    spark = (
        SparkSession.builder
        .appName("silver-item-inventory-spark")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Read bronze as an Iceberg streaming source.
    # Iceberg streaming reads are snapshot-based (new commits appear as new batches).
    # watermark bounds the state retained for late arrivals.
    bronze = (
        spark.readStream
        .format("iceberg")
        .option("stream-from-timestamp", "0")
        .load("rest.bronze.item_inventory_spark")
        .withWatermark("event_ts", "10 minutes")
    )

    query = (
        bronze.writeStream
        .foreachBatch(upsert_to_silver)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime="20 seconds")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
