"""
Spark Structured Streaming: silver.item_attributes_spark → gold.item_category_count_spark

Stream reads silver item_attributes. On each micro-batch, does a full batch
read of the current silver state and recomputes item count per category.

This demonstrates that MoR UPDATE snapshots flow through to a streaming
consumer — changing an item's category in Postgres will be visible in the
gold table within one trigger interval.

Aggregation:
  - item_count: distinct items in each category (current state)

The gold table is updated via MERGE INTO on category.

Trigger: every 30 seconds.
"""

import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import current_timestamp, count

CHECKPOINT_PATH = "s3a://warehouse/_chk/gold_item_category_count_spark"


def upsert_to_gold(batch_df: DataFrame, batch_id: int):
    print(f"[gold-item-category-count] batch {batch_id}: {batch_df.count()} rows")
    if batch_df.rdd.isEmpty():
        print(f"[gold-item-category-count] batch {batch_id}: empty, skipping")
        return

    spark = batch_df.sparkSession

    # Full batch read of current silver state so the counts reflect
    # the latest category for every item, not just what changed this batch.
    current_state = (
        spark.read
        .format("iceberg")
        .table("rest.silver.item_attributes_spark")
        .groupBy("category")
        .agg(count("item_id").alias("item_count"))
        .withColumn("commit_ts", current_timestamp())
    )

    current_state.createOrReplaceTempView("_gold_category_count_updates")

    spark.sql("""
        MERGE INTO rest.gold.item_category_count_spark AS t
        USING _gold_category_count_updates AS s
        ON t.category = s.category
        WHEN MATCHED THEN UPDATE SET
            t.item_count = s.item_count,
            t.commit_ts  = s.commit_ts
        WHEN NOT MATCHED THEN INSERT
            (category, item_count, commit_ts)
            VALUES (s.category, s.item_count, s.commit_ts)
    """)


def main():
    spark = (
        SparkSession.builder
        .appName("gold-item-category-count-spark")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    attributes = (
        spark.readStream
        .format("iceberg")
        .option("streaming-skip-delete-snapshots", "true")
        .load("rest.silver.item_attributes_spark")
    )

    query = (
        attributes.writeStream
        .foreachBatch(upsert_to_gold)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime="30 seconds")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
