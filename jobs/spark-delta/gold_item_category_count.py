"""
Spark Structured Streaming: Delta silver.item_attributes → Delta gold.item_category_count

Stream reads Delta silver. On each micro-batch, does a full batch read of the
current silver state and recomputes item count per category via MERGE INTO.

Delta streaming reads work on both append AND update/delete commits, making
the full bronze → silver → gold streaming chain possible with Delta Lake.

Trigger: every 30 seconds.
"""

import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import current_timestamp, count

CHECKPOINT_BASE   = os.environ.get("CHECKPOINT_BASE", "s3a://warehouse/_chk")
CHECKPOINT_PATH   = f"{CHECKPOINT_BASE}/gold_item_category_count_delta"
SILVER_TABLE_PATH = "s3a://warehouse/delta/silver/item_attributes"
GOLD_TABLE_PATH   = "s3a://warehouse/delta/gold/item_category_count"


def upsert_to_gold(batch_df: DataFrame, batch_id: int):
    print(f"[gold-item-category-count-delta] batch {batch_id}: {batch_df.count()} rows")
    if batch_df.rdd.isEmpty():
        print(f"[gold-item-category-count-delta] batch {batch_id}: empty, skipping")
        return

    spark = batch_df.sparkSession

    # Full batch read of current silver state so counts reflect latest category
    # for every item, not just what changed this micro-batch.
    current_state = (
        spark.read
        .format("delta")
        .load(SILVER_TABLE_PATH)
        .groupBy("category")
        .agg(count("item_id").alias("item_count"))
        .withColumn("commit_ts", current_timestamp())
    )

    current_state.createOrReplaceTempView("_gold_category_count_delta_updates")

    spark.sql(f"""
        MERGE INTO delta.`{GOLD_TABLE_PATH}` AS t
        USING _gold_category_count_delta_updates AS s
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
        .appName("gold-item-category-count-delta")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    silver = (
        spark.readStream
        .format("delta")
        .option("startingVersion", "0")
        .load(SILVER_TABLE_PATH)
    )

    query = (
        silver.writeStream
        .foreachBatch(upsert_to_gold)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime="30 seconds")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
