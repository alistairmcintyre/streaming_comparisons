"""
Spark Structured Streaming: Hudi silver.item_attributes → Hudi gold.item_category_count

Stream reads Hudi silver (incremental). On each micro-batch, does a full
snapshot read of silver to recompute item counts per category, then upserts
into the gold MOR table.

Trigger: every 30 seconds.
"""

import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import current_timestamp, count

CHECKPOINT_BASE   = os.environ.get("CHECKPOINT_BASE", "s3a://warehouse/_chk")
CHECKPOINT_PATH   = f"{CHECKPOINT_BASE}/gold_item_category_count_hudi"
SILVER_TABLE_PATH = "s3a://warehouse/hudi/silver/item_attributes"
GOLD_TABLE_PATH   = "s3a://warehouse/hudi/gold/item_category_count"


def upsert_to_gold(batch_df: DataFrame, batch_id: int):
    print(f"[gold-item-category-count-hudi] batch {batch_id}: {batch_df.count()} rows")
    if batch_df.rdd.isEmpty():
        print(f"[gold-item-category-count-hudi] batch {batch_id}: empty, skipping")
        return

    spark = batch_df.sparkSession

    # Full snapshot read of silver to get accurate category counts
    current_state = (
        spark.read
        .format("hudi")
        .load(SILVER_TABLE_PATH)
        .groupBy("category")
        .agg(count("item_id").alias("item_count"))
        .withColumn("commit_ts", current_timestamp())
    )

    hudi_opts = {
        "hoodie.table.name": "gold_item_category_count",
        "hoodie.datasource.write.table.type": "MERGE_ON_READ",
        "hoodie.datasource.write.operation": "upsert",
        "hoodie.datasource.write.recordkey.field": "category",
        "hoodie.datasource.write.precombine.field": "commit_ts",
        "hoodie.datasource.write.hive_style_partitioning": "true",
        "hoodie.datasource.write.partitionpath.field": "",
        "hoodie.metadata.enable": "false",
    }

    (
        current_state.write
        .format("hudi")
        .options(**hudi_opts)
        .mode("append")
        .save(GOLD_TABLE_PATH)
    )


def main():
    spark = (
        SparkSession.builder
        .appName("gold-item-category-count-hudi")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    silver = (
        spark.readStream
        .format("hudi")
        .option("hoodie.datasource.query.type", "incremental")
        .option("hoodie.datasource.read.begin.instanttime", "0")
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
