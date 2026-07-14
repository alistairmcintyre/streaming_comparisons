"""
Spark Structured Streaming: Paimon silver.item_attributes → Paimon gold.item_category_count

Stream reads Paimon silver (primary-key table, changelog-producer=input enables
proper streaming change records). On each micro-batch, does a full snapshot read
of silver to recompute item counts per category, then MERGEs into gold.

Trigger: every 30 seconds.
"""

import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import current_timestamp, count

CHECKPOINT_BASE = os.environ.get("CHECKPOINT_BASE", "s3a://warehouse/_chk")
CHECKPOINT_PATH = f"{CHECKPOINT_BASE}/gold_item_category_count_paimon"


def create_gold_table(spark):
    spark.sql("""
        CREATE TABLE IF NOT EXISTS paimon.gold.item_category_count (
            category   STRING    NOT NULL,
            item_count BIGINT,
            commit_ts  TIMESTAMP
        ) TBLPROPERTIES (
            'primary-key' = 'category',
            'merge-engine' = 'deduplicate',
            'file.format'  = 'parquet'
        )
    """)


def upsert_to_gold(batch_df: DataFrame, batch_id: int):
    print(f"[gold-item-category-count-paimon] batch {batch_id}: {batch_df.count()} rows")
    if batch_df.rdd.isEmpty():
        print(f"[gold-item-category-count-paimon] batch {batch_id}: empty, skipping")
        return

    spark = batch_df.sparkSession

    # Full snapshot read to ensure counts reflect current silver state
    current_state = (
        spark.read
        .format("paimon")
        .table("paimon.silver.item_attributes")
        .groupBy("category")
        .agg(count("item_id").alias("item_count"))
        .withColumn("commit_ts", current_timestamp())
    )

    current_state.createOrReplaceTempView("_gold_category_count_paimon_updates")

    spark.sql("""
        MERGE INTO paimon.gold.item_category_count AS t
        USING _gold_category_count_paimon_updates AS s
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
        .appName("gold-item-category-count-paimon")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    create_gold_table(spark)

    silver = (
        spark.readStream
        .format("paimon")
        .option("scan.mode", "from-snapshot-full")
        .option("scan.snapshot-id", "1")
        .table("paimon.silver.item_attributes")
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
