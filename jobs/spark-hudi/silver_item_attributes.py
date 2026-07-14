"""
Spark Structured Streaming: Hudi bronze.item_attributes → Hudi silver.item_attributes

Hudi silver uses MOR (Merge-On-Read) with upsert. Deletes are handled by
issuing a Hudi delete operation for rows where op='d'.

Note: Hudi streaming reads require the IncrementalSource — Hudi exposes
new commits via spark.readStream.format("hudi") with INCREMENTAL query type.
"""

import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, current_timestamp, to_date, row_number
from pyspark.sql.window import Window

CHECKPOINT_BASE   = os.environ.get("CHECKPOINT_BASE", "s3a://warehouse/_chk")
CHECKPOINT_PATH   = f"{CHECKPOINT_BASE}/silver_item_attributes_hudi"
BRONZE_TABLE_PATH = "s3a://warehouse/hudi/bronze/item_attributes"
SILVER_TABLE_PATH = "s3a://warehouse/hudi/silver/item_attributes"


def upsert_to_silver(batch_df: DataFrame, batch_id: int):
    print(f"[silver-attr-hudi] batch {batch_id}: {batch_df.count()} rows")
    if batch_df.rdd.isEmpty():
        print(f"[silver-attr-hudi] batch {batch_id}: empty, skipping")
        return

    spark = batch_df.sparkSession

    w = Window.partitionBy("item_id").orderBy(
        col("source_updated_at").desc(),
        col("kafka_offset").desc(),
    )
    latest = (
        batch_df
        .withColumn("_rn", row_number().over(w))
        .filter(col("_rn") == 1)
        .drop("_rn", "kafka_offset", "kafka_partition")
        .withColumn("event_date", to_date(col("event_ts")))
        .withColumn("commit_ts", current_timestamp())
    )

    # Split deletes from upserts — Hudi handles them via separate operations
    deletes = latest.filter(col("op") == "d").select("item_id")
    upserts = latest.filter(col("op") != "d")

    hudi_common = {
        "hoodie.table.name": "silver_item_attributes",
        "hoodie.datasource.write.table.type": "MERGE_ON_READ",
        "hoodie.datasource.write.recordkey.field": "item_id",
        "hoodie.datasource.write.precombine.field": "source_updated_at",
        "hoodie.datasource.write.hive_style_partitioning": "true",
        "hoodie.datasource.write.partitionpath.field": "event_date",
        "hoodie.metadata.enable": "false",
    }

    if not upserts.rdd.isEmpty():
        upsert_opts = {**hudi_common, "hoodie.datasource.write.operation": "upsert"}
        (
            upserts.write
            .format("hudi")
            .options(**upsert_opts)
            .mode("append")
            .save(SILVER_TABLE_PATH)
        )

    if not deletes.rdd.isEmpty():
        delete_opts = {
            **hudi_common,
            "hoodie.datasource.write.operation": "delete",
        }
        (
            deletes.write
            .format("hudi")
            .options(**delete_opts)
            .mode("append")
            .save(SILVER_TABLE_PATH)
        )


def main():
    spark = (
        SparkSession.builder
        .appName("silver-item-attributes-hudi")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Hudi incremental streaming read — reads from the earliest commit
    bronze = (
        spark.readStream
        .format("hudi")
        .option("hoodie.datasource.query.type", "incremental")
        .option("hoodie.datasource.read.begin.instanttime", "0")
        .load(BRONZE_TABLE_PATH)
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
