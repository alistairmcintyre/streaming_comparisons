"""
Spark Structured Streaming: Hudi bronze.customers → Hudi silver.customers

Hudi silver uses MOR (Merge-On-Read) with upsert. Deletes are handled by
issuing a Hudi delete operation for rows where op='d' (GDPR erasure).
"""

import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, current_timestamp, to_date, row_number
from pyspark.sql.window import Window

CHECKPOINT_BASE   = os.environ.get("CHECKPOINT_BASE", "s3a://warehouse/_chk")
CHECKPOINT_PATH   = f"{CHECKPOINT_BASE}/silver_customers_hudi"
BRONZE_TABLE_PATH = "s3a://warehouse/hudi/bronze/customers"
SILVER_TABLE_PATH = "s3a://warehouse/hudi/silver/customers"


def upsert_to_silver(batch_df: DataFrame, batch_id: int):
    print(f"[silver-customers-hudi] batch {batch_id}: {batch_df.count()} rows")
    if batch_df.rdd.isEmpty():
        print(f"[silver-customers-hudi] batch {batch_id}: empty, skipping")
        return

    # event_ts (Debezium source.ts_ms) is present + monotonic on deletes;
    # source_updated_at is NULL on a delete and would let an in-batch insert
    # mask the delete.
    w = Window.partitionBy("customer_id").orderBy(
        col("event_ts").desc(),
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
    deletes = latest.filter(col("op") == "d").select("customer_id")
    upserts = latest.filter(col("op") != "d")

    hudi_common = {
        "hoodie.table.name": "silver_customers",
        "hoodie.datasource.write.table.type": "MERGE_ON_READ",
        "hoodie.datasource.write.recordkey.field": "customer_id",
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
        .appName("silver-customers-hudi")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

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
