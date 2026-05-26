"""
Spark Structured Streaming: bronze.item_sales_spark → silver.item_sales_spark
"""

import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, current_timestamp, to_date, row_number
from pyspark.sql.window import Window

CHECKPOINT_BASE = os.environ.get("CHECKPOINT_BASE", "s3a://warehouse/_chk")
CHECKPOINT_PATH = f"{CHECKPOINT_BASE}/silver_item_sales_spark"


def upsert_to_silver(batch_df: DataFrame, batch_id: int):
    print(f"[silver-sales] batch {batch_id}: {batch_df.count()} rows")
    if batch_df.rdd.isEmpty():
        print(f"[silver-sales] batch {batch_id}: empty, skipping")
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

    latest.createOrReplaceTempView("_silver_sales_updates")

    spark.sql("""
        MERGE INTO rest.silver.item_sales_spark AS t
        USING _silver_sales_updates AS s
        ON t.item_id = s.item_id
        WHEN MATCHED AND s.op = 'd'
            THEN DELETE
        WHEN MATCHED AND s.op <> 'd'
             AND s.source_updated_at >= t.source_updated_at
            THEN UPDATE SET
                t.quantity          = s.quantity,
                t.total_price       = s.total_price,
                t.source_updated_at = s.source_updated_at,
                t.event_ts          = s.event_ts,
                t.event_date        = s.event_date,
                t.ingest_ts         = s.ingest_ts,
                t.commit_ts         = s.commit_ts
        WHEN NOT MATCHED AND s.op <> 'd'
            THEN INSERT (item_id, quantity, total_price, source_updated_at,
                         event_ts, event_date, ingest_ts, commit_ts)
                 VALUES (s.item_id, s.quantity, s.total_price, s.source_updated_at,
                         s.event_ts, s.event_date, s.ingest_ts, s.commit_ts)
    """)


def main():
    spark = (
        SparkSession.builder
        .appName("silver-item-sales-spark")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    bronze = (
        spark.readStream
        .format("iceberg")
        .option("stream-from-timestamp", "0")
        .load("rest.bronze.item_sales_spark")
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
