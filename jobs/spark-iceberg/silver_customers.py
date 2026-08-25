"""
Spark Structured Streaming: bronze.customers_spark → silver.customers_spark

Keeps a current view of each customer (SCD1). foreachBatch dedupes to the latest
event per customer_id, then MERGE INTO applies inserts/updates and hard-deletes
(op='d', i.e. GDPR erasure).
"""

import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, current_timestamp, to_date, row_number
from pyspark.sql.window import Window

CHECKPOINT_BASE = os.environ.get("CHECKPOINT_BASE", "s3a://warehouse/_chk")
CHECKPOINT_PATH = f"{CHECKPOINT_BASE}/silver_customers_spark"


def upsert_to_silver(batch_df: DataFrame, batch_id: int):
    print(f"[silver-customers] batch {batch_id}: {batch_df.count()} rows")
    if batch_df.rdd.isEmpty():
        print(f"[silver-customers] batch {batch_id}: empty, skipping")
        return

    spark = batch_df.sparkSession

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

    latest.createOrReplaceTempView("_silver_customers_updates")

    spark.sql("""
        MERGE INTO rest.silver.customers_spark AS t
        USING _silver_customers_updates AS s
        ON t.customer_id = s.customer_id
        WHEN MATCHED AND s.op = 'd'
            THEN DELETE
        WHEN MATCHED AND s.op <> 'd'
             AND s.event_ts >= t.event_ts
            THEN UPDATE SET
                t.name              = s.name,
                t.country           = s.country,
                t.segment           = s.segment,
                t.source_updated_at = s.source_updated_at,
                t.event_ts          = s.event_ts,
                t.event_date        = s.event_date,
                t.ingest_ts         = s.ingest_ts,
                t.commit_ts         = s.commit_ts
        WHEN NOT MATCHED AND s.op <> 'd'
            THEN INSERT (customer_id, name, country, segment, source_updated_at,
                         event_ts, event_date, ingest_ts, commit_ts)
                 VALUES (s.customer_id, s.name, s.country, s.segment, s.source_updated_at,
                         s.event_ts, s.event_date, s.ingest_ts, s.commit_ts)
    """)


def main():
    spark = (
        SparkSession.builder
        .appName("silver-customers-spark")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    bronze = (
        spark.readStream
        .format("iceberg")
        .option("stream-from-timestamp", "0")
        .load("rest.bronze.customers_spark")
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
