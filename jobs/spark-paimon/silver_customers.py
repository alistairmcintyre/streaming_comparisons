"""
Spark Structured Streaming: Paimon bronze.customers → Paimon silver.customers

Paimon silver uses a primary-key table (MOR by default). MERGE INTO is used for
upserts and hard deletes (op='d' = GDPR erasure), identical to the Iceberg/Delta
pattern. Paimon primary-key tables are streaming-readable, enabling the full
bronze → silver → gold streaming chain.
"""

import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, current_timestamp, to_date, row_number
from pyspark.sql.window import Window

from cdc import iceberg_props  # shared helper (mounted at /opt/shared)

CHECKPOINT_BASE = os.environ.get("CHECKPOINT_BASE", "s3a://warehouse/_chk")
CHECKPOINT_PATH = f"{CHECKPOINT_BASE}/silver_customers_paimon"


def create_silver_table(spark):
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS paimon.silver.customers (
            customer_id      BIGINT       NOT NULL,
            op               STRING,
            name             STRING,
            country          STRING,
            segment          STRING,
            source_updated_at TIMESTAMP,
            event_ts         TIMESTAMP,
            event_date       DATE,
            ingest_ts        TIMESTAMP,
            commit_ts        TIMESTAMP
        ) TBLPROPERTIES (
            'primary-key'    = 'customer_id',
            'merge-engine'   = 'deduplicate',
            'file.format'    = 'parquet',
            'changelog-producer' = 'input'{iceberg_props()}
        )
    """)


def upsert_to_silver(batch_df: DataFrame, batch_id: int):
    print(f"[silver-customers-paimon] batch {batch_id}: {batch_df.count()} rows")
    if batch_df.rdd.isEmpty():
        print(f"[silver-customers-paimon] batch {batch_id}: empty, skipping")
        return

    spark = batch_df.sparkSession

    # Order by event_ts (Debezium source.ts_ms) — present and monotonic on
    # deletes, unlike source_updated_at which is NULL on a delete (after=null)
    # and would let an in-batch insert mask the delete.
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

    latest.createOrReplaceTempView("_silver_customers_paimon_updates")

    spark.sql("""
        MERGE INTO paimon.silver.customers AS t
        USING _silver_customers_paimon_updates AS s
        ON t.customer_id = s.customer_id
        WHEN MATCHED AND s.op = 'd'
            THEN DELETE
        WHEN MATCHED AND s.op <> 'd'
             AND s.event_ts >= t.event_ts
            THEN UPDATE SET
                t.op                = s.op,
                t.name              = s.name,
                t.country           = s.country,
                t.segment           = s.segment,
                t.source_updated_at = s.source_updated_at,
                t.event_ts          = s.event_ts,
                t.event_date        = s.event_date,
                t.ingest_ts         = s.ingest_ts,
                t.commit_ts         = s.commit_ts
        WHEN NOT MATCHED AND s.op <> 'd'
            THEN INSERT (customer_id, op, name, country, segment, source_updated_at,
                         event_ts, event_date, ingest_ts, commit_ts)
                 VALUES (s.customer_id, s.op, s.name, s.country, s.segment, s.source_updated_at,
                         s.event_ts, s.event_date, s.ingest_ts, s.commit_ts)
    """)


def main():
    spark = (
        SparkSession.builder
        .appName("silver-customers-paimon")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    spark.sql("CREATE DATABASE IF NOT EXISTS paimon.silver")
    create_silver_table(spark)

    bronze = (
        spark.readStream
        .format("paimon")
        .option("scan.mode", "from-snapshot-full")
        .option("scan.snapshot-id", "1")
        .table("paimon.bronze.customers")
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
