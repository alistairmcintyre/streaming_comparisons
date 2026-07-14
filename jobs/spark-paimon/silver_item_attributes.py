"""
Spark Structured Streaming: Paimon bronze.item_attributes → Paimon silver.item_attributes

Paimon silver uses a primary-key table (MOR by default). MERGE INTO is used
for upserts and deletes, identical to the Iceberg/Delta pattern.

Paimon primary-key tables support streaming reads natively via the 'paimon'
format — this enables the full bronze → silver → gold streaming chain.
"""

import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, current_timestamp, to_date, row_number
from pyspark.sql.window import Window

CHECKPOINT_BASE = os.environ.get("CHECKPOINT_BASE", "s3a://warehouse/_chk")
CHECKPOINT_PATH = f"{CHECKPOINT_BASE}/silver_item_attributes_paimon"


def create_silver_table(spark):
    spark.sql("""
        CREATE TABLE IF NOT EXISTS paimon.silver.item_attributes (
            item_id          BIGINT       NOT NULL,
            op               STRING,
            name             STRING,
            price            DOUBLE,
            category         STRING,
            source_updated_at TIMESTAMP,
            event_ts         TIMESTAMP,
            event_date       DATE,
            ingest_ts        TIMESTAMP,
            commit_ts        TIMESTAMP
        ) TBLPROPERTIES (
            'primary-key'    = 'item_id',
            'merge-engine'   = 'deduplicate',
            'file.format'    = 'parquet',
            'changelog-producer' = 'input'
        )
    """)


def upsert_to_silver(batch_df: DataFrame, batch_id: int):
    print(f"[silver-attr-paimon] batch {batch_id}: {batch_df.count()} rows")
    if batch_df.rdd.isEmpty():
        print(f"[silver-attr-paimon] batch {batch_id}: empty, skipping")
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

    latest.createOrReplaceTempView("_silver_attr_paimon_updates")

    spark.sql("""
        MERGE INTO paimon.silver.item_attributes AS t
        USING _silver_attr_paimon_updates AS s
        ON t.item_id = s.item_id
        WHEN MATCHED AND s.op = 'd'
            THEN DELETE
        WHEN MATCHED AND s.op <> 'd'
             AND s.source_updated_at >= t.source_updated_at
            THEN UPDATE SET
                t.op                = s.op,
                t.name              = s.name,
                t.price             = s.price,
                t.category          = s.category,
                t.source_updated_at = s.source_updated_at,
                t.event_ts          = s.event_ts,
                t.event_date        = s.event_date,
                t.ingest_ts         = s.ingest_ts,
                t.commit_ts         = s.commit_ts
        WHEN NOT MATCHED AND s.op <> 'd'
            THEN INSERT (item_id, op, name, price, category, source_updated_at,
                         event_ts, event_date, ingest_ts, commit_ts)
                 VALUES (s.item_id, s.op, s.name, s.price, s.category, s.source_updated_at,
                         s.event_ts, s.event_date, s.ingest_ts, s.commit_ts)
    """)


def main():
    spark = (
        SparkSession.builder
        .appName("silver-item-attributes-paimon")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    create_silver_table(spark)

    # Paimon streaming read from the beginning of history
    bronze = (
        spark.readStream
        .format("paimon")
        .option("scan.mode", "from-snapshot-full")
        .option("scan.snapshot-id", "1")
        .table("paimon.bronze.item_attributes")
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
