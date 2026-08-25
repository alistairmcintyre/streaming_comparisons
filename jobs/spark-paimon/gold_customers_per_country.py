"""
Spark Structured Streaming: Paimon silver.customers → Paimon gold.customers_per_country

Stream reads Paimon silver (primary-key table, changelog-producer=input enables
proper streaming change records). On each micro-batch, does a full snapshot read
of silver to recompute active customers per country, then MERGEs into gold.

Trigger: every 30 seconds.
"""

import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import current_timestamp, count

from cdc import iceberg_props  # shared helper (mounted at /opt/shared)

CHECKPOINT_BASE = os.environ.get("CHECKPOINT_BASE", "s3a://warehouse/_chk")
CHECKPOINT_PATH = f"{CHECKPOINT_BASE}/gold_customers_per_country_paimon"


def create_gold_table(spark):
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS paimon.gold.customers_per_country (
            country        STRING    NOT NULL,
            customer_count BIGINT,
            commit_ts      TIMESTAMP
        ) TBLPROPERTIES (
            'primary-key' = 'country',
            'merge-engine' = 'deduplicate',
            'file.format'  = 'parquet'{iceberg_props()}
        )
    """)


def upsert_to_gold(batch_df: DataFrame, batch_id: int):
    print(f"[gold-customers-per-country-paimon] batch {batch_id}: {batch_df.count()} rows")
    if batch_df.rdd.isEmpty():
        print(f"[gold-customers-per-country-paimon] batch {batch_id}: empty, skipping")
        return

    spark = batch_df.sparkSession

    current_state = (
        spark.read
        .format("paimon")
        .table("paimon.silver.customers")
        .groupBy("country")
        .agg(count("customer_id").alias("customer_count"))
        .withColumn("commit_ts", current_timestamp())
    )

    current_state.createOrReplaceTempView("_gold_customers_per_country_paimon_updates")

    spark.sql("""
        MERGE INTO paimon.gold.customers_per_country AS t
        USING _gold_customers_per_country_paimon_updates AS s
        ON t.country = s.country
        WHEN MATCHED THEN UPDATE SET
            t.customer_count = s.customer_count,
            t.commit_ts      = s.commit_ts
        WHEN NOT MATCHED THEN INSERT
            (country, customer_count, commit_ts)
            VALUES (s.country, s.customer_count, s.commit_ts)
    """)


def main():
    spark = (
        SparkSession.builder
        .appName("gold-customers-per-country-paimon")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    spark.sql("CREATE DATABASE IF NOT EXISTS paimon.gold")
    create_gold_table(spark)

    silver = (
        spark.readStream
        .format("paimon")
        .option("scan.mode", "from-snapshot-full")
        .option("scan.snapshot-id", "1")
        .table("paimon.silver.customers")
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
