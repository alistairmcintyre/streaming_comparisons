"""
Spark Structured Streaming: silver.customers_spark → gold.customers_per_country_spark

Stream reads silver customers. On each micro-batch, does a full batch read of the
current silver state and recomputes the active customer count per country.

Demonstrates that MoR UPDATE snapshots flow through to a streaming consumer —
changing a customer's country (or a GDPR delete) in Postgres is reflected in the
gold table within one trigger interval.

The gold table is updated via MERGE INTO on country.

Trigger: every 30 seconds.
"""

import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import current_timestamp, count

CHECKPOINT_PATH = "s3a://warehouse/_chk/gold_customers_per_country_spark"


def upsert_to_gold(batch_df: DataFrame, batch_id: int):
    print(f"[gold-customers-per-country] batch {batch_id}: {batch_df.count()} rows")
    if batch_df.rdd.isEmpty():
        print(f"[gold-customers-per-country] batch {batch_id}: empty, skipping")
        return

    spark = batch_df.sparkSession

    # Full batch read of current silver state so the counts reflect the latest
    # country for every customer, not just what changed this batch.
    current_state = (
        spark.read
        .format("iceberg")
        .table("rest.silver.customers_spark")
        .groupBy("country")
        .agg(count("customer_id").alias("customer_count"))
        .withColumn("commit_ts", current_timestamp())
    )

    current_state.createOrReplaceTempView("_gold_customers_per_country_updates")

    spark.sql("""
        MERGE INTO rest.gold.customers_per_country_spark AS t
        USING _gold_customers_per_country_updates AS s
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
        .appName("gold-customers-per-country-spark")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    customers = (
        spark.readStream
        .format("iceberg")
        .option("streaming-skip-delete-snapshots", "true")
        .load("rest.silver.customers_spark")
    )

    query = (
        customers.writeStream
        .foreachBatch(upsert_to_gold)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime="30 seconds")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
