"""
Spark Structured Streaming: Delta silver.customers (Change Data Feed) →
Delta gold.customers_per_country — INCREMENTAL, retraction-aware aggregation.

Reads ONLY silver's change feed (readChangeFeed), never the full table, so the
work per micro-batch is proportional to the number of CHANGES, not to silver's
size — it scales to a billions-row silver. Each change row contributes +/-1 to a
country's running count; the net per-country delta is MERGEd into the (tiny) gold
table, which holds the counts as state:
    insert, update_postimage  -> +1  (row enters a country)
    delete,  update_preimage  -> -1  (row leaves a country)
A country-change update emits preimage(-1 old) + postimage(+1 new), so a move
between countries nets correctly — no full recount.

Replaces the previous design, which stream-read silver and re-scanned the WHOLE
table every micro-batch (O(rows) — dead at billions) AND crashed on silver's
MERGE update commits (DELTA_SOURCE_TABLE_IGNORE_CHANGES). CDF fixes both.

Idempotency: foreachBatch is at-least-once and counters aren't idempotent under
retry, so each MERGE is stamped with (txnAppId, txnVersion=batchId) — Delta skips
a (appId, version) it already committed, making a replayed batch a no-op.
"""
import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when, lit, sum as _sum

CHECKPOINT_BASE   = os.environ.get("CHECKPOINT_BASE", "s3a://warehouse/_chk")
CHECKPOINT_PATH   = f"{CHECKPOINT_BASE}/gold_customers_per_country_delta"
SILVER_TABLE_PATH = "s3a://warehouse/delta/silver/customers"
GOLD_TABLE_PATH   = "s3a://warehouse/delta/gold/customers_per_country"
TXN_APP_ID        = "delta-gold-customers-per-country"

PLUS  = ("insert", "update_postimage")   # row enters a country
MINUS = ("delete", "update_preimage")    # row leaves a country


def apply_deltas_to_gold(batch_df: DataFrame, batch_id: int):
    spark = batch_df.sparkSession

    # Net +/-1 per country from this batch's change rows (cached: read once).
    deltas = (
        batch_df
        .withColumn(
            "sign",
            when(col("_change_type").isin(*PLUS), lit(1))
            .when(col("_change_type").isin(*MINUS), lit(-1))
            .otherwise(lit(0)),
        )
        .filter(col("country").isNotNull() & (col("sign") != 0))
        .groupBy("country")
        .agg(_sum("sign").alias("delta"))
        .filter(col("delta") != 0)
    ).cache()

    try:
        if not deltas.take(1):
            return
        deltas.createOrReplaceTempView("_gold_deltas")

        # Idempotent MERGE: Delta skips the commit if (appId, batchId) already applied.
        spark.conf.set("spark.databricks.delta.write.txnAppId", TXN_APP_ID)
        spark.conf.set("spark.databricks.delta.write.txnVersion", str(batch_id))

        spark.sql(f"""
            MERGE INTO delta.`{GOLD_TABLE_PATH}` AS t
            USING _gold_deltas AS s
            ON t.country = s.country
            WHEN MATCHED THEN UPDATE SET
                t.customer_count = t.customer_count + s.delta,
                t.commit_ts      = current_timestamp()
            WHEN NOT MATCHED THEN INSERT (country, customer_count, commit_ts)
                VALUES (s.country, s.delta, current_timestamp())
        """)
        print(f"[gold-customers-per-country-delta] batch {batch_id}: applied country deltas")
    finally:
        deltas.unpersist()


def main():
    spark = SparkSession.builder.appName("gold-customers-per-country-delta").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # Read ONLY the change feed — proportional to changes, not table size.
    # maxFilesPerTrigger bounds each micro-batch (including the one-time catch-up
    # from version 0), so memory stays flat even against a huge silver.
    changes = (
        spark.readStream
        .format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", "0")
        .option("maxFilesPerTrigger", "200")
        .load(SILVER_TABLE_PATH)
    )

    query = (
        changes.writeStream
        .foreachBatch(apply_deltas_to_gold)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime="15 seconds")
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
