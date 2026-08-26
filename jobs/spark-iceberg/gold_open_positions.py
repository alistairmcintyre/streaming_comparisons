"""
Spark Structured Streaming: Iceberg silver.trades_spark → gold.open_positions_spark.

Folds signed fills into a net book per (account_id, symbol) via MERGE, work ∝ the
fills in each micro-batch (append stream of silver.trades — never a full scan):
    BUY -> +quantity   SELL -> -quantity ;  net_quantity += Σ signed_qty
Enriched with account country/tier by a batch snapshot lookup of silver.accounts.

EXACTLY-ONCE: Iceberg has NO idempotent-write primitive (no txnAppId/txnVersion like
Delta), so this incrementing `+=` MERGE is **at-least-once** — a failed micro-batch
that replays could double-apply. Mitigation is a periodic BATCH RECONCILIATION
(recompute the whole book from silver.trades off-peak) to correct drift; see
gold_open_positions_reconcile.py. In a no-failure run the fold is exact.
(This is the concrete cost of Iceberg lacking Delta's idempotent write / Flink's
exactly-once state — cheap incremental, but you buy correctness with a reconcile.)
"""
import os
from iceberg_tables import ensure_all  # in-pipeline DDL
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when, lit, sum as _sum, count as _count, broadcast

CHECKPOINT_BASE = os.environ.get("CHECKPOINT_BASE", "s3a://warehouse/_chk")
CHECKPOINT_PATH = f"{CHECKPOINT_BASE}/gold_open_positions_spark"
SILVER_TRADES   = "rest.silver.trades_spark"
SILVER_ACCOUNTS = "rest.silver.accounts_spark"
GOLD            = "rest.gold.open_positions_spark"


def fold_to_book(batch: DataFrame, batch_id: int):
    spark = batch.sparkSession
    signed = batch.withColumn(
        "sq", when(col("side") == "BUY", col("quantity")).otherwise(-col("quantity")))
    deltas = (signed.groupBy("account_id", "symbol").agg(
        _sum("sq").alias("dq"),
        _sum(col("sq") * col("price")).alias("dnot"),
        _count(lit(1)).alias("dcnt")))

    try:
        accts = spark.table(SILVER_ACCOUNTS).select("account_id", "country", "tier")
        deltas = deltas.join(broadcast(accts), "account_id", "left")
    except Exception:
        deltas = deltas.withColumn("country", lit(None).cast("string")) \
                       .withColumn("tier", lit(None).cast("string"))

    deltas = deltas.cache()
    try:
        if not deltas.take(1):
            return
        deltas.createOrReplaceTempView("_pos_deltas")
        spark.sql(f"""
            MERGE INTO {GOLD} AS t
            USING _pos_deltas AS s
            ON t.account_id = s.account_id AND t.symbol = s.symbol
            WHEN MATCHED THEN UPDATE SET
                t.net_quantity = t.net_quantity + s.dq,
                t.net_notional = t.net_notional + s.dnot,
                t.trade_count  = t.trade_count + s.dcnt,
                t.status       = CASE WHEN (t.net_quantity + s.dq) <> 0 THEN 'OPEN' ELSE 'CLOSED' END,
                t.country      = s.country,
                t.tier         = s.tier,
                t.commit_ts    = current_timestamp()
            WHEN NOT MATCHED THEN INSERT
                (account_id, symbol, net_quantity, net_notional, trade_count, status, country, tier, commit_ts)
                VALUES (s.account_id, s.symbol, s.dq, s.dnot, s.dcnt,
                        CASE WHEN s.dq <> 0 THEN 'OPEN' ELSE 'CLOSED' END,
                        s.country, s.tier, current_timestamp())
        """)
        print(f"[gold-open-positions-spark] batch {batch_id}: folded fills into the book")
    finally:
        deltas.unpersist()


def main():
    spark = SparkSession.builder.appName("gold-open-positions-spark").getOrCreate()
    ensure_all(spark)  # idempotent create-if-not-exists; no separate ddl-init
    spark.sparkContext.setLogLevel("WARN")

    trades = (spark.readStream.format("iceberg")
              .option("streaming-skip-overwrite-snapshots", "true")
              .option("streaming-skip-delete-snapshots", "true")
              .option("streaming-max-files-per-micro-batch", "500")
              .load(SILVER_TRADES))

    (trades.writeStream.foreachBatch(fold_to_book)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime="15 seconds")
        .start().awaitTermination())


if __name__ == "__main__":
    main()
