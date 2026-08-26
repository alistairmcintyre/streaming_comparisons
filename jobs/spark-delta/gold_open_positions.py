"""
Spark Structured Streaming: Delta bronze.trades → Delta gold.open_positions.

Folds signed fills into a net book per (account_id, symbol) via MERGE — the
update-heavy table that stresses the format. Work is proportional to the fills in
each micro-batch (append stream of silver.trades), never a full scan:
    BUY  -> +quantity      SELL -> -quantity
    net_quantity += Σ signed_qty ;  net_notional += Σ signed_qty*price
    status = OPEN while net_quantity != 0, else CLOSED
Enriched with account country/tier by a BATCH snapshot lookup of silver.accounts
(dimension → attribute at processing time; no CDF needed).

Idempotency: foreachBatch is at-least-once and the counters aren't idempotent, so
each MERGE is stamped with (txnAppId, txnVersion=batchId) → a replayed batch is a no-op.
"""
import os
from delta_tables import ensure_all  # in-pipeline DDL
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when, lit, sum as _sum, count as _count, broadcast

CHECKPOINT_BASE = os.environ.get("CHECKPOINT_BASE", "s3a://warehouse/_chk")
CHECKPOINT_PATH = f"{CHECKPOINT_BASE}/gold_open_positions_delta"
_BASE = os.environ.get("DELTA_WAREHOUSE", "s3a://warehouse/delta")
SILVER_TRADES   = f"{_BASE}/silver/trades"
SILVER_ACCOUNTS = f"{_BASE}/silver/accounts"
GOLD            = f"{_BASE}/gold/open_positions"
TXN_APP_ID      = "delta-gold-open-positions"


def fold_to_book(batch: DataFrame, batch_id: int):
    spark = batch.sparkSession
    signed = batch.withColumn(
        "sq", when(col("side") == "BUY", col("quantity")).otherwise(-col("quantity")))
    deltas = (signed.groupBy("account_id", "symbol").agg(
        _sum("sq").alias("dq"),
        _sum(col("sq") * col("price")).alias("dnot"),
        _count(lit(1)).alias("dcnt")))

    # Enrich with the current account attributes (batch snapshot of the dimension).
    try:
        accts = (spark.read.format("delta").load(SILVER_ACCOUNTS)
                 .select("account_id", "country", "tier"))
        deltas = deltas.join(broadcast(accts), "account_id", "left")
    except Exception:
        deltas = deltas.withColumn("country", lit(None).cast("string")) \
                       .withColumn("tier", lit(None).cast("string"))

    deltas = deltas.cache()
    try:
        if not deltas.take(1):
            return
        deltas.createOrReplaceTempView("_pos_deltas")
        spark.conf.set("spark.databricks.delta.write.txnAppId", TXN_APP_ID)
        spark.conf.set("spark.databricks.delta.write.txnVersion", str(batch_id))
        spark.sql(f"""
            MERGE INTO delta.`{GOLD}` AS t
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
        print(f"[gold-open-positions-delta] batch {batch_id}: folded fills into the book")
    finally:
        deltas.unpersist()


def main():
    spark = SparkSession.builder.appName("gold-open-positions-delta").getOrCreate()
    ensure_all(spark)  # idempotent create-if-not-exists; no separate ddl-init
    spark.sparkContext.setLogLevel("WARN")

    # silver.trades is append-only (deduped fills) → plain streaming read, bounded per batch.
    trades = (spark.readStream.format("delta")
              .option("startingVersion", "0")
              .option("maxFilesPerTrigger", "200")
              .load(SILVER_TRADES))

    (trades.writeStream.foreachBatch(fold_to_book)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime="15 seconds")
        .start().awaitTermination())


if __name__ == "__main__":
    main()
