"""
Spark Structured Streaming: Delta bronze.trades → Delta gold.open_positions.

Folds signed fills into a net book per (account_id, symbol) via MERGE, the
update-heavy table that stresses the format. Work is proportional to the fills in
each micro-batch (append stream of silver.trades), never a full scan:
    BUY  -> +quantity      SELL -> -quantity
    net_quantity += Σ signed_qty ;  net_notional += Σ signed_qty*price
    status = OPEN while net_quantity != 0, else CLOSED
Enrichment is not denormalised into gold. country/tier are account attributes, and a
current-state position row has no defensible temporal semantic for them, the value
would be "whatever the last batch that happened to touch this row saw", which is an
artifact of batch boundaries rather than a fact about the position. Enrich at read time:

    SELECT p.*, a.country, a.tier
    FROM gold.open_positions p
    LEFT JOIN silver.accounts a USING (account_id)

LEFT, always: the trades and accounts CDC streams are independent, so a fill can land
before its account row does. An inner join would silently drop that position from the
book, the row count would depend on dimension timing, with no error.

Idempotency: foreachBatch is at-least-once and the counters aren't idempotent, so
each MERGE is stamped with (txnAppId, txnVersion=batchId) → a replayed batch is a no-op.
"""
import os
from delta_tables import ensure_all  # in-pipeline DDL
from pyspark.sql import SparkSession, DataFrame
from latency import (SAMPLE_ACCOUNTS, observe_event_time,
                     attach_latency_listener)
from pyspark.sql.functions import (
    col, when, lit, sum as _sum, count as _count,
    min as _min, max as _max,
)

CHECKPOINT_BASE = os.environ.get("CHECKPOINT_BASE", "s3a://warehouse/_chk")
CHECKPOINT_PATH = f"{CHECKPOINT_BASE}/gold_open_positions_delta"
_BASE = os.environ.get("DELTA_WAREHOUSE", "s3a://warehouse/delta")
SILVER_TRADES   = f"{_BASE}/silver/trades"
GOLD            = f"{_BASE}/gold/open_positions"
TXN_APP_ID      = "delta-gold-open-positions"


def fold_to_book(batch: DataFrame, batch_id: int):
    spark = batch.sparkSession
    signed = batch.withColumn(
        "sq", when(col("side") == "BUY", col("quantity")).otherwise(-col("quantity")))
    deltas = (signed.groupBy("account_id", "symbol").agg(
        _sum("sq").alias("dq"),
        _sum(col("sq") * col("price")).alias("dnot"),
        _count(lit(1)).alias("dcnt"),
        # EVENT time carried through the fold: opened_at = MIN over the position's
        # history, last_updated_at = MAX. Both fold incrementally via least/greatest
        # in the MERGE, so this stays proportional to the batch, no rescan.
        _min("executed_at").alias("dmin"),
        _max("executed_at").alias("dmax")))

    # No dimension read here, deliberately. This previously did a FULL batch read of
  # silver.accounts on every micro-batch (every 15s, forever) and broadcast it, to
    # stamp country/tier onto the book. That is a rescan of a silver table (the one
    # thing README.md (Design principles) rules out), it is O(dimension) rather than
    # O(batch), and the value it wrote had no defensible temporal meaning anyway.
    # country/tier now come from a LEFT JOIN to silver.accounts at query time.

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
                -- least/greatest skip NULLs, so a pre-existing row written before
                -- these columns existed adopts the batch value instead of staying NULL.
                t.opened_at       = least(t.opened_at, s.dmin),
                t.last_updated_at = greatest(t.last_updated_at, s.dmax),
                t.commit_ts    = current_timestamp()
            WHEN NOT MATCHED THEN INSERT
                (account_id, symbol, net_quantity, net_notional, trade_count, status,
                 opened_at, last_updated_at, commit_ts)
                VALUES (s.account_id, s.symbol, s.dq, s.dnot, s.dcnt,
                        CASE WHEN s.dq <> 0 THEN 'OPEN' ELSE 'CLOSED' END,
                        s.dmin, s.dmax, current_timestamp())
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

    attach_latency_listener(spark, "delta-gold")
    trades = observe_event_time(trades, sample=SAMPLE_ACCOUNTS)

    (trades.writeStream.foreachBatch(fold_to_book)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime="15 seconds")
        .start().awaitTermination())


if __name__ == "__main__":
    main()
