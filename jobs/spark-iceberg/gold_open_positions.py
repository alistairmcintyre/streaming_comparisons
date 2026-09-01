"""
Spark Structured Streaming: Iceberg silver.trades_spark → gold.open_positions_spark.

Folds signed fills into a net book per (account_id, symbol) via MERGE, work ∝ the
fills in each micro-batch (append stream of silver.trades, never a full scan):
    BUY -> +quantity   SELL -> -quantity ;  net_quantity += Σ signed_qty
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

EXACTLY-ONCE: Iceberg has no idempotent-write primitive (no txnAppId/txnVersion like
Delta), so this incrementing `+=` MERGE is **at-least-once**, a failed micro-batch that
replays double-applies, and the drift is permanent. In a no-failure run the fold is exact.

This is DETECTED, NOT REPAIRED, and that is deliberate. After the load is drained
(infra/aws/scripts/quiesce-run.sh) the snapshot checks the fold invariant

    sum(gold.trade_count) == count(silver.trades)

per engine and publishes any difference in invariants.csv. A reconcile job that
recomputed the book would repair drift but add write amplification to exactly the
engine that drifted, contaminating the numbers this pipeline exists to produce.
"Did Iceberg drift under sustained load, and by how much?" is a result worth having;
silently correcting it is not. This is the concrete cost of lacking Delta's idempotent
write and Flink's exactly-once state: cheap incremental writes, measured drift.
"""
import os
from iceberg_tables import ensure_all  # in-pipeline DDL
from pyspark.sql import SparkSession, DataFrame
from latency import (SAMPLE_ACCOUNTS, observe_event_time,
                     attach_latency_listener)
from pyspark.sql.functions import (
    col, when, lit, sum as _sum, count as _count,
    min as _min, max as _max,
)

CHECKPOINT_BASE = os.environ.get("CHECKPOINT_BASE", "s3a://warehouse/_chk")
CHECKPOINT_PATH = f"{CHECKPOINT_BASE}/gold_open_positions_spark"
SILVER_TRADES   = "rest.silver.trades_spark"
GOLD            = "rest.gold.open_positions_spark"


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
        spark.sql(f"""
            MERGE INTO {GOLD} AS t
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

    attach_latency_listener(spark, "iceberg-gold")
    trades = observe_event_time(trades, sample=SAMPLE_ACCOUNTS)

    (trades.writeStream.foreachBatch(fold_to_book)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime="15 seconds")
        .start().awaitTermination())


if __name__ == "__main__":
    main()
