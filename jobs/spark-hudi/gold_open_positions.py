"""
Spark Structured Streaming: Hudi silver.trades → Hudi gold.open_positions

Folds signed fills into a net book per (account_id, symbol). Work is proportional to
the CHANGES in each micro-batch, never a rescan of silver — the project's first rule
(STREAMING_DESIGN_PRINCIPLES.md).

Hudi has no arithmetic MERGE like Delta/Iceberg (`SET x = x + s.dx`), so the fold is
done explicitly: aggregate the batch, read back ONLY the affected keys, add, upsert.
Partitioning gold by `symbol` keeps that read-back a partition prune rather than a
full scan.

Enrichment is NOT denormalised into gold. country/tier are account attributes with no
defensible temporal semantic on a current-state position row, so they come from a read-
time LEFT JOIN to silver.accounts instead:

    SELECT p.*, a.country, a.tier
    FROM gold.open_positions_hudi_rt p
    LEFT JOIN silver.accounts_hudi a USING (account_id)

LEFT, always: trades and accounts are independent CDC streams, so a fill can land before
its account row. An inner join would silently drop that position from the book.

At-least-once, like the Iceberg gold: Hudi has no idempotent-write primitive, so a
replayed micro-batch double-counts and the drift is permanent. (Delta's gold IS
exactly-once — it stamps each MERGE with txnAppId/txnVersion.) The drift is DETECTED,
not repaired: after the load is drained the snapshot checks
sum(gold.trade_count) == count(silver.trades) per engine and publishes the difference
in invariants.csv. Repairing it would add write amplification to the engine that
drifted and contaminate the comparison — see that script's header.
"""
import os
from pyspark.sql import SparkSession
from latency import observe_event_time, attach_latency_listener
from pyspark.sql.functions import (
    col, when, lit, sum as _sum, count as _count, coalesce, current_timestamp,
    min as _min, max as _max, least, greatest,
)
from hudi_tables import SILVER_TRADES, GOLD_POSITIONS, gold_positions_opts
from gold_schema import conform

CHECKPOINT_BASE = os.environ.get("CHECKPOINT_BASE", "s3a://warehouse/_chk")
CHECKPOINT_PATH = f"{CHECKPOINT_BASE}/gold_open_positions_hudi"


def fold_to_book(batch_df, batch_id):
    spark = batch_df.sparkSession
    signed_qty = when(col("side") == "BUY", col("quantity")).otherwise(-col("quantity"))
    signed_not = when(col("side") == "BUY", col("quantity") * col("price")) \
                 .otherwise(-(col("quantity") * col("price")))

    deltas = (batch_df.groupBy("account_id", "symbol")
              .agg(_sum(signed_qty).alias("dq"),
                   _sum(signed_not).alias("dnot"),
                   _count(lit(1)).alias("dcnt"),
                   # EVENT time carried through the fold: opened_at = MIN over the
                   # position's history, last_updated_at = MAX. Both fold via
                   # least/greatest below, so this stays proportional to the batch.
                   _min("executed_at").alias("dmin"),
                   _max("executed_at").alias("dmax")))
    # Cached: this job takes three actions on `deltas` (the emptiness probe, the
    # affected-symbol collect, and the write). Uncached, each one re-scans the batch,
    # so Hudi would read its silver three times per micro-batch where Delta/Iceberg
    # read once — a measurement artefact, not a Hudi property.
    deltas = deltas.cache()
    try:
        if not deltas.take(1):
            return

        # Read back ONLY the symbols this batch touched (partition prune, not a scan).
        syms = [r["symbol"] for r in deltas.select("symbol").distinct().collect()]
        try:
            cur = (spark.read.format("hudi").load(GOLD_POSITIONS)
                   .filter(col("symbol").isin(syms))
                   .select("account_id", "symbol", "net_quantity", "net_notional", "trade_count",
                           "opened_at", "last_updated_at"))
        except Exception:                      # first batch: table does not exist yet
            cur = None

        j = deltas if cur is None else deltas.join(cur, ["account_id", "symbol"], "left")
        if cur is None:
            j = (j.withColumn("net_quantity", col("dq"))
                  .withColumn("net_notional", col("dnot"))
                  .withColumn("trade_count", col("dcnt"))
                  .withColumn("opened_at", col("dmin"))
                  .withColumn("last_updated_at", col("dmax")))
        else:
            # least/greatest skip NULLs, so a key absent from the read-back (or a row
            # written before these columns existed) adopts the batch value.
            j = (j.withColumn("net_quantity", coalesce(col("net_quantity"), lit(0)) + col("dq"))
                  .withColumn("net_notional", coalesce(col("net_notional"), lit(0)) + col("dnot"))
                  .withColumn("trade_count", coalesce(col("trade_count"), lit(0)) + col("dcnt"))
                  .withColumn("opened_at", least(col("opened_at"), col("dmin")))
                  .withColumn("last_updated_at", greatest(col("last_updated_at"), col("dmax"))))

        # No dimension read here, deliberately. This previously did a FULL batch read
        # of silver.accounts on EVERY micro-batch and broadcast it — a rescan of a
        # silver table, O(dimension) not O(batch), to stamp a value with no defensible
        # temporal meaning. country/tier now come from a LEFT JOIN to silver.accounts
        # at query time. See the module docstring.

        # OPEN/CLOSED, matching the other four engines. This job previously wrote
        # LONG/SHORT/FLAT, which made `status` incomparable across engines.
        # conform(), not a bare select: Hudi has NO DDL, so the table's schema is whatever
        # DataFrame is written to it. Left to infer, net_notional came out decimal(33,4)
        # — what SUM(int * decimal(12,4)) widens to — against the decimal(38,4) the other
        # four engines DECLARE. Nothing failed; one engine of five just had a different
        # type for the column, invisible to any check that only compares values.
        out = (j.withColumn("status", when(col("net_quantity") != 0, lit("OPEN")).otherwise(lit("CLOSED")))
                .withColumn("commit_ts", current_timestamp()))
        out = conform(out)

        (out.write.format("hudi").options(**gold_positions_opts())
            .mode("append").save(GOLD_POSITIONS))
    finally:
        deltas.unpersist()

def main():
    spark = SparkSession.builder.appName("gold-open-positions-hudi").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # NO per-batch admission control. Delta caps its stream with maxFilesPerTrigger
    # and Iceberg with streaming-max-files-per-micro-batch; Hudi 1.2.0 exposes no
    # equivalent — there is no ReadLimit machinery and no hoodie.* option for it
    # (checked against hudi-spark4.0-bundle_2.13-1.2.0.jar). So after a stall this
    # source pulls the whole backlog in one micro-batch.
    # This is a real capability difference, not an oversight, and it makes Hudi's
    # batch sizes incomparable to the other engines' under recovery — recorded in
    # results.json rather than papered over. The KEEP_LATEST_BY_HOURS cleaner
    # retention in hudi_tables.py is what stops the worst case (losing the start
    # instant and silently falling back to a full-table scan).
    trades = (spark.readStream.format("hudi").load(SILVER_TRADES)
              .select("account_id", "symbol", "side", "quantity", "price", "executed_at"))

    attach_latency_listener(spark, "hudi-gold")
    trades = observe_event_time(trades)

    (trades.writeStream.foreachBatch(fold_to_book)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime="15 seconds")
        .start().awaitTermination())


if __name__ == "__main__":
    main()
