"""
Spark Structured Streaming: Hudi silver.trades → Hudi gold.open_positions

Folds signed fills into a net book per (account_id, symbol). Work is proportional to
the CHANGES in each micro-batch, never a rescan of silver — the project's first rule
(STREAMING_DESIGN_PRINCIPLES.md).

Hudi has no arithmetic MERGE like Delta/Iceberg (`SET x = x + s.dx`), so the fold is
done explicitly: aggregate the batch, read back ONLY the affected keys, add, upsert.
Partitioning gold by `symbol` keeps that read-back a partition prune rather than a
full scan. Like the Delta/Iceberg golds this incrementing fold is at-least-once — a
replayed batch double-counts; exactly-once would need a txn-id guard per engine.
"""
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, sum as _sum, count as _count, coalesce, current_timestamp, broadcast,
)
from hudi_tables import SILVER_TRADES, SILVER_ACCOUNTS, GOLD_POSITIONS, gold_positions_opts

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
                   _count(lit(1)).alias("dcnt")))
    if not deltas.take(1):
        return

    # Read back ONLY the symbols this batch touched (partition prune, not a scan).
    syms = [r["symbol"] for r in deltas.select("symbol").distinct().collect()]
    try:
        cur = (spark.read.format("hudi").load(GOLD_POSITIONS)
               .filter(col("symbol").isin(syms))
               .select("account_id", "symbol", "net_quantity", "net_notional", "trade_count"))
    except Exception:                      # first batch: table does not exist yet
        cur = None

    j = deltas if cur is None else deltas.join(cur, ["account_id", "symbol"], "left")
    if cur is None:
        j = (j.withColumn("net_quantity", col("dq"))
              .withColumn("net_notional", col("dnot"))
              .withColumn("trade_count", col("dcnt")))
    else:
        j = (j.withColumn("net_quantity", coalesce(col("net_quantity"), lit(0)) + col("dq"))
              .withColumn("net_notional", coalesce(col("net_notional"), lit(0)) + col("dnot"))
              .withColumn("trade_count", coalesce(col("trade_count"), lit(0)) + col("dcnt")))

    # Enrich with the account dimension, as the other engines' golds do.
    try:
        accts = (spark.read.format("hudi").load(SILVER_ACCOUNTS)
                 .select("account_id", "country", "tier"))
        j = j.join(broadcast(accts), "account_id", "left")
    except Exception:
        j = j.withColumn("country", lit(None).cast("string")) \
             .withColumn("tier", lit(None).cast("string"))

    out = (j.withColumn("status", when(col("net_quantity") == 0, lit("FLAT"))
                        .when(col("net_quantity") > 0, lit("LONG")).otherwise(lit("SHORT")))
            .withColumn("commit_ts", current_timestamp())
            .select("account_id", "symbol", "net_quantity", "net_notional",
                    "trade_count", "status", "country", "tier", "commit_ts"))

    (out.write.format("hudi").options(**gold_positions_opts())
        .mode("append").save(GOLD_POSITIONS))


def main():
    spark = SparkSession.builder.appName("gold-open-positions-hudi").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    trades = (spark.readStream.format("hudi").load(SILVER_TRADES)
              .select("account_id", "symbol", "side", "quantity", "price"))

    (trades.writeStream.foreachBatch(fold_to_book)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime="15 seconds")
        .start().awaitTermination())


if __name__ == "__main__":
    main()
