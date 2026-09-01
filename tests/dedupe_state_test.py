"""silver.trades dedupe, does it actually survive a restart, on RocksDB state?

This is the only place a re-delivered trade_id can be removed on Delta and Iceberg
(Paimon/Fluss dedupe structurally via a first-row PK table, Hudi via upsert on trade_id),
and gold cannot repair a duplicate: its fold is `+=` over (account_id, symbol) and never
sees trade_id at all. So this operator is worth testing rather than assuming.

Two things are checked, and the second is the one that matters:

  1. a re-delivery inside the watermark collapses to one row;
  2. it still collapses when the re-delivery arrives in a LATER QUERY RUN, i.e. the
     dedupe state was checkpointed and restored, not merely held in memory for the life
     of one query. A state store that quietly failed to persist would pass a
     single-run test and duplicate every trade across a restart in production.

Also asserts the RocksDB state store is genuinely in use: the manifests set it so the
2h window's state lives on disk rather than in executor heap, and a config that silently
did nothing would leave that state on-heap where it was.
"""
import os, sys, shutil, datetime as dt
from pyspark.sql import SparkSession
from pyspark.sql.types import (StructType, StructField, LongType, StringType, IntegerType,
                               DecimalType, TimestampType)
from decimal import Decimal

ROOT = "/tmp/dedupe_probe"
PROVIDER = "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider"
shutil.rmtree(ROOT, ignore_errors=True)

spark = (SparkSession.builder.appName("dedupe-state").master("local[2]")
         .config("spark.sql.streaming.stateStore.providerClass", PROVIDER)
         .config("spark.sql.shuffle.partitions", "2").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

D = lambda d, h=0: dt.datetime(2026, 1, d, h)
SCHEMA = StructType([
    StructField("trade_id", LongType()), StructField("account_id", LongType()),
    StructField("symbol", StringType()), StructField("side", StringType()),
    StructField("quantity", IntegerType()), StructField("price", DecimalType(12, 4)),
    StructField("executed_at", TimestampType()), StructField("event_ts", TimestampType()),
    StructField("ingest_ts", TimestampType())])

def land(name, rows):
    """Land a file in bronze, as Debezium->bronze would."""
    (spark.createDataFrame([(t, 1, "AAPL", "BUY", 10, Decimal("10.0000"), e, e, e)
                            for t, e in rows], SCHEMA)
     .write.mode("append").parquet(f"{ROOT}/bronze"))

def drain():
    """One run of the REAL silver dedupe, to completion, then stop, as a restart would."""
    src = spark.readStream.schema(SCHEMA).parquet(f"{ROOT}/bronze")
    # NO_DEDUPE drops the operator so tests/run-checks.sh can confirm these assertions
    # actually fail when the dedupe is gone, rather than passing for some other reason.
    if not os.environ.get("NO_DEDUPE"):
        src = (src.withWatermark("event_ts", "2 hours")
                  .dropDuplicatesWithinWatermark(["trade_id"]))
    q = (src.writeStream.format("parquet").outputMode("append")
         .option("path", f"{ROOT}/silver")
         .option("checkpointLocation", f"{ROOT}/_chk")
         .trigger(availableNow=True).start())
    q.awaitTermination()
    return q.lastProgress

land("b1", [(1, D(1, 0)), (2, D(1, 0)), (3, D(1, 1))])
p1 = drain()
# A re-delivery of trade 3 (same id, same event time) landing in a LATER run.
land("b2", [(3, D(1, 1)), (4, D(1, 2))])
p2 = drain()

rows = sorted(spark.read.parquet(f"{ROOT}/silver").collect(), key=lambda r: r["trade_id"])
ids = [r["trade_id"] for r in rows]
print(f"  silver trade_ids after two runs: {ids}")

metrics = {}
for p in (p1, p2):
    for op in (p or {}).get("stateOperators", []):
        metrics.update(op.get("customMetrics", {}))
rocks = sorted(k for k in metrics if k.lower().startswith("rocksdb"))

fails = []
def chk(d, ok):
    print(("  PASS  " if ok else "  FAIL  ") + d)
    if not ok: fails.append(d)

chk("re-delivery across a RESTART collapsed (state was checkpointed)", ids == [1, 2, 3, 4])
chk("no trade_id appears twice", len(ids) == len(set(ids)))
chk(f"RocksDB state store actually in use ({len(rocks)} rocksdb metrics)", bool(rocks))
if rocks:
    print("      e.g. " + ", ".join(rocks[:4]))
spark.stop()
print("\nsilver dedupe holds across restart on RocksDB state" if not fails
      else "\nsilver dedupe is WRONG: " + "; ".join(fails))
sys.exit(1 if fails else 0)
