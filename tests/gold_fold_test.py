"""Gold open-positions fold, against a REAL table, for one Spark engine.

    python3 tests/gold_fold_test.py delta|iceberg|hudi

Imports and calls the ENGINE'S OWN fold_to_book (not a copy of it) so what passes here
is the code that runs on the cluster. The three engines fold the same trades three
different ways (Delta MERGE with a txn stamp, Iceberg MERGE, Hudi read-modify-upsert),
and the point of the exercise is that they must agree to the last decimal.

The scenario is built so every incremental rule has to hold ACROSS micro-batches, which
is where a fold that only ever gets tested on one batch quietly breaks:

  acct 1 AAPL  buy 100@10 (day 5), buy 50@12 (day 3) | sell 150@11 (day 7) | buy 20@9 (day 1)
        -> nets to zero in batch 2 (must go CLOSED) and REOPENS in batch 3.
        -> the day-1 fill arrives LAST and is the EARLIEST: opened_at must move BACK to
           day 1, while last_updated_at must not move back off day 7.
  acct 2 MSFT  buy 10@100 (day 4) | sell 4@105 (day 6)          -> stays OPEN
  acct 3 TSLA  buy 5@200  (day 2) | sell 5@210 (day 8)          -> ends CLOSED at zero

Anything the fold gets wrong, sign, accumulation, status transition, least/greatest
direction, or double-counting, changes one of the printed numbers.
"""
import os, sys, datetime as dt
from decimal import Decimal

ENGINE = sys.argv[1]
ROOT = f"/tmp/gold_probe_{ENGINE}"
os.environ["LATENCY_EMIT_ENABLED"] = "false"
os.environ["DELTA_WAREHOUSE"] = f"{ROOT}/delta"
os.environ["HUDI_WAREHOUSE"]  = f"{ROOT}/hudi"
# JOBS_ROOT lets the meta-check in tests/run-checks.sh point this at a deliberately
# broken copy of the jobs tree and confirm the assertions actually bite.
JOBS = os.environ.get("JOBS_ROOT", "jobs")
sys.path.insert(0, f"{JOBS}/_shared")
sys.path.insert(0, f"{JOBS}/spark-{ENGINE}")

from pyspark.sql import SparkSession
from pyspark.sql.types import (StructType, StructField, LongType, StringType, IntegerType,
                               DecimalType, TimestampType)

b = (SparkSession.builder.appName(f"gold-fold-{ENGINE}").master("local[2]")
     .config("spark.sql.shuffle.partitions", "2")
     .config("spark.sql.warehouse.dir", f"{ROOT}/_wh"))
if ENGINE == "delta":
    b = (b.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
          .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"))
elif ENGINE == "iceberg":
    # The catalog must be called `rest`: iceberg_tables reads ICEBERG_CATALOG but the job
    # modules hardcode "rest.gold.open_positions_spark", so `rest` is the only name where
    # the DDL and the job agree. Backed by a local hadoop warehouse here.
    b = (b.config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
          .config("spark.sql.catalog.rest", "org.apache.iceberg.spark.SparkCatalog")
          .config("spark.sql.catalog.rest.type", "hadoop")
          .config("spark.sql.catalog.rest.warehouse", f"{ROOT}/iceberg"))
else:
    b = (b.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
          .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog"))
spark = b.getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

import gold_open_positions as job

if ENGINE == "delta":
    from delta_tables import ensure_all; ensure_all(spark)
    READ = lambda: spark.read.format("delta").load(job.GOLD)
elif ENGINE == "iceberg":
    from iceberg_tables import ensure_all; ensure_all(spark)
    READ = lambda: spark.table(job.GOLD)
else:
    # Glue meta-sync needs a live catalog and has nothing to do with the fold under test.
    _orig = job.gold_positions_opts
    job.gold_positions_opts = lambda: {**_orig(),
                                       "hoodie.datasource.meta.sync.enable": "false",
                                       "hoodie.datasource.hive_sync.enable": "false"}
    READ = lambda: spark.read.format("hudi").load(job.GOLD_POSITIONS)

D = lambda d: dt.datetime(2026, 1, d)
P = lambda p: Decimal(p).quantize(Decimal("0.0001"))
SILVER = StructType([
    StructField("trade_id", LongType(), False), StructField("account_id", LongType()),
    StructField("symbol", StringType()), StructField("side", StringType()),
    StructField("quantity", IntegerType()), StructField("price", DecimalType(12, 4)),
    StructField("executed_at", TimestampType()), StructField("event_ts", TimestampType()),
    StructField("ingest_ts", TimestampType())])

BATCHES = [
    [(1, 1, "AAPL", "BUY",  100, P("10"),  D(5)),
     (2, 1, "AAPL", "BUY",   50, P("12"),  D(3)),
     (3, 2, "MSFT", "BUY",   10, P("100"), D(4)),
     (4, 3, "TSLA", "BUY",    5, P("200"), D(2))],
    [(5, 1, "AAPL", "SELL", 150, P("11"),  D(7)),
     (6, 2, "MSFT", "SELL",   4, P("105"), D(6)),
     (7, 3, "TSLA", "SELL",   5, P("210"), D(8))],
    [(8, 1, "AAPL", "BUY",   20, P("9"),   D(1))],
]
for i, rows in enumerate(BATCHES):
    job.fold_to_book(spark.createDataFrame([r + (r[6], r[6]) for r in rows], SILVER), i)

# FIELD PARITY, measured on the table this engine actually built. Hudi has no DDL, its
# schema is whatever DataFrame was written, so this is the only way to catch it drifting.
# It had: net_notional came out decimal(33,4) against the decimal(38,4) the other four
# declare, and every value-based check passed regardless.
from schemas import (GOLD_OPEN_POSITIONS, HIVE_PARTITION_COLUMNS, HUDI_META_PREFIX,
                     hive_order)
_fields = READ().schema.fields
_business = [(f.name, f.dataType.simpleString()) for f in _fields
             if not f.name.startswith(HUDI_META_PREFIX)]
_extra = [f.name for f in _fields if f.name.startswith(HUDI_META_PREFIX)]

book = sorted(READ().collect(), key=lambda r: (r["account_id"], r["symbol"]))
lines = [f"{r['account_id']}|{r['symbol']}|{r['net_quantity']}|{float(r['net_notional']):.4f}"
         f"|{r['trade_count']}|{r['status']}|{r['opened_at']:%Y-%m-%d}|{r['last_updated_at']:%Y-%m-%d}"
         for r in book]
print(f"  book ({ENGINE}), acct|symbol|net_qty|net_notional|trades|status|opened|updated")
for l in lines:
    print("      " + l)
    print("BOOK " + l)          # machine-readable; tests/gold-consistency.sh diffs these

EXPECT = [
    "1|AAPL|20|130.0000|4|OPEN|2026-01-01|2026-01-07",
    "2|MSFT|6|580.0000|2|OPEN|2026-01-04|2026-01-06",
    "3|TSLA|0|-50.0000|2|CLOSED|2026-01-02|2026-01-08",
]
fails = []
def chk(d, ok):
    print(("  PASS  " if ok else "  FAIL  ") + d)
    if not ok: fails.append(d)

# Hudi writes the Hive-partitioned layout: same fields, partition columns LAST. Anything
# else and Athena reads every later column one place out (see hive_order in schemas.py).
_expect = (hive_order(GOLD_OPEN_POSITIONS, HIVE_PARTITION_COLUMNS["GOLD_OPEN_POSITIONS"])
           if ENGINE == "hudi" else GOLD_OPEN_POSITIONS)
chk(f"gold returns the canonical {len(_expect)} fields, in order, with the same types"
    + (" (partition column last, Hive layout)" if ENGINE == "hudi" else ""),
    _business == _expect)
if _business != _expect:
    _want = dict(_expect); _got = dict(_business)
    for _n, _t in _expect:
        if _got.get(_n) != _t:
            print(f"          {_n}: got {_got.get(_n, '<missing>')}  want {_t}")
    for _n, _t in _business:
        if _n not in _want:
            print(f"          {_n}: EXTRA column ({_t})")
# Hudi carries _hoodie_* metadata on every table; anything ELSE extra is real drift.
chk("no non-metadata extra columns", all(e.startswith(HUDI_META_PREFIX) for e in _extra))
chk("exactly one row per (account, symbol)", len(lines) == 3)
for want in EXPECT:
    a, s = want.split("|")[0], want.split("|")[1]
    got = next((l for l in lines if l.startswith(f"{a}|{s}|")), None)
    chk(f"{a}/{s} folds to {want.split('|',2)[2]}", got == want)
    if got and got != want:
        print(f"          got  {got}")
        print(f"          want {want}")
spark.stop()
print(f"\ngold fold ({ENGINE}) is correct" if not fails
      else f"\ngold fold ({ENGINE}) is WRONG: " + "; ".join(fails))
sys.exit(1 if fails else 0)
