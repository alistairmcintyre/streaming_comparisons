"""Hudi's tables must MATCH the canon, measured on real tables, not asserted.

Hudi is the one engine with no DDL: a Hudi table's schema is whatever DataFrame is written
to it. Every other engine DECLARES its schema and tests/schema_parity_test.py can read the
declaration; here the only truth is the table on disk.

That distinction has already cost one real bug, gold's net_notional inferred to
decimal(33,4) against the decimal(38,4) the other four declare, with every value-based
check passing throughout. gold is now covered by tests/gold_fold_test.py, which measures
the table it builds. This covers the other three: bronze.trades, silver.trades and
silver.accounts, each written through the same conform() call the job uses.

Glue meta-sync is forced off, it needs a live catalog and has nothing to do with schema.
"""
import os, sys, datetime as dt
from decimal import Decimal
ROOT = "/tmp/hudi_schema_probe"
# BEFORE importing hudi_tables: its table paths are module-level f-strings over _BASE,
# so patching the attribute afterwards leaves them pointing at s3a://warehouse.
os.environ["HUDI_WAREHOUSE"] = ROOT
sys.path.insert(0, "jobs/_shared")
from pyspark.sql import SparkSession
from pyspark.sql.types import (StructType, StructField, LongType, StringType, IntegerType,
                               DecimalType, TimestampType, BooleanType, DateType)
from pyspark.sql.functions import col
from schemas import (BRONZE_TRADES, SILVER_TRADES, SILVER_ACCOUNTS, HUDI_META_PREFIX,
                     PARTITION_ARTEFACTS, conform)
import hudi_tables

spark = (SparkSession.builder.appName("hudi-schema").master("local[2]")
         .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
         .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
         .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
         .config("spark.sql.shuffle.partitions", "2").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

NO_SYNC = {"hoodie.datasource.meta.sync.enable": "false",
           "hoodie.datasource.hive_sync.enable": "false"}
T = dt.datetime(2026, 1, 5)
P = Decimal("10.0000")

# One row per table, in the WIDEST plausible source shape, conform() is what narrows and
# casts it, exactly as in the job.
raw_trade = StructType([
    StructField("op", StringType()), StructField("trade_id", LongType()),
    StructField("account_id", LongType()), StructField("symbol", StringType()),
    StructField("side", StringType()), StructField("quantity", IntegerType()),
    StructField("price", DecimalType(12, 4)), StructField("executed_at", TimestampType()),
    StructField("event_ts", TimestampType()), StructField("ingest_ts", TimestampType()),
    StructField("kafka_offset", LongType()), StructField("kafka_partition", IntegerType()),
    StructField("source_lsn", LongType()), StructField("executed_date", DateType())])
trade_row = [("c", 1, 7, "AAPL", "BUY", 10, P, T, T, T, 0, 0, 100, T.date())]

raw_acct = StructType([
    StructField("account_id", LongType()), StructField("name", StringType()),
    StructField("country", StringType()), StructField("tier", StringType()),
    StructField("source_updated_at", TimestampType()), StructField("event_ts", TimestampType()),
    StructField("effective_from", TimestampType()), StructField("effective_to", TimestampType()),
    StructField("is_current", BooleanType()), StructField("source_lsn", LongType()),
    StructField("op", StringType()), StructField("commit_ts", TimestampType())])
acct_row = [(7, "acme", "GB", "A", T, T, T, None, True, 100, "c", T)]

CASES = [
    ("bronze.trades", BRONZE_TRADES, raw_trade, trade_row, ["executed_date"],
     hudi_tables.bronze_trades_opts, hudi_tables.BRONZE_TRADES),
    ("silver.trades", SILVER_TRADES, raw_trade, trade_row, ["executed_date"],
     hudi_tables.silver_trades_opts, hudi_tables.SILVER_TRADES),
    ("silver.accounts", SILVER_ACCOUNTS, raw_acct, acct_row, [],
     hudi_tables.silver_accounts_opts, hudi_tables.SILVER_ACCOUNTS),
]

fails = []
for name, canon, schema, rows, extra, opts, path in CASES:
    df = conform(spark.createDataFrame(rows, schema), canon, extra=extra)
    (df.write.format("hudi").options(**{**opts(), **NO_SYNC})
       .mode("overwrite").save(path))
    fields = spark.read.format("hudi").load(path).schema.fields
    got = [(f.name, f.dataType.simpleString()) for f in fields
           if not f.name.startswith(HUDI_META_PREFIX) and f.name not in PARTITION_ARTEFACTS]
    if got == canon:
        print(f"  PASS  hudi {name:16} {len(canon)} fields, in order, types match")
    else:
        print(f"  FAIL  hudi {name}")
        fails.append(name)
        want, have = dict(canon), dict(got)
        for n, t in canon:
            if have.get(n) != t:
                print(f"            {n}: got {have.get(n, '<missing>')}  want {t}")
        for n, t in got:
            if n not in want:
                print(f"            {n}: EXTRA ({t})")
        if [c for c, _ in got] != [c for c, _ in canon] and set(have) == set(want):
            print(f"            order differs: {[c for c,_ in got]}")

spark.stop()
print("\nhudi's written schemas match the canon" if not fails
      else "\nhudi SCHEMA DRIFT in: " + ", ".join(fails))
sys.exit(1 if fails else 0)
