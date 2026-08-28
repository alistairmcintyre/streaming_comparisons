"""End-to-end SCD2 against a REAL Delta table, across MICRO-BATCHES.

tests/scd2_spark_test.py covers the staging logic with an empty `current` — i.e. versions
arriving in one batch. This covers the path that actually happens: version N lands in one
micro-batch, N+1 in a later one, and the close-out has to find N by READING THE TABLE.

It also exercises what a unit test cannot: the MERGE's column list against the real
schema, and whether foreachBatch's wiring produces a table you can read back.
"""
import sys, datetime as dt
sys.path.insert(0, "jobs/_shared")
from pyspark.sql import SparkSession
from pyspark.sql.types import (StructType, StructField, LongType, StringType,
                               TimestampType, BooleanType)
from pyspark.sql.functions import col
from scd2 import stage_scd2

D = lambda d: dt.datetime(2026, 1, d)
T = "/tmp/scd2_delta_probe"
spark = (SparkSession.builder.appName("scd2-delta").master("local[2]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.shuffle.partitions", "2").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

spark.sql(f"""CREATE OR REPLACE TABLE delta.`{T}` (
    account_id BIGINT, name STRING, country STRING, tier STRING,
    source_updated_at TIMESTAMP, event_ts TIMESTAMP,
    effective_from TIMESTAMP, effective_to TIMESTAMP, is_current BOOLEAN,
    source_lsn BIGINT, op STRING, commit_ts TIMESTAMP) USING delta""")

bschema = StructType([
    StructField("account_id", LongType()), StructField("name", StringType()),
    StructField("country", StringType()), StructField("tier", StringType()),
    StructField("source_updated_at", TimestampType()), StructField("event_ts", TimestampType()),
    StructField("effective_from", TimestampType()), StructField("source_lsn", LongType()),
    StructField("op", StringType()), StructField("commit_ts", TimestampType())])

ATTRS = ["name", "country", "tier", "source_updated_at", "event_ts", "op", "commit_ts"]

def run_batch(rows):
    """The same body as jobs/spark-delta/silver_accounts.py:upsert_scd2."""
    batch = spark.createDataFrame(rows, bschema)
    ids = [r[0] for r in batch.select("account_id").distinct().collect()]
    current = (spark.read.format("delta").load(T)
               .filter(col("is_current") & col("account_id").isin(ids))
               .select("account_id", "source_lsn", "effective_from"))
    stage_scd2(batch, current, attrs=ATTRS).createOrReplaceTempView("_scd2")
    spark.sql(f"""
        MERGE INTO delta.`{T}` AS t USING _scd2 AS s
        ON t.account_id = s.account_id AND t.source_lsn = s.source_lsn
        WHEN MATCHED AND s.action = 'close' THEN UPDATE SET
            t.effective_to = s.effective_to, t.is_current = false
        WHEN NOT MATCHED AND s.action = 'new' THEN INSERT
            (account_id, name, country, tier, source_updated_at, event_ts,
             effective_from, effective_to, is_current, source_lsn, op, commit_ts)
            VALUES (s.account_id, s.name, s.country, s.tier, s.source_updated_at,
                    s.event_ts, s.effective_from, s.effective_to, s.is_current,
                    s.source_lsn, s.op, s.commit_ts)""")

# batch 1 — day 1
run_batch([(1, "acme", "GB", "A", D(1), D(1), D(1), 100, "c", D(1)),
           (2, "beta", "US", "X", D(3), D(3), D(3), 150, "c", D(3))])
# batch 2 — day 5, a LATER micro-batch: the close-out must find v100 in the TABLE
run_batch([(1, "acme", "GB", "B", D(5), D(5), D(5), 200, "u", D(5))])
# batch 3 — an at-least-once re-delivery of batch 2
run_batch([(1, "acme", "GB", "B", D(5), D(5), D(5), 200, "u", D(5))])

out = sorted(spark.read.format("delta").load(T).collect(),
             key=lambda r: (r["account_id"], r["source_lsn"]))
print("  table after 3 micro-batches:")
for r in out:
    print(f"      acct={r['account_id']} lsn={r['source_lsn']} tier={r['tier']} "
          f"from={r['effective_from'].date()} to={r['effective_to'].date() if r['effective_to'] else None} "
          f"current={r['is_current']}")

fails = []
def chk(d, ok):
    print(("  PASS  " if ok else "  FAIL  ") + d)
    if not ok: fails.append(d)

g = {(r["account_id"], r["source_lsn"]): r for r in out}
chk("v100 closed ACROSS BATCHES at day 5", (1,100) in g and g[(1,100)]["effective_to"] == D(5)
                                            and g[(1,100)]["is_current"] is False)
chk("v200 is current",  (1,200) in g and g[(1,200)]["effective_to"] is None and g[(1,200)]["is_current"] is True)
chk("v100 attributes preserved by the close (tier still A)", (1,100) in g and g[(1,100)]["tier"] == "A")
chk("acct2 untouched",  (2,150) in g and g[(2,150)]["is_current"] is True)
chk("re-delivery did not duplicate", len(out) == 3)
for a in (1, 2):
    chk(f"acct{a} has exactly one current row",
        len([r for r in out if r["account_id"] == a and r["is_current"]]) == 1)

spark.stop()
print("\nSCD2 Delta MERGE is correct" if not fails else "\nSCD2 Delta MERGE is WRONG: " + "; ".join(fails))
sys.exit(1 if fails else 0)
