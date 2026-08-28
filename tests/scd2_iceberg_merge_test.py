"""SCD2 against a REAL Iceberg table, across micro-batches.

Same scenario as the Delta test. Iceberg's MERGE is a different code path — different
planner, different row-level-operation machinery (this table is MOR), and it needs a
catalog rather than a path — so "it works on Delta" proves nothing here.
"""
import sys, datetime as dt
sys.path.insert(0, "jobs/_shared")
from pyspark.sql import SparkSession
from pyspark.sql.types import (StructType, StructField, LongType, StringType, TimestampType)
from pyspark.sql.functions import col
from scd2 import stage_scd2

D = lambda d: dt.datetime(2026, 1, d)
TBL = "local.silver.accounts"
spark = (SparkSession.builder.appName("scd2-iceberg").master("local[2]")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local.type", "hadoop")
    .config("spark.sql.catalog.local.warehouse", "/tmp/iceberg_scd2_probe")
    .config("spark.sql.shuffle.partitions", "2").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

spark.sql("CREATE NAMESPACE IF NOT EXISTS local.silver")
spark.sql(f"DROP TABLE IF EXISTS {TBL}")
spark.sql(f"""CREATE TABLE {TBL} (
    account_id BIGINT, name STRING, country STRING, tier STRING,
    source_updated_at TIMESTAMP, event_ts TIMESTAMP,
    effective_from TIMESTAMP, effective_to TIMESTAMP, is_current BOOLEAN,
    source_lsn BIGINT, op STRING, commit_ts TIMESTAMP)
  USING iceberg TBLPROPERTIES (
    'format-version'='2','write.merge.mode'='merge-on-read','write.update.mode'='merge-on-read')""")

bschema = StructType([
    StructField("account_id", LongType()), StructField("name", StringType()),
    StructField("country", StringType()), StructField("tier", StringType()),
    StructField("source_updated_at", TimestampType()), StructField("event_ts", TimestampType()),
    StructField("effective_from", TimestampType()), StructField("source_lsn", LongType()),
    StructField("op", StringType()), StructField("commit_ts", TimestampType())])
ATTRS = ["name", "country", "tier", "source_updated_at", "event_ts", "op", "commit_ts"]

def run_batch(rows):
    batch = spark.createDataFrame(rows, bschema)
    ids = [r[0] for r in batch.select("account_id").distinct().collect()]
    current = (spark.table(TBL).filter(col("is_current") & col("account_id").isin(ids))
               .select("account_id", "source_lsn", "effective_from", *ATTRS))
    stage_scd2(batch, current, attrs=ATTRS).createOrReplaceTempView("_scd2")
    spark.sql(f"""
        MERGE INTO {TBL} AS t USING _scd2 AS s
        ON t.account_id = s.account_id AND t.source_lsn = s.source_lsn
        WHEN MATCHED AND s.action = 'close' THEN UPDATE SET
            t.effective_to = s.effective_to, t.is_current = false
        WHEN NOT MATCHED AND s.action = 'new' THEN INSERT
            (account_id, name, country, tier, source_updated_at, event_ts,
             effective_from, effective_to, is_current, source_lsn, op, commit_ts)
            VALUES (s.account_id, s.name, s.country, s.tier, s.source_updated_at,
                    s.event_ts, s.effective_from, s.effective_to, s.is_current,
                    s.source_lsn, s.op, s.commit_ts)""")

run_batch([(1, "acme", "GB", "A", D(1), D(1), D(1), 100, "c", D(1)),
           (2, "beta", "US", "X", D(3), D(3), D(3), 150, "c", D(3))])
run_batch([(1, "acme", "GB", "B", D(5), D(5), D(5), 200, "u", D(5))])
run_batch([(1, "acme", "GB", "B", D(5), D(5), D(5), 200, "u", D(5))])   # re-delivery


# batch 4 — TWO versions of one account in a SINGLE micro-batch. The earlier one is
# already historical on arrival: it must never be written as current, and the pair
# must not stage the same key twice (a MERGE tolerates that; a Hudi upsert does not).
run_batch([(3, "gamma", "IE", "P", D(2), D(2), D(2), 10, "c", D(2)),
           (3, "gamma", "IE", "Q", D(6), D(6), D(6), 20, "u", D(6))])

out = sorted(spark.table(TBL).collect(), key=lambda r: (r["account_id"], r["source_lsn"]))
print("  table after 4 micro-batches:")
for r in out:
    print(f"      acct={r['account_id']} lsn={r['source_lsn']} tier={r['tier']} "
          f"to={r['effective_to'].date() if r['effective_to'] else None} current={r['is_current']}")
fails = []
def chk(d, ok):
    print(("  PASS  " if ok else "  FAIL  ") + d)
    if not ok: fails.append(d)
g = {(r["account_id"], r["source_lsn"]): r for r in out}
chk("v100 closed across batches", (1,100) in g and g[(1,100)]["effective_to"] == D(5) and g[(1,100)]["is_current"] is False)
chk("v200 current", (1,200) in g and g[(1,200)]["is_current"] is True)
chk("v100 attributes preserved", (1,100) in g and g[(1,100)]["tier"] == "A")
chk("re-delivery did not duplicate", len(out) == 5)
chk("in-batch v10 closed at v20's start, attributes intact",
    (3,10) in g and g[(3,10)]["effective_to"] == D(6)
    and not g[(3,10)]["is_current"] and g[(3,10)]["tier"] == "P")
chk("in-batch v20 is current", (3,20) in g and g[(3,20)]["is_current"])

for a in (1, 2, 3):
    chk(f"acct{a} exactly one current row", len([r for r in out if r["account_id"]==a and r["is_current"]])==1)
spark.stop()
print("\nSCD2 Iceberg MERGE is correct" if not fails else "\nSCD2 Iceberg MERGE is WRONG: " + "; ".join(fails))
sys.exit(1 if fails else 0)
