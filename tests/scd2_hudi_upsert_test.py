"""SCD2 against a REAL Hudi table, across micro-batches.

Same scenario as the Delta/Iceberg tests, but Hudi has no MERGE here, the close-out
rides entirely on the composite record key (account_id, source_lsn) plus the precombine
field. That is a genuinely different mechanism, and the close row carries the same key
as the row it is closing, so whether it lands at all depends on Hudi's merge semantics.

Glue meta-sync is forced off: silver_accounts_opts() enables AwsGlueCatalogSyncTool,
which needs a live Glue catalog and has nothing to do with the write semantics under test.
"""
import sys, datetime as dt
sys.path.insert(0, "jobs/_shared")
from pyspark.sql import SparkSession
from pyspark.sql.types import (StructType, StructField, LongType, StringType, TimestampType)
from pyspark.sql.functions import col, current_timestamp
from scd2 import stage_scd2
from hudi_tables import silver_accounts_opts

D = lambda d: dt.datetime(2026, 1, d)
PATH = "/tmp/hudi_scd2_probe/silver_accounts"
OPTS = {**silver_accounts_opts(),
        "hoodie.datasource.meta.sync.enable": "false",
        "hoodie.datasource.hive_sync.enable": "false"}

spark = (SparkSession.builder.appName("scd2-hudi").master("local[2]")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
    .config("spark.sql.shuffle.partitions", "2").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

bschema = StructType([
    StructField("account_id", LongType()), StructField("name", StringType()),
    StructField("country", StringType()), StructField("tier", StringType()),
    StructField("source_updated_at", TimestampType()), StructField("event_ts", TimestampType()),
    StructField("effective_from", TimestampType()), StructField("source_lsn", LongType()),
    StructField("op", StringType())])
ATTRS = ["name", "country", "tier", "source_updated_at", "event_ts", "op"]

def run_batch(rows):
    batch = spark.createDataFrame(rows, bschema)
    ids = [r[0] for r in batch.select("account_id").distinct().collect()]
    try:
        current = (spark.read.format("hudi").load(PATH)
                   .filter(col("is_current") & col("account_id").isin(ids))
                   .select("account_id", "source_lsn", "effective_from", *ATTRS))
    except Exception:
        current = spark.createDataFrame(
            [], batch.select("account_id", "source_lsn", "effective_from", *ATTRS).schema)
    staged = stage_scd2(batch, current, attrs=ATTRS)
    (staged.drop("action").withColumn("commit_ts", current_timestamp())
        .write.format("hudi").options(**OPTS).mode("append").save(PATH))

run_batch([(1, "acme", "GB", "A", D(1), D(1), D(1), 100, "c"),
           (2, "beta", "US", "X", D(3), D(3), D(3), 150, "c")])
run_batch([(1, "acme", "GB", "B", D(5), D(5), D(5), 200, "u")])
run_batch([(1, "acme", "GB", "B", D(5), D(5), D(5), 200, "u")])   # re-delivery


# batch 4. Two versions of one account in a SINGLE micro-batch. The earlier one is
# already historical on arrival: it must never be written as current, and the pair
# must not stage the same key twice (a MERGE tolerates that; a Hudi upsert does not).
run_batch([(3, "gamma", "IE", "P", D(2), D(2), D(2), 10, "c"),
           (3, "gamma", "IE", "Q", D(6), D(6), D(6), 20, "u")])

out = sorted(spark.read.format("hudi").load(PATH)
             .select("account_id","source_lsn","tier","effective_from","effective_to","is_current")
             .collect(), key=lambda r: (r["account_id"], r["source_lsn"]))
print("  table after 4 micro-batches:")
for r in out:
    print(f"      acct={r['account_id']} lsn={r['source_lsn']} tier={r['tier']} "
          f"to={r['effective_to'].date() if r['effective_to'] else None} current={r['is_current']}")
fails = []
def chk(d, ok):
    print(("  PASS  " if ok else "  FAIL  ") + d)
    if not ok: fails.append(d)
g = {(r["account_id"], r["source_lsn"]): r for r in out}
chk("v100 closed across batches", (1,100) in g and g[(1,100)]["effective_to"] == D(5) and not g[(1,100)]["is_current"])
chk("v200 current", (1,200) in g and g[(1,200)]["is_current"])
chk("v100 attributes preserved", (1,100) in g and g[(1,100)]["tier"] == "A")
chk("re-delivery did not duplicate", len(out) == 5)
chk("in-batch v10 closed at v20's start, attributes intact",
    (3,10) in g and g[(3,10)]["effective_to"] == D(6)
    and not g[(3,10)]["is_current"] and g[(3,10)]["tier"] == "P")
chk("in-batch v20 is current", (3,20) in g and g[(3,20)]["is_current"])

for a in (1, 2, 3):
    chk(f"acct{a} exactly one current row", len([r for r in out if r["account_id"]==a and r["is_current"]])==1)
spark.stop()
print("\nSCD2 Hudi upsert is correct" if not fails else "\nSCD2 Hudi upsert is WRONG: " + "; ".join(fails))
sys.exit(1 if fails else 0)
