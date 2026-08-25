"""
Volume-test check for the Delta gold streaming-read of a MERGE-updated silver.

Prints silver's commit history (operation + rows updated/deleted) to prove the
gold stream WAS exposed to update/delete commits, then compares gold against a
batch recompute of silver's current view. If gold's stream crashed on the first
update commit, gold will be frozen/stale => MISMATCH.

Exit 0 = gold kept up (matches silver); 1 = mismatch/unreadable (stream stalled
or crashed). Run inside the spark-delta image.
"""
import sys
from pyspark.sql import SparkSession

SILVER = "s3a://warehouse/delta/silver/customers"
GOLD = "s3a://warehouse/delta/gold/customers_per_country"

spark = SparkSession.builder.appName("volume-check-delta").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

print("=== silver commit history (did it emit UPDATE/DELETE commits?) ===")
try:
    hist = spark.sql(f"DESCRIBE HISTORY delta.`{SILVER}`")
    hist.selectExpr(
        "version", "operation",
        "operationMetrics['numTargetRowsInserted'] AS ins",
        "operationMetrics['numTargetRowsUpdated']  AS upd",
        "operationMetrics['numTargetRowsDeleted']  AS del",
    ).orderBy("version").show(80, False)
    m = hist.selectExpr(
        "sum(cast(operationMetrics['numTargetRowsUpdated'] as long)) u",
        "sum(cast(operationMetrics['numTargetRowsDeleted'] as long)) d",
    ).collect()[0]
    print(f"silver totals: rows updated={m['u']}, deleted={m['d']} "
          f"(both > 0 ⇒ the gold stream was exposed to update/delete commits)")
except Exception as e:
    print("silver history err:", str(e)[:200])

print("=== correctness: gold vs batch recompute of silver current view ===")
sv = spark.read.format("delta").load(SILVER)
expected = {r["country"]: r["c"] for r in
            sv.groupBy("country").count().withColumnRenamed("count", "c").collect()
            if r["country"] is not None}
try:
    g = spark.read.format("delta").load(GOLD)
    actual = {r["country"]: r["customer_count"] for r in g.collect()}
except Exception as e:
    print("gold read err:", str(e)[:200])
    print("VOLUME FAIL — gold unreadable")
    sys.exit(1)

match = expected == actual
print("silver batch-recompute:", dict(sorted(expected.items())))
print("gold                  :", dict(sorted(actual.items())))
if match:
    print("MATCH — gold kept up with silver ✅")
else:
    print("MISMATCH — gold is stale/behind (stream stalled or crashed on an update commit)")
sys.exit(0 if match else 1)
