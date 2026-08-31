"""ensure_all() must not bump the Delta table version when nothing has changed.

ALTER TABLE ... SET TBLPROPERTIES writes a METADATA COMMIT even when every value is
already correct. All four Delta jobs call ensure_all() at startup and it covers all four
tables, so on the old code any job starting or restarting produced a metadata change on
tables the OTHER jobs were streaming from, and killed those streams:

    DELTA_METADATA_CHANGED: The metadata of the Delta table has been changed by a
    concurrent update.

Live consequence: delta-silver-trades failed repeatedly and finished 37 source versions
behind bronze — 2,240,000 rows against 4,086,000 — while its gold/silver invariant still
read `ok`, because that compares gold to silver rather than to bronze.

So the property under test is not "the properties end up right" (they always did) but
"a second call writes NOTHING".
"""
import os, sys
ROOT = "/tmp/delta_converge_probe"
os.environ["DELTA_WAREHOUSE"] = ROOT
sys.path.insert(0, "jobs/_shared")
from pyspark.sql import SparkSession
import delta_tables as D

spark = (SparkSession.builder.appName("delta-converge").master("local[2]")
         .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
         .config("spark.sql.catalog.spark_catalog",
                 "org.apache.spark.sql.delta.catalog.DeltaCatalog")
         .config("spark.sql.warehouse.dir", f"{ROOT}/_wh")
         .config("spark.sql.shuffle.partitions", "1").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

D.ensure_all(spark)
def version(rel):
    return (spark.sql(f"DESCRIBE HISTORY delta.`{ROOT}/{rel}`")
            .selectExpr("max(version)").collect()[0][0])

before = {rel: version(rel) for rel in D._TABLE_PATHS}
D.ensure_all(spark)                      # a second job starting up
after = {rel: version(rel) for rel in D._TABLE_PATHS}

fails = []
for rel in D._TABLE_PATHS:
    ok = before[rel] == after[rel]
    print(("  PASS  " if ok else "  FAIL  ")
          + f"{rel:24} version {before[rel]} -> {after[rel]}"
          + ("" if ok else "   <- a metadata commit a streaming reader would die on"))
    if not ok:
        fails.append(rel)

# and it must still CONVERGE when a property really has drifted
rel = D._TABLE_PATHS[0]
# Drift to a VALID alternate value — these are booleans, so 'false' is the only sane
# "wrong" setting. An arbitrary string is rejected by Delta and tests nothing.
k, v = next(iter(D._TABLE_PROPERTIES.items()))
spark.sql(f"ALTER TABLE delta.`{ROOT}/{rel}` SET TBLPROPERTIES ('{k}' = 'false')")
D.ensure_all(spark)
got = {r["key"]: r["value"] for r in
       spark.sql(f"SHOW TBLPROPERTIES delta.`{ROOT}/{rel}`").collect()}.get(k)
ok = got == v
print(("  PASS  " if ok else "  FAIL  ") + f"real drift is still corrected ({k} -> {got})")
if not ok:
    fails.append("drift")

spark.stop()
print("\nensure_all is a no-op when nothing changed" if not fails
      else "\nensure_all still writes metadata commits: " + ", ".join(fails))
sys.exit(1 if fails else 0)
