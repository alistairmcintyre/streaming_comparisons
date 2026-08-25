"""
Integration-test assertion for the Spark→Hudi customers pipeline (bronze→silver→gold).

Reads the path-addressed Hudi tables (snapshot query) and checks the fixed oracle:
  silver.customers            = 9 rows (customer 10 GDPR-deleted) — current view per id
  gold.customers_per_country  = DE=2 FR=2 GB=2 SG=2 US=1

Gold streams from silver via a Hudi incremental query. Exit 0 = PASS.
Run inside the spark-hudi image.
"""
import sys
from pyspark.sql import SparkSession

EXPECTED = {"DE": 2, "FR": 2, "GB": 2, "SG": 2, "US": 1}
EXPECTED_ROWS = 9
SILVER = "s3a://warehouse/hudi/silver/customers"
GOLD = "s3a://warehouse/hudi/gold/customers_per_country"

spark = SparkSession.builder.appName("integration-test-hudi").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
ok = True

print("=== hudi silver.customers (current view per customer_id) ===")
try:
    sv = spark.read.format("hudi").load(SILVER)
    rows = sv.count()
    sv.select("customer_id", "name", "country", "segment").orderBy("customer_id").show(50, False)
    counts = {r["country"]: r["c"] for r in
              sv.groupBy("country").count().withColumnRenamed("count", "c").collect()}
    print(f"silver rows = {rows} (expected {EXPECTED_ROWS}); per-country = {dict(sorted(counts.items()))}")
    ok = ok and rows == EXPECTED_ROWS and counts == EXPECTED
except Exception as e:
    print(f"SILVER NOT POPULATED: {str(e)[:200]}")
    ok = False

print("=== hudi gold.customers_per_country ===")
try:
    g = spark.read.format("hudi").load(GOLD)
    gcounts = {r["country"]: r["customer_count"] for r in g.collect()}
    g.select("country", "customer_count").orderBy("country").show()
    match = gcounts == EXPECTED
    print(f"gold counts = {dict(sorted(gcounts.items()))}  expected {dict(sorted(EXPECTED.items()))} -> {'OK' if match else 'MISMATCH'}")
    ok = ok and match
except Exception as e:
    print(f"GOLD NOT POPULATED: {str(e)[:200]}")
    ok = False

print("HUDI PASS" if ok else "HUDI FAIL")
sys.exit(0 if ok else 1)
