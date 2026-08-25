"""
Integration-test assertion for a Spark→Paimon customers pipeline.

Reads paimon.silver.customers and checks it matches the fixed integration-test oracle:
  9 customers (customer 10 deleted via GDPR), per-country DE=2 FR=2 GB=2 SG=2 US=1.
Exits 0 (PASS) or 1 (FAIL). Run inside the spark-paimon image.
"""
import sys
from pyspark.sql import SparkSession

EXPECTED_ROWS = 9
EXPECTED = {"DE": 2, "FR": 2, "GB": 2, "SG": 2, "US": 1}

spark = SparkSession.builder.appName("integration-test-paimon").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

sv = spark.read.format("paimon").table("paimon.silver.customers")
rows = sv.count()
counts = {r["country"]: r["c"] for r in
          sv.groupBy("country").count().withColumnRenamed("count", "c").collect()}

print("SILVER current view:")
sv.select("customer_id", "name", "country", "segment").orderBy("customer_id").show(50, False)
print(f"row count   = {rows}   (expected {EXPECTED_ROWS})")
print(f"per-country = {dict(sorted(counts.items()))}")
print(f"expected    = {dict(sorted(EXPECTED.items()))}")

ok = rows == EXPECTED_ROWS and counts == EXPECTED
print("PASS" if ok else "FAIL")
sys.exit(0 if ok else 1)
