"""Smoke-test read-only check: print the Paimon silver current view + counts."""
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("verify-paimon").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

print("=== SILVER current view (expect 9 rows; customer 10 deleted; 3,4 -> SG) ===")
sv = spark.read.format("paimon").table("paimon.silver.customers")
sv.select("customer_id", "name", "country", "segment").orderBy("customer_id").show(50, False)
print(f"SILVER row count = {sv.count()}")

print("=== per-country counts (oracle: DE=2 FR=2 GB=2 SG=2 US=1) ===")
sv.groupBy("country").count().orderBy("country").show()

print("=== GOLD table (if the gold job has run) ===")
try:
    spark.read.format("paimon").table("paimon.gold.customers_per_country").orderBy("country").show()
except Exception as e:
    print(f"gold not present yet: {e}")
