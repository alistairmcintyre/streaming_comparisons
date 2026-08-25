"""
Integration-test assertion for the flink-iceberg dual-silver pipeline.

Checks the two silver approaches produce their expected delete semantics:
  - non-direct (customers_flink): SOFT delete → 10 rows, customer 10 present
    with country=NULL (tombstone).
  - direct (customers_flink_direct): HARD delete → 9 rows, customer 10 gone.
Exits 0 (PASS) or 1 (FAIL). Run inside the spark (iceberg) image.
"""
import sys
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("integration-test-flink-iceberg").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

ok = True

print("=== non-direct silver (SOFT delete: expect 10 rows, customer 10 country=NULL) ===")
nd = spark.table("rest.silver.customers_flink")
nd_rows = nd.count()
nd_null = nd.filter("country is null").count()
nd.select("customer_id", "country", "segment").orderBy("customer_id").show(50, False)
print(f"rows={nd_rows} (expect 10), null-country tombstones={nd_null} (expect >=1)")
ok = ok and nd_rows == 10 and nd_null >= 1

print("=== direct silver (HARD delete: expect 9 rows, no customer 10) ===")
d = spark.table("rest.silver.customers_flink_direct")
d_rows = d.count()
d_has10 = d.filter("customer_id = 10").count()
d.select("customer_id", "country", "segment").orderBy("customer_id").show(50, False)
print(f"rows={d_rows} (expect 9), customer 10 present={d_has10} (expect 0)")
ok = ok and d_rows == 9 and d_has10 == 0

print("PASS" if ok else "FAIL")
sys.exit(0 if ok else 1)
