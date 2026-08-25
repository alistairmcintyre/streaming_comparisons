"""Smoke-test read-only check: print the flink-iceberg silver/gold tables."""
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("verify-iceberg").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

tables = [
    ("rest.silver.customers_flink",
     "NON-DIRECT silver (SOFT delete: expect 10 rows; customer 10 country=NULL tombstone)"),
    ("rest.silver.customers_flink_direct",
     "DIRECT silver (HARD delete: expect 9 rows; no customer 10)"),
    ("rest.gold.customers_per_country_flink",
     "gold non-direct (expect GB=2 etc; NULLs excluded)"),
    ("rest.gold.customers_per_country_flink_direct",
     "gold direct (expect GB=2 etc)"),
]

for tbl, desc in tables:
    print(f"=== {tbl}\n    {desc} ===")
    try:
        df = spark.table(tbl)
        df.orderBy(df.columns[0]).show(50, False)
        print(f"row count = {df.count()}")
    except Exception as e:
        print(f"not ready: {str(e)[:200]}")
