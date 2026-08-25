"""Check the flink-iceberg GOLD tables — i.e. that gold stream-read silver."""
import sys
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("integration-test-gold-flink").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

EXPECTED = {"DE": 2, "FR": 2, "GB": 2, "SG": 2, "US": 1}
ok = True

for t in ["rest.gold.customers_per_country_flink",
          "rest.gold.customers_per_country_flink_direct"]:
    print(f"=== {t} ===")
    try:
        df = spark.table(t)
        counts = {r["country"]: r["customer_count"] for r in df.collect()}
        df.orderBy("country").show()
        match = counts == EXPECTED
        print(f"counts   = {dict(sorted(counts.items()))}")
        print(f"expected = {dict(sorted(EXPECTED.items()))}  -> {'OK' if match else 'MISMATCH'}")
        ok = ok and match
    except Exception as e:
        print(f"NOT POPULATED (gold did not stream-read silver): {str(e)[:150]}")
        ok = False

print("GOLD PASS" if ok else "GOLD FAIL")
sys.exit(0 if ok else 1)
