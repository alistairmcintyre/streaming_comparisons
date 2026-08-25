"""
Verify the flink-paimon pipeline — the key question: does GOLD stream-read SILVER?

Reads the Paimon tables Flink wrote (warehouse s3://warehouse/paimon, the same
catalog the spark-paimon image configures) and asserts the integration-test oracle:
  silver.customers            = 9 rows (customer 10 GDPR-deleted) — current view per id
  gold.customers_per_country  = DE=2 FR=2 GB=2 SG=2 US=1 — populated ⇒ gold DID
                                stream-read silver's changelog (the thing Iceberg can't do)

Exit 0 = PASS. Run inside the spark-paimon image.
"""
import sys
from pyspark.sql import SparkSession

EXPECTED = {"DE": 2, "FR": 2, "GB": 2, "SG": 2, "US": 1}
EXPECTED_ROWS = 9

spark = SparkSession.builder.appName("integration-test-gold-paimon").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
ok = True

print("=== paimon.silver.customers (current view per customer_id) ===")
try:
    sv = spark.read.format("paimon").table("paimon.silver.customers")
    sv_rows = sv.count()
    sv.select("customer_id", "name", "country", "segment").orderBy("customer_id").show(50, False)
    print(f"silver rows = {sv_rows} (expected {EXPECTED_ROWS})")
    ok = ok and sv_rows == EXPECTED_ROWS
except Exception as e:
    print(f"SILVER NOT POPULATED: {str(e)[:150]}")
    ok = False

print("=== paimon.gold.customers_per_country (did gold stream-read silver?) ===")
try:
    g = spark.read.format("paimon").table("paimon.gold.customers_per_country")
    counts = {r["country"]: r["customer_count"] for r in g.collect()}
    g.orderBy("country").show()
    match = counts == EXPECTED
    print(f"counts   = {dict(sorted(counts.items()))}")
    print(f"expected = {dict(sorted(EXPECTED.items()))}  -> {'OK' if match else 'MISMATCH'}")
    ok = ok and match
except Exception as e:
    print(f"GOLD NOT POPULATED (gold did NOT stream-read silver): {str(e)[:150]}")
    ok = False

print("PAIMON PASS — gold stream-read silver ✅" if ok else "PAIMON FAIL")
sys.exit(0 if ok else 1)
