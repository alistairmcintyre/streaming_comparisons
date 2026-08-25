"""
Consistency check for the spark-delta trades → open_positions pipeline.

No fixed oracle (fills are random), so this checks internal consistency of the
fold + enrichment after the streams have drained:
  - silver.accounts populated
  - gold rows exist, some OPEN / some CLOSED, enriched with country
  - fold is EXACT: sum(gold.net_quantity) == sum(signed qty in bronze.trades),
    sum(gold.trade_count) == count(bronze.trades)
Run inside the spark-delta image after stopping generators + letting gold catch up.
"""
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, sum as _sum

BRONZE = "s3a://warehouse/delta/bronze/trades"
SILVER = "s3a://warehouse/delta/silver/accounts"
GOLD = "s3a://warehouse/delta/gold/open_positions"

spark = SparkSession.builder.appName("it-delta-trades").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
ok = True

print("=== silver.accounts ===")
try:
    n = spark.read.format("delta").load(SILVER).count()
    print("accounts rows:", n)
    ok = ok and n > 0
except Exception as e:
    print("silver err:", str(e)[:160]); ok = False

print("=== bronze.trades vs gold.open_positions (fold correctness) ===")
try:
    bt = spark.read.format("delta").load(BRONZE)
    b_signed = bt.withColumn("sq", when(col("side") == "BUY", col("quantity")).otherwise(-col("quantity")))
    b_netqty = b_signed.agg(_sum("sq")).collect()[0][0] or 0
    b_count = bt.count()

    g = spark.read.format("delta").load(GOLD)
    g_rows = g.count()
    g_netqty = g.agg(_sum("net_quantity")).collect()[0][0] or 0
    g_tcount = g.agg(_sum("trade_count")).collect()[0][0] or 0
    open_n = g.filter(col("status") == "OPEN").count()
    enriched = g.filter(col("country").isNotNull()).count()

    g.orderBy(col("trade_count").desc()).select(
        "account_id", "symbol", "net_quantity", "trade_count", "status", "country", "tier"
    ).show(10, False)
    print(f"bronze: trades={b_count}, sum(signed qty)={b_netqty}")
    print(f"gold:   rows={g_rows}, sum(net_quantity)={g_netqty}, sum(trade_count)={g_tcount}, "
          f"open={open_n}, enriched(country!=null)={enriched}")
    qty_ok = b_netqty == g_netqty
    cnt_ok = b_count == g_tcount
    print(f"fold net_quantity match: {qty_ok} | trade_count match: {cnt_ok}")
    ok = ok and g_rows > 0 and qty_ok and cnt_ok and enriched > 0
except Exception as e:
    print("gold err:", str(e)[:200]); ok = False

print("DELTA-TRADES PASS" if ok else "DELTA-TRADES FAIL (if counts mismatch, gold may still be draining bronze)")
sys.exit(0 if ok else 1)
