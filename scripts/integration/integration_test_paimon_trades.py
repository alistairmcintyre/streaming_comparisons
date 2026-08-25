"""
Consistency check for the flink-paimon trades → open_positions pipeline (native fold).

Reads the Paimon tables Flink wrote (warehouse s3://warehouse/paimon) via the
spark-paimon catalog, after streams drained:
  - silver.accounts populated
  - gold.open_positions: rows exist; fold is EXACT —
    sum(gold.net_quantity) == sum(signed qty in silver.trades),
    sum(gold.trade_count) == count(silver.trades)
Run inside the spark-paimon image.
"""
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, sum as _sum

spark = SparkSession.builder.appName("it-paimon-trades").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
ok = True

print("=== paimon.silver.trades vs paimon.gold.open_positions (fold correctness) ===")
try:
    st = spark.read.format("paimon").table("paimon.silver.trades")
    s_signed = st.withColumn("sq", when(col("side") == "BUY", col("quantity")).otherwise(-col("quantity")))
    s_netqty = s_signed.agg(_sum("sq")).collect()[0][0] or 0
    s_count = st.count()

    g = spark.read.format("paimon").table("paimon.gold.open_positions")
    g_rows = g.count()
    g_netqty = g.agg(_sum("net_quantity")).collect()[0][0] or 0
    g_tcount = g.agg(_sum("trade_count")).collect()[0][0] or 0
    open_n = g.filter(col("net_quantity") != 0).count()

    g.orderBy(col("trade_count").desc()).select(
        "account_id", "symbol", "net_quantity", "trade_count").show(10, False)
    print(f"silver.trades: rows={s_count}, sum(signed qty)={s_netqty}")
    print(f"gold: rows={g_rows}, sum(net_quantity)={g_netqty}, sum(trade_count)={g_tcount}, open(net!=0)={open_n}")
    qty_ok = s_netqty == g_netqty
    cnt_ok = s_count == g_tcount
    print(f"fold net_quantity match: {qty_ok} | trade_count match: {cnt_ok}")
    ok = ok and g_rows > 0 and qty_ok and cnt_ok
except Exception as e:
    print("gold/silver err:", str(e)[:200]); ok = False

print("=== paimon.silver.accounts ===")
try:
    n = spark.read.format("paimon").table("paimon.silver.accounts").count()
    print("accounts rows:", n)
    ok = ok and n > 0
except Exception as e:
    print("accounts err:", str(e)[:150]); ok = False

print("PAIMON-TRADES PASS" if ok else "PAIMON-TRADES FAIL (if counts mismatch, gold may still be draining silver)")
sys.exit(0 if ok else 1)
