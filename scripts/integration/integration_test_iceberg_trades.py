"""
Consistency check for the spark-iceberg trades → open_positions pipeline.
Reads the Iceberg tables via the REST catalog after streams drain:
  - silver.accounts_spark populated
  - fold EXACT (in a no-failure run): sum(gold.net_quantity) == sum(signed qty in
    silver.trades_spark), sum(gold.trade_count) == count(silver.trades_spark)
Run inside the spark-iceberg image.
"""
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, sum as _sum

spark = SparkSession.builder.appName("it-iceberg-trades").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
ok = True

print("=== silver.accounts_spark ===")
try:
    n = spark.table("rest.silver.accounts_spark").count()
    print("accounts rows:", n); ok = ok and n > 0
except Exception as e:
    print("silver err:", str(e)[:160]); ok = False

print("=== silver.trades_spark vs gold.open_positions_spark (fold correctness) ===")
try:
    st = spark.table("rest.silver.trades_spark")
    s_signed = st.withColumn("sq", when(col("side") == "BUY", col("quantity")).otherwise(-col("quantity")))
    s_netqty = s_signed.agg(_sum("sq")).collect()[0][0] or 0
    s_count = st.count()

    g = spark.table("rest.gold.open_positions_spark")
    g_rows = g.count()
    g_netqty = g.agg(_sum("net_quantity")).collect()[0][0] or 0
    g_tcount = g.agg(_sum("trade_count")).collect()[0][0] or 0
    open_n = g.filter(col("status") == "OPEN").count()
    enriched = g.filter(col("country").isNotNull()).count()

    g.orderBy(col("trade_count").desc()).select(
        "account_id", "symbol", "net_quantity", "trade_count", "status", "country", "tier").show(10, False)
    print(f"silver.trades: rows={s_count}, sum(signed qty)={s_netqty}")
    print(f"gold: rows={g_rows}, sum(net_quantity)={g_netqty}, sum(trade_count)={g_tcount}, open={open_n}, enriched={enriched}")
    qty_ok = s_netqty == g_netqty
    cnt_ok = s_count == g_tcount
    print(f"fold net_quantity match: {qty_ok} | trade_count match: {cnt_ok}")
    ok = ok and g_rows > 0 and qty_ok and cnt_ok and enriched > 0
except Exception as e:
    print("gold err:", str(e)[:200]); ok = False

print("ICEBERG-TRADES PASS" if ok else "ICEBERG-TRADES FAIL (if counts mismatch, gold may still be draining silver)")
sys.exit(0 if ok else 1)
