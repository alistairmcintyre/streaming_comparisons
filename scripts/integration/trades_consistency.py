"""Post-run consistency check for one engine's bronze -> silver -> gold trades pipeline.

    spark-submit trades_consistency.py delta|iceberg|hudi|paimon
    ENGINE=delta ... (the images' entrypoint passes no argv, so the env var is the hook)
    make consistency-check ENGINE=iceberg

Run AFTER quiesce-run.sh, against a live run's tables. Mid-flight, gold legitimately
trails silver by whatever is in the current micro-batch, so a drift figure would just be
measuring the race.

There is no fixed oracle (fills are random) so this checks INTERNAL consistency:

  1. one row per trade_id in silver             (dedupe actually happened)
  2. one current row per account_id             (the SCD2 invariant)
  3. the fold is EXACT against silver           (no double-count, no loss)
  4. status agrees with net_quantity            (OPEN iff net != 0)
  5. opened_at <= last_updated_at               (no backwards validity)
  6. the read-time LEFT JOIN to silver.accounts enriches WITHOUT fanning out

(6) replaces what these tests used to do: assert a `country` column on gold. country and
tier were removed from gold when enrichment moved to query time, a position row has no
defensible temporal semantic for an account attribute. The join is now the thing under
test, and the assertion that matters is that it does not multiply rows: silver.accounts is
SCD2, so joining WITHOUT the is_current filter fans every position out by its account's
version count, silently inflating every downstream aggregate.

These tests also used to compare gold against BRONZE on Delta. gold reads SILVER, and
silver is deduplicated, so any at-least-once re-delivery made that comparison fail for a
reason that was not a bug.
"""
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, sum as _sum, count as _count

# argv or the ENGINE env var: the Spark images' entrypoint ends with "${JOB_FILE}" and
# passes no arguments through, so compose/k8s can only reach this via the environment.
ENGINE = sys.argv[1] if len(sys.argv) > 1 else os.environ.get("ENGINE", "")
spark = SparkSession.builder.appName(f"it-{ENGINE}-trades").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

D = "s3a://warehouse"
_ICE = {"silver/trades": "silver.trades_spark", "silver/accounts": "silver.accounts_spark",
        "gold/open_positions": "gold.open_positions_spark"}
READERS = {
    "delta":   lambda t: spark.read.format("delta").load(f"{D}/delta/{t}"),
    "hudi":    lambda t: spark.read.format("hudi").load(f"{D}/hudi/{t}"),
    "iceberg": lambda t: spark.table("rest." + _ICE[t]),
    "paimon":  lambda t: spark.read.format("paimon").table("paimon." + t.replace("/", ".")),
}
if ENGINE not in READERS:
    print(f"unknown engine {ENGINE}; expected one of {sorted(READERS)}")
    sys.exit(2)
read = READERS[ENGINE]

fails = []
def chk(desc, ok, detail=""):
    print(("  PASS  " if ok else "  FAIL  ") + desc + (f"   {detail}" if detail else ""))
    if not ok:
        fails.append(desc)

try:
    trades = read("silver/trades")
    accounts = read("silver/accounts")
    gold = read("gold/open_positions")
except Exception as e:
    print(f"could not read {ENGINE}'s tables: {str(e)[:300]}")
    sys.exit(1)

# 1. dedupe
n_rows, n_ids = trades.count(), trades.select("trade_id").distinct().count()
chk("silver.trades has one row per trade_id", n_rows == n_ids,
    f"rows={n_rows} distinct={n_ids} dupes={n_rows - n_ids}")

# 2. SCD2 invariant
cur = accounts.filter(col("is_current"))
multi = cur.groupBy("account_id").agg(_count("*").alias("n")).filter(col("n") > 1).count()
chk("silver.accounts has exactly one current row per account",
    accounts.count() > 0 and multi == 0, f"accounts_with_multiple_current={multi}")

# 3. the fold, against SILVER (what gold actually reads), not bronze
signed = trades.withColumn("sq", when(col("side") == "BUY", col("quantity")).otherwise(-col("quantity")))
s_qty = signed.agg(_sum("sq")).collect()[0][0] or 0
g_qty = gold.agg(_sum("net_quantity")).collect()[0][0] or 0
g_cnt = gold.agg(_sum("trade_count")).collect()[0][0] or 0
chk("sum(gold.net_quantity) == sum(signed qty in silver.trades)", s_qty == g_qty,
    f"silver={s_qty} gold={g_qty}")
chk("sum(gold.trade_count) == count(silver.trades)", n_rows == g_cnt,
    f"silver={n_rows} gold={g_cnt}")

# 4/5. internal consistency of the book
bad_status = gold.filter(((col("net_quantity") != 0) & (col("status") != "OPEN")) |
                         ((col("net_quantity") == 0) & (col("status") != "CLOSED"))).count()
chk("status is OPEN iff net_quantity != 0", bad_status == 0, f"violations={bad_status}")
backwards = gold.filter(col("opened_at") > col("last_updated_at")).count()
chk("opened_at <= last_updated_at", backwards == 0, f"violations={backwards}")

# 6. the documented read-time enrichment: LEFT JOIN, current rows only
g_rows = gold.count()
enriched = gold.join(cur.select("account_id", "country", "tier"), "account_id", "left")
e_rows = enriched.count()
chk("read-time LEFT JOIN does not fan out", g_rows == e_rows, f"gold={g_rows} joined={e_rows}")
chk("read-time LEFT JOIN actually enriches",
    g_rows > 0 and enriched.filter(col("country").isNotNull()).count() > 0)

print(f"\n{ENGINE.upper()} trades pipeline is internally consistent" if not fails
      else f"\n{ENGINE.upper()} INCONSISTENT: " + "; ".join(fails)
           + "\n(if only the fold totals mismatch, gold may still be draining silver, quiesce first)")
sys.exit(1 if fails else 0)
