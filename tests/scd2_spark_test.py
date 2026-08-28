"""Unit test for jobs/_shared/scd2.py — the SCD2 staging shared by the Spark engines.

Same scenarios as tests/scd2-behaviour.sh does for Flink, so the two families are held to
one definition of SCD2:

  account 1: v100 (day 1), then v200 (day 5)   -> v100 closed at day 5, v200 current
  account 2: v150, never changed                -> current, never closed
  account 3: v400 (day 6), then an OUT-OF-ORDER v300 (day 5, arrives later)
                                                -> v400 stays current; v300 is a late
                                                   historical row, NOT a second current one
  account 4: v500 arrives twice (re-delivery)   -> collapses, one row
  account 5: v600 ALREADY in the table, batch brings v700
                                                -> close row carries v600's real attributes
                                                   (a MERGE only updates two columns and
                                                   hides a null here; a Hudi upsert does not)

Plain DataFrames, no Delta/Iceberg/Hudi — the logic is engine-agnostic and this runs in
seconds without a table format.
"""
import sys, datetime as dt
sys.path.insert(0, "jobs/_shared")
from pyspark.sql import SparkSession
from pyspark.sql.types import (StructType, StructField, LongType, StringType, TimestampType, BooleanType)
from scd2 import stage_scd2

D = lambda d: dt.datetime(2026, 1, d)
spark = (SparkSession.builder.appName("scd2-unit").master("local[2]")
         .config("spark.sql.shuffle.partitions", "2").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

schema = StructType([
    StructField("account_id", LongType()), StructField("tier", StringType()),
    # a NON-STRING attribute on purpose: close rows are rebuilt from `current` and cast
    # back to the batch's types, and casting them all to string breaks unionByName.
    StructField("source_updated_at", TimestampType()),
    StructField("effective_from", TimestampType()), StructField("source_lsn", LongType())])

batch = spark.createDataFrame([
    (1, "A", D(1), D(1), 100), (2, "X", D(3), D(3), 150), (1, "B", D(5), D(5), 200),
    (3, "P", D(6), D(6), 400), (3, "Q", D(5), D(5), 300),  # generated BEFORE 400, arrives after
    (4, "Z", D(8), D(8), 500), (4, "Z", D(8), D(8), 500),  # re-delivery
], schema)
current = spark.createDataFrame([], schema)             # empty table to start

ATTRS = ["tier", "source_updated_at"]
staged = stage_scd2(batch, current, attrs=ATTRS).collect()

# Second scenario: a key whose current row is ALREADY in the table. This is the only path
# that produces a close row rebuilt from `current` rather than from the batch.
existing = spark.createDataFrame([(5, "M", D(2), D(2), 600)], schema)
staged += stage_scd2(spark.createDataFrame([(5, "N", D(9), D(9), 700)], schema),
                     existing, attrs=ATTRS).collect()

# model the sink: PK (account_id, source_lsn), last write wins
merged = {}
for r in staged:
    merged[(r["account_id"], r["source_lsn"])] = r
rows = sorted(merged.values(), key=lambda r: (r["account_id"], r["source_lsn"]))

print("  after PK merge on (account_id, source_lsn):")
for r in rows:
    print(f"      acct={r['account_id']} lsn={r['source_lsn']} from={r['effective_from'].date()} "
          f"to={r['effective_to'].date() if r['effective_to'] else None} current={r['is_current']}")

fails = []
def chk(desc, ok):
    print(("  PASS  " if ok else "  FAIL  ") + desc)
    if not ok: fails.append(desc)

g = {(r["account_id"], r["source_lsn"]): r for r in rows}
chk("acct1 v100 closed at day 5",
    g[(1,100)]["effective_to"] == D(5) and g[(1,100)]["is_current"] is False)
chk("acct1 v200 current",       g[(1,200)]["effective_to"] is None and g[(1,200)]["is_current"] is True)
chk("acct2 never closed",       g[(2,150)]["effective_to"] is None and g[(2,150)]["is_current"] is True)
chk("acct3 v400 stays current", g[(3,400)]["is_current"] is True)
chk("acct3 out-of-order v300 is NOT current", g[(3,300)]["is_current"] is False)
chk("acct4 re-delivery collapsed to one row", len([r for r in rows if r["account_id"] == 4]) == 1)
chk("acct5 v600 closed from the TABLE at day 9",
    g[(5,600)]["effective_to"] == D(9) and g[(5,600)]["is_current"] is False)
chk("acct5 close row kept v600's attributes", g[(5,600)]["tier"] == "M"
    and g[(5,600)]["source_updated_at"] == D(2))
chk("acct5 v700 current", g[(5,700)]["is_current"] is True)
# A validity range must never run backwards. It cannot happen with ordered input, but an
# out-of-order arrival makes it reachable, and a backwards range would silently match
# nothing in an as-of join rather than erroring.
bad = [r for r in rows if r["effective_to"] is not None and r["effective_to"] < r["effective_from"]]
chk("no validity range runs backwards", not bad)

for a in (1, 2, 3, 4, 5):
    n = len([r for r in rows if r["account_id"] == a and r["is_current"]])
    chk(f"acct{a} has exactly one current row", n == 1)

spark.stop()
print(("\nSCD2 staging is correct" if not fails else "\nSCD2 staging is WRONG: " + "; ".join(fails)))
sys.exit(1 if fails else 0)
