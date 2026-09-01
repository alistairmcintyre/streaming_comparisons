"""Soak the REAL hudi silver.accounts SCD2 write path, in isolation.

WHY THIS EXISTS: on the AWS run, `hudi-silver-accounts` went FAILED after exhausting
its 10 retries, and the captured log was 300 lines of ColumnResolutionHelper stack
frames with the actual message truncated off the top. This drives the SAME function
the job drives -- imported from jobs/spark-hudi/silver_accounts.py, not a copy -- so
whatever the job hits, this hits, and the message is printed in full.

Differences from tests/scd2_hudi_upsert_test.py, which passes and did NOT catch it:
  * that test hand-calls stage_scd2 over 4 tiny batches; this runs the job's own
    upsert_scd2 under a REAL structured-streaming query with a checkpoint, so the
    micro-batch plan, the read-back of `current` from a growing Hudi table, and
    inline compaction (every 5 delta commits) all happen for real;
  * that test forces Glue meta-sync OFF. silver_accounts_opts() turns it ON, so on
    AWS every commit also runs AwsGlueCatalogSyncTool -- a path no local test has
    ever exercised. SOAK_GLUE_SYNC=true re-enables it.

Kafka is replaced by a rate source shaped into the exact schema main() produces; the
account mix matches generators/gen_accounts.py (1000 ids, ~15% new, rest country/tier
churn) plus deliberate re-deliveries and out-of-order versions.

  HUDI_WAREHOUSE   where to write   (local dir, or s3a://... to soak against AWS)
  SOAK_BATCHES     micro-batches to run          (default 40)
  SOAK_EVENTS_PER  events per micro-batch        (default 75 = 5/s x 15s trigger)
  SOAK_GLUE_SYNC   "true" to leave Glue sync on  (default off)
  SOAK_GLUE_DB     Glue database when sync is on (default "silver")
  SOAK_GLUE_TABLE  Glue table when sync is on    (default "accounts_hudi_probe" -- a
                   SCRATCH name, so a probe run cannot redefine the benchmark's own
                   silver.accounts_hudi through AwsGlueCatalogSyncTool)
  SOAK_DRIVER_MEM  driver heap. DEFAULT 2g TO MATCH infra/aws/k8s/95-spark-hudi.yaml --
                   the heap matters here: at the container default the
                   broadcast build in stage_scd2's join dies with a StackOverflowError
                   on batch 2, and at 6g the same code runs clean. Any conclusion drawn
                   from this probe is only about AWS if the heap matches AWS.
"""
import os, sys, shutil, traceback

sys.path.insert(0, "jobs/_shared")
sys.path.insert(0, "jobs/spark-hudi")

WAREHOUSE = os.environ.get("HUDI_WAREHOUSE", "/tmp/hudi_accounts_soak/hudi")
BATCHES   = int(os.environ.get("SOAK_BATCHES", "40"))
PER_BATCH = int(os.environ.get("SOAK_EVENTS_PER", "75"))
GLUE_SYNC = os.environ.get("SOAK_GLUE_SYNC", "false").lower() == "true"
DRIVER_MEM = os.environ.get("SOAK_DRIVER_MEM", "2g")
EXTRA_CONF = [kv.split("=", 1) for kv in
              os.environ.get("SOAK_CONF", "").split(",") if "=" in kv]
os.environ["HUDI_WAREHOUSE"] = WAREHOUSE          # read at import by hudi_tables

import hudi_tables
import silver_accounts as job                     # the REAL job module

_real_opts = hudi_tables.silver_accounts_opts
if not GLUE_SYNC:                                 # keep the local run off the catalog
    def _no_sync():
        return {**_real_opts(), "hoodie.datasource.meta.sync.enable": "false",
                "hoodie.datasource.hive_sync.enable": "false"}
    job.silver_accounts_opts = _no_sync
else:
    # A REAL Glue sync, but NEVER onto the benchmark's own table. silver_accounts_opts()
    # syncs to silver.accounts_hudi, and AwsGlueCatalogSyncTool would rewrite that table's
    # columns, serde and partition keys from whatever this probe happened to write --
    # so a diagnostic run would silently redefine a table the next benchmark reads.
    # The probe gets its own name; nothing else about the sync path changes, which is the
    # point of running it at all.
    GLUE_DB = os.environ.get("SOAK_GLUE_DB", "silver")
    GLUE_TABLE = os.environ.get("SOAK_GLUE_TABLE", "accounts_hudi_probe")
    def _probe_sync():
        o = dict(_real_opts())
        o["hoodie.datasource.hive_sync.database"] = GLUE_DB
        o["hoodie.datasource.hive_sync.table"] = GLUE_TABLE
        o["hoodie.table.name"] = GLUE_TABLE
        return o
    job.silver_accounts_opts = _probe_sync
    print(f"GLUE SYNC ON -> {GLUE_DB}.{GLUE_TABLE} "
          f"(_ro and _rt); NOT the benchmark's accounts_hudi", flush=True)

from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, lit, expr, current_timestamp, concat,
                                   when, floor, rand)

LOCAL = not WAREHOUSE.startswith("s3")
CHK = ("/tmp/hudi_accounts_soak/_chk" if LOCAL else f"{WAREHOUSE}/_soak_chk")
if LOCAL:
    shutil.rmtree("/tmp/hudi_accounts_soak", ignore_errors=True)

b = (SparkSession.builder.appName("hudi-accounts-soak").master("local[2]")
     .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
     .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
     .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
     .config("spark.sql.shuffle.partitions", "4")
     .config("spark.driver.memory", DRIVER_MEM))
if not LOCAL:
    # IRSA is not available off-cluster, so s3a uses whatever the ambient chain resolves
    # -- the SSO session exported into this container's env. Same provider class the
    # manifests set, so the probe exercises the production code path, not a variant.
    b = (b.config("spark.hadoop.fs.s3a.aws.credentials.provider",
                  "software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider")
          .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"))
for k, v in EXTRA_CONF:
    b = b.config(k.strip(), v.strip())
spark = b.getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Report the heap the JVM ACTUALLY got. spark.driver.memory is only honoured if the
# driver JVM has not already been launched, so a builder .config() can silently be a
# no-op -- and every conclusion here is conditioned on the heap really being AWS's.
_heap = spark.sparkContext._jvm.java.lang.Runtime.getRuntime().maxMemory() // (1 << 20)
print(f"driver heap actually = {_heap} MiB (asked for {DRIVER_MEM}); "
      f"extra conf = {EXTRA_CONF}", flush=True)

# Rate source -> the exact frame main() hands to upsert_scd2.
# value is a monotonic counter, so source_lsn is a strict total order like the real CDC
# stream. account_id churns over the 1000 seeded ids; every 37th event repeats the
# PREVIOUS lsn for the same account (an at-least-once re-delivery) and every 53rd lands
# an out-of-order (lower) lsn: the two cases scd2.py calls out as mattering.
src = (spark.readStream.format("rate").option("rowsPerSecond", PER_BATCH)
       .option("rampUpTime", "0s").load())

# ONE select, exactly as main() builds `parsed` from from_json -- a chain of
# withColumns would give the micro-batch a deeper plan than the real job has, and the
# whole point is to reproduce the JOB, not the harness.
parsed = (src.selectExpr(
    "'u' AS op",
    "(value % 1000) + 1 AS account_id",
    "concat('acct-', cast((value % 1000) + 1 as string)) AS name",
    "element_at(array('GB','US','DE','FR','JP'), cast(value % 5 as int) + 1) AS country",
    "element_at(array('bronze','silver','gold'), cast(value % 3 as int) + 1) AS tier",
    "timestamp AS source_updated_at",
    "timestamp AS event_ts",
    "timestamp AS effective_from",
    # every 37th event repeats a prior version (at-least-once re-delivery) and every
    # 53rd lands a lower lsn (late historical arrival) -- the two cases scd2.py calls
    # the number matters.
    "CASE WHEN value % 37 = 0 THEN value - 1000 "
    "     WHEN value % 53 = 0 THEN value - 1500 "
    "     ELSE value END AS source_lsn",
    "current_timestamp() AS commit_ts"))

# `n` counts COMPLETED batches, not entered ones, and `stopping` suppresses the
# InterruptedException that q.stop() raises in whatever batch is still in flight.
# Counting on entry let the main loop see n == BATCHES while the last batch was still
# running, stop the query underneath it, and then print OK over the top of its failure --
# which would make this regression test green on exactly the bug it exists to catch.
seen = {"n": 0, "err": None, "stopping": False}

def wrapped(batch, batch_id):
    """Call the job's own upsert_scd2, but print the FULL failure -- the thing the
    truncated AWS log lost."""
    try:
        job.upsert_scd2(batch, batch_id)
    except Exception as e:
        if seen["stopping"]:
            return                                  # our own shutdown, not a defect
        seen["err"] = e
        print(f"\n!! BATCH {batch_id} FAILED: {type(e).__name__}", flush=True)
        print(str(e)[:4000], flush=True)
        traceback.print_exc()
        raise
    seen["n"] += 1
    if seen["n"] % 5 == 0:
        print(f"   ... {seen['n']} batches ok", flush=True)

print(f"soaking {BATCHES} batches x {PER_BATCH} events -> {WAREHOUSE} "
      f"(glue_sync={GLUE_SYNC})", flush=True)

q = (parsed.writeStream.foreachBatch(wrapped)
     .option("checkpointLocation", CHK)
     .trigger(processingTime="2 seconds").start())

while q.isActive and seen["n"] < BATCHES and seen["err"] is None:
    q.awaitTermination(2)

seen["stopping"] = True
q.stop()

if seen["err"] is not None:
    print(f"\nFAIL: reproduced after {seen['n']} batches", flush=True)
    sys.exit(1)

# The table must still be readable, and SCD2's core invariant must hold.
df = spark.read.format("hudi").load(hudi_tables.SILVER_ACCOUNTS)
multi = (df.filter(col("is_current")).groupBy("account_id").count()
           .filter(col("count") > 1).count())
print(f"\nOK: {seen['n']} batches, {df.count()} rows, "
      f"{df.filter(col('is_current')).count()} current", flush=True)
if multi:
    print(f"FAIL: {multi} accounts have more than one current row", flush=True)
    sys.exit(1)
print("OK: exactly one current row per account", flush=True)
