"""
Delta maintenance — VACUUM only.

Compaction is NOT done here: the tables enable Optimized Writes + Auto Compaction
IN the streaming pipeline (delta.autoOptimize.optimizeWrite / .autoCompact — set on
the tables in jobs/_shared/delta_tables.py, and belt-and-suspenders as session confs on
the streaming SparkApplication). That mirrors Paimon's in-writer compaction, so this
job runs only the piece that CAN'T run in-stream: VACUUM (GC of tombstoned files past
retention — there is no auto-vacuum in OSS Delta).

Warehouse base is env-driven so it works locally (MinIO 'warehouse') and on AWS.
"""
import os
from pyspark.sql import SparkSession

BASE = os.environ.get("DELTA_WAREHOUSE", "s3a://warehouse/delta")
TABLES = [
    f"{BASE}/bronze/trades",
    f"{BASE}/silver/trades",
    f"{BASE}/silver/accounts",
    f"{BASE}/gold/open_positions",
]
# Aggressive retention for an ephemeral benchmark; disable the 7-day safety check.
# NEVER do this on a real table (you can delete files a reader/time-travel still needs).
VACUUM_RETAIN_HOURS = float(os.environ.get("VACUUM_RETAIN_HOURS", "1"))

spark = (
    SparkSession.builder.appName("delta-vacuum")
    .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

for path in TABLES:
    try:
        print(f"VACUUM delta.`{path}` RETAIN {VACUUM_RETAIN_HOURS} HOURS", flush=True)
        spark.sql(f"VACUUM delta.`{path}` RETAIN {VACUUM_RETAIN_HOURS} HOURS")
    except Exception as e:  # a table may not exist yet early in the run
        print(f"skip {path}: {str(e)[:200]}", flush=True)

spark.stop()
