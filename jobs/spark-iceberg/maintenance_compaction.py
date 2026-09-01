"""
Iceberg maintenance job: compaction, position delete rewrite, snapshot expiry.

Runs in a loop every 10 minutes, processing all 8 tables.
Spark is used for both Spark-written and Flink-written tables. Iceberg is
engine-agnostic, so this is standard practice.

Why this is not optional:
  - merge-on-read silver tables accumulate position/equality delete files at
    every streaming commit (10, 20s cadence).
  - Without rewrite_position_delete_files, read performance collapses within
    an hour as Iceberg must apply thousands of delete files on every scan.
  - expire_snapshots prevents unbounded metadata growth.
"""

import os
import time
import logging
from datetime import datetime, timedelta

from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

COMPACTION_INTERVAL_SECS = int(os.environ.get("COMPACTION_INTERVAL_SECS", "600"))  # 10 min
SNAPSHOT_RETAIN_HOURS    = int(os.environ.get("SNAPSHOT_RETAIN_HOURS", "1"))
SNAPSHOT_RETAIN_LAST     = int(os.environ.get("SNAPSHOT_RETAIN_LAST", "5"))

# The tables this run actually writes. This list went stale once when the pipeline it
# was written for was replaced: the job kept compacting eight tables that no longer
# existed, left the real ones untouched, failed per table without raising, and reported
# healthy throughout. That matters more for Iceberg than for any other engine here:
# it is the one format with no in-writer compaction, so this job is the only thing
# keeping its file counts down (see README.md (Sizing)).
TABLES = [
    "rest.bronze.trades_spark",
    "rest.silver.trades_spark",
    "rest.silver.accounts_spark",
    "rest.gold.open_positions_spark",
]


def compact_table(spark: SparkSession, table: str):
    log.info(f"Compacting: {table}")
    try:
        spark.sql(f"""
            CALL rest.system.rewrite_data_files(
                table => '{table}',
                options => map(
                    'target-file-size-bytes', '134217728',
                    'delete-file-threshold',  '5',
                    'min-input-files',        '2'
                )
            )
        """).show(truncate=False)
    except Exception as e:
        log.warning(f"rewrite_data_files failed for {table}: {e}")

    try:
        spark.sql(f"""
            CALL rest.system.rewrite_position_delete_files(
                table => '{table}',
                options => map('target-file-size-bytes', '134217728')
            )
        """).show(truncate=False)
    except Exception as e:
        log.warning(f"rewrite_position_delete_files failed for {table}: {e}")

    try:
        cutoff = (datetime.utcnow() - timedelta(hours=SNAPSHOT_RETAIN_HOURS)).strftime("%Y-%m-%d %H:%M:%S")
        spark.sql(f"""
            CALL rest.system.expire_snapshots(
                table       => '{table}',
                older_than  => TIMESTAMP '{cutoff}',
                retain_last => {SNAPSHOT_RETAIN_LAST}
            )
        """).show(truncate=False)
    except Exception as e:
        log.warning(f"expire_snapshots failed for {table}: {e}")


def main():
    spark = (
        SparkSession.builder
        .appName("iceberg-compactor")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    log.info(f"Compactor running. Interval: {COMPACTION_INTERVAL_SECS}s, retain last {SNAPSHOT_RETAIN_LAST} snapshots, expire after {SNAPSHOT_RETAIN_HOURS}h")

    while True:
        log.info("Starting compaction cycle...")
        for table in TABLES:
            compact_table(spark, table)
        log.info(f"Compaction cycle complete. Sleeping {COMPACTION_INTERVAL_SECS}s...")
        time.sleep(COMPACTION_INTERVAL_SECS)


if __name__ == "__main__":
    main()
