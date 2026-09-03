"""
Iceberg table DDL as importable, idempotent statements, run IN the pipeline.

Each streaming job calls ensure_all(spark) at startup. CREATE ... IF NOT EXISTS is
create-once + idempotent, so every stage self-provisions with no separate ddl-init
and no ordering dependency. Catalog name is `rest` in both environments (bound to the
REST catalog locally and the Glue catalog on AWS via spark.sql.catalog.rest.*), so the
same SQL is portable. format-version 2 (Athena requirement).
"""
import os

_CAT = os.environ.get("ICEBERG_CATALOG", "rest")

_APPEND_PROPS = """  'format-version'='2', 'write.format.default'='parquet',
  'write.parquet.compression-codec'='zstd', 'write.target-file-size-bytes'='134217728',
  'commit.retry.num-retries'='10', 'write.distribution-mode'='none',
  'write.spark.fanout.enabled'='true'"""
_MOR_PROPS = """  'format-version'='2', 'write.format.default'='parquet',
  'write.parquet.compression-codec'='zstd', 'commit.retry.num-retries'='10',
  'write.delete.mode'='merge-on-read', 'write.update.mode'='merge-on-read',
  'write.merge.mode'='merge-on-read'"""

_STATEMENTS = [
    f"CREATE NAMESPACE IF NOT EXISTS {_CAT}.bronze",
    f"CREATE NAMESPACE IF NOT EXISTS {_CAT}.silver",
    f"CREATE NAMESPACE IF NOT EXISTS {_CAT}.gold",
    f"""CREATE TABLE IF NOT EXISTS {_CAT}.bronze.trades_spark (
        op STRING, trade_id BIGINT, account_id BIGINT, symbol STRING, side STRING,
        quantity INT, price DECIMAL(12,4), executed_at TIMESTAMP, event_ts TIMESTAMP,
        ingest_ts TIMESTAMP, kafka_offset BIGINT, kafka_partition INT,
          -- Postgres LSN: a strict TOTAL order across the replication stream.
          -- kafka_offset only orders within a partition, so it is a valid
          -- tiebreaker only because Debezium keys by primary key. lsn has no
          -- such precondition, the definitive key for a backfill ranking.
          source_lsn BIGINT)
      USING iceberg PARTITIONED BY (days(executed_at)) TBLPROPERTIES ({_APPEND_PROPS})""",
    f"""CREATE TABLE IF NOT EXISTS {_CAT}.silver.trades_spark (
        trade_id BIGINT, account_id BIGINT, symbol STRING, side STRING, quantity INT,
        price DECIMAL(12,4), executed_at TIMESTAMP, event_ts TIMESTAMP, ingest_ts TIMESTAMP,
        -- The CDC total order. bronze carried it and silver dropped it, so the ordering
        -- key this repo documents everywhere existed on three engines of five.
        source_lsn BIGINT)
      USING iceberg PARTITIONED BY (days(executed_at)) TBLPROPERTIES ({_APPEND_PROPS})""",
    # DELIBERATELY UNPARTITIONED, and this is the second answer to that question. The
    # first was bucket(16, account_id), on the reasoning that this was the only silver or
    # gold table with no layout, that every SCD2 micro-batch reads the current row for the
    # keys in its batch, and that an unorganised table therefore makes the MERGE scan
    # everything. That reasoning is fine and the conclusion was wrong, because it ignored
    # how a STREAMING table is written. Measured on real S3, 1000 accounts and 36,000
    # versions built by 120 micro-batches (tests/bucket_layout_bench.py):
    #
    #                            files   lookup     MERGE
    #     no layout               120    1384ms    5692ms
    #     bucket(16, account_id) 1920   15159ms   28825ms
    #
    # Bucketing was 11x worse on the lookup and 5x worse on the MERGE. The mechanism is
    # the file count. A bucketed append must write one file per bucket it touches, so 120
    # micro-batches produce 1920 files instead of 120, and the pruning that is supposed to
    # pay for that never arrives: a 75-key batch hashes across all 16 buckets, so every
    # bucket is read anyway. All of the cost, none of the benefit.
    # Bucketing pays when a query touches FEW buckets. Per-batch SCD2 maintenance touches
    # all of them by construction, which is the case against it here and would be the case
    # against it on any streamed dimension with randomly distributed keys.
    # NOT is_current either, the other tempting choice: partitioning on a mutable field
    # moves a row between partitions on every update.
    # The lever that actually helps is compaction (infra/aws/k8s/93-maintenance.yaml), which
    # attacks the file count directly rather than trading it for pruning. Measured, and it
    # is the ONLY thing that moved: 132 files to 1 took the MERGE from 567ms to 271ms
    # (tests/layout_strategy_bench.py).
    # AND SORTING IS NOT WORTH ADDING EITHER, which was the obvious next idea. Both
    # write-side distribution modes are inert on this table: hash distributes by partition
    # columns and there are none, and range distributes by sort order but only redistributes
    # WITHIN one write, so a 300-row micro-batch landing in about one file has nothing to
    # redistribute. Clustering across commits is a compaction concern on any engine.
    # The deeper reason is scale: this whole table, every version of every account, is
    # 539 KB and compacts to a SINGLE file. File statistics prune files, so with one file
    # there is nothing to skip. Data skipping is a large-table technique and this is not a
    # large table. Adding strategy => 'sort' to the rewrite would buy nothing and cost a
    # global sort.
    # Delta's clusterBy("account_id") on the same table is equally moot at this size; its
    # real advantage is optimizeWrite + autoCompact running INLINE, holding the file count
    # down continuously rather than every 15 minutes.
    f"""CREATE TABLE IF NOT EXISTS {_CAT}.silver.accounts_spark (
        account_id BIGINT NOT NULL, name STRING, country STRING, tier STRING,
          source_updated_at TIMESTAMP, event_ts TIMESTAMP,
          -- SCD2: every version retained, validity MATERIALISED by the atomic close-out
          -- (one MERGE writes the new version AND re-writes its predecessor with
          -- effective_to set, so a reader never sees two current rows or none).
          -- Natural key (account_id, source_lsn): a CDC re-delivery collapses, a real
          -- change does not.
          effective_from TIMESTAMP, effective_to TIMESTAMP, is_current BOOLEAN,
          source_lsn BIGINT, op STRING, commit_ts TIMESTAMP)
      USING iceberg TBLPROPERTIES ({_MOR_PROPS})""",
    f"""CREATE TABLE IF NOT EXISTS {_CAT}.gold.open_positions_spark (
        account_id BIGINT NOT NULL, symbol STRING NOT NULL, net_quantity BIGINT,
        net_notional DECIMAL(38,4), trade_count BIGINT, status STRING,
        -- country/tier are not denormalised here, they are account attributes with no
        -- defensible temporal semantic on a current-state row. Enrich at query time:
        --   SELECT p.*, a.country, a.tier FROM gold.open_positions_spark p
        --   LEFT JOIN silver.accounts_spark a USING (account_id)
        -- LEFT, always: a fill can land before its account row (independent CDC
        -- streams), and an inner join would silently drop that position.
        -- EVENT time (from executed_at); commit_ts stays PROCESSING time, so
        -- (commit_ts - last_updated_at) is a per-row processing delay readable
        -- straight out of the table, identically in all five engines.
        opened_at TIMESTAMP, last_updated_at TIMESTAMP, commit_ts TIMESTAMP)
      USING iceberg PARTITIONED BY (symbol) TBLPROPERTIES ({_MOR_PROPS})""",
]


def ensure_all(spark):
    for stmt in _STATEMENTS:
        spark.sql(stmt)
