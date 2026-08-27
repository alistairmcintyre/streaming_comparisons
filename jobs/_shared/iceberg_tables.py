"""
Iceberg table DDL as importable, idempotent statements — run IN the pipeline.

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
        ingest_ts TIMESTAMP, kafka_offset BIGINT, kafka_partition INT)
      USING iceberg PARTITIONED BY (days(executed_at)) TBLPROPERTIES ({_APPEND_PROPS})""",
    f"""CREATE TABLE IF NOT EXISTS {_CAT}.silver.trades_spark (
        trade_id BIGINT, account_id BIGINT, symbol STRING, side STRING, quantity INT,
        price DECIMAL(12,4), executed_at TIMESTAMP, event_ts TIMESTAMP, ingest_ts TIMESTAMP)
      USING iceberg PARTITIONED BY (days(executed_at)) TBLPROPERTIES ({_APPEND_PROPS})""",
    f"""CREATE TABLE IF NOT EXISTS {_CAT}.silver.accounts_spark (
        account_id BIGINT NOT NULL, name STRING, country STRING, tier STRING,
        source_updated_at TIMESTAMP, event_ts TIMESTAMP, commit_ts TIMESTAMP)
      USING iceberg TBLPROPERTIES ({_MOR_PROPS})""",
    f"""CREATE TABLE IF NOT EXISTS {_CAT}.gold.open_positions_spark (
        account_id BIGINT NOT NULL, symbol STRING NOT NULL, net_quantity BIGINT,
        net_notional DECIMAL(38,4), trade_count BIGINT, status STRING,
        -- country/tier are NOT denormalised here — they are account attributes with no
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
