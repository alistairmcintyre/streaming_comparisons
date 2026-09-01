-- Fluss hot-tier tables for the trades / open-positions pipeline.
-- All are datalake-enabled → the Fluss tiering service continuously moves them
-- into Paimon, which exposes the Iceberg view for Athena/Trino/spark-iceberg.
--
-- LAYERING. Fluss collapses bronze and silver into one table: the PK table is the
-- cleaned, deduped current view (that is the point of a hot tier, no separate
-- landing zone to re-read). We name the database `silver` rather than inventing a
-- bronze it does not have, so a layer name means the same thing in every engine and
-- the cross-engine reconciliation can address tables uniformly.
--   silver.trades       ← Kafka (Debezium), deduped on trade_id
--   silver.accounts     ← Kafka (Debezium), SCD2 dimension (every version retained)
--   gold.open_positions ← the streaming fold over silver.trades
--
-- This is a TEMPLATE rendered by submit.sh (envsubst) for the active DEPLOY_ENV.
-- FLUSS_ICEBERG_OPTS carries the 'paimon.'-prefixed Iceberg-compat options, which
-- Fluss forwards to the Paimon table the tiering service creates:
--   local: 'paimon.metadata.iceberg.storage' = 'hadoop-catalog'  (S3/MinIO warehouse)
--   aws:   'paimon.metadata.iceberg.storage' = 'hive-catalog' + Glue client so
--          Athena discovers the tables (see submit.sh).

CREATE CATALOG fluss_catalog WITH (
    'type'              = 'fluss',
    'bootstrap.servers' = '${FLUSS_BOOTSTRAP}'
);

CREATE DATABASE IF NOT EXISTS fluss_catalog.silver;
CREATE DATABASE IF NOT EXISTS fluss_catalog.gold;

-- silver.trades. PK table (dedupes re-deliveries on trade_id), tiers to Paimon.
CREATE TABLE IF NOT EXISTS fluss_catalog.silver.trades (
    trade_id    BIGINT,
    account_id  BIGINT,
    symbol      STRING,
    side        STRING,
    quantity    INT,
    price       DECIMAL(12,4),
    executed_at TIMESTAMP(6),
    -- event_ts is the SOURCE commit time (Debezium source.ts_ms); ingest_ts is when this
  -- pipeline wrote the row. Fluss has no bronze hop (silver is its landing table) so
    -- ingest_ts is stamped here rather than one hop earlier. Both were missing, leaving
    -- silver.trades with a different field list on this engine than on the other four.
    event_ts    TIMESTAMP(6),
    ingest_ts   TIMESTAMP(6),
    source_lsn  BIGINT,
    PRIMARY KEY (trade_id) NOT ENFORCED
) WITH (
    -- first_row keeps the FIRST row per primary key. That is the correct semantic for
    -- this table on its own terms, a trade is an immutable execution, so a re-delivery
    -- of trade_id must be dropped, not applied, and it also fixes the gold fold's
    -- state profile. FlinkTableSource.getChangelogMode() returns insertOnly() for a
    -- first_row PK table (fluss-flink-common/.../source/FlinkTableSource.java), where a
    -- default PK table emits a full retracting changelog. Flink then plans MIN/MAX in
    -- gold as a single scalar per key instead of a retractable multiset of every value
    -- ever seen. O(keys) instead of O(rows). See gold_open_positions.sql.
    -- FIRST-WINS: a trade is an IMMUTABLE EXECUTION. The source never updates one, so
    -- the only duplicates reaching here are at-least-once CDC re-deliveries, which are
    -- byte-identical, first-wins and last-wins are equivalent for them, and first-wins
    -- is cheaper: FlinkTableSource.getChangelogMode() returns insertOnly() for a
    -- FIRST_ROW table, so the gold MIN/MAX is one scalar per key rather than a retract
    -- multiset. Last-wins belongs on mutable entities; silver.accounts is that here.
    -- NB: first_row does not support DELETE. table.delete.behavior is left UNSET so it
    -- defaults to 'ignore'; setting it to 'allow' makes TableDescriptorValidation throw.
    'table.merge-engine'              = 'first_row',
    'table.datalake.enabled'          = 'true',
    'table.datalake.freshness'        = '30s',
    -- RETENTION + LAKE MAINTENANCE (declared, not inherited).
    -- table.log.ttl defaults to 7d, so the log was already bounded, but a hot tier
    -- should hold a working set, not a week: the lake holds history. local-ttl keeps
    -- only the recent window on the tablet servers' disks; older segments live in
    -- remote storage once tiered.
    'table.log.ttl'                    = '3d',
    'table.log.local-ttl'              = '6h',
    -- BOTH DEFAULT TO FALSE. Without them the tiered Paimon tables get no compaction
    -- and no snapshot expiry EVER, unbounded small files and unbounded snapshot
    -- metadata, the one genuinely unbounded thing on the Fluss path.
    'table.datalake.auto-compaction'   = 'true',
    'table.datalake.auto-expire-snapshot' = 'true'${FLUSS_ICEBERG_OPTS}
);

-- silver.accounts. SCD2 dimension: Every version is retained, keyed
-- (account_id, source_lsn), with effective_to / is_current materialised by the atomic
-- close-out in silver_accounts.sql. Not "latest row wins", that is SCD1, and it destroys
-- the ability to answer what an account was AT THE TIME OF A TRADE.
-- Gold does not join this: country/tier are enriched at READ time, in all five engines.
CREATE TABLE IF NOT EXISTS fluss_catalog.silver.accounts (
    account_id        BIGINT,
    name              STRING,
    country           STRING,
    tier              STRING,
    source_updated_at TIMESTAMP(6),
    event_ts          TIMESTAMP(6),
    effective_from    TIMESTAMP(6),
    effective_to      TIMESTAMP(6),
    is_current        BOOLEAN,
    source_lsn        BIGINT,
    -- Raw Debezium op ('d' is filtered at the source, so this carries c/u/r) and the write
    -- time. Both were absent here and present on Delta/Iceberg, so the field lists diverged.
    op                STRING,
    commit_ts         TIMESTAMP(6),
    PRIMARY KEY (account_id, source_lsn) NOT ENFORCED
) WITH (
    'table.datalake.enabled'          = 'true',
    'table.datalake.freshness'        = '30s',
    -- RETENTION + LAKE MAINTENANCE (declared, not inherited).
    -- table.log.ttl defaults to 7d, so the log was already bounded, but a hot tier
    -- should hold a working set, not a week: the lake holds history. local-ttl keeps
    -- only the recent window on the tablet servers' disks; older segments live in
    -- remote storage once tiered.
    'table.log.ttl'                    = '3d',
    'table.log.local-ttl'              = '6h',
    -- BOTH DEFAULT TO FALSE. Without them the tiered Paimon tables get no compaction
    -- and no snapshot expiry EVER, unbounded small files and unbounded snapshot
    -- metadata, the one genuinely unbounded thing on the Fluss path.
    'table.datalake.auto-compaction'   = 'true',
    'table.datalake.auto-expire-snapshot' = 'true'${FLUSS_ICEBERG_OPTS}
);

-- gold.open_positions, net book per (account_id, symbol), tiers to Paimon.
--
-- opened_at / last_updated_at are EVENT time (from executed_at); commit_ts is
-- PROCESSING time. (commit_ts - last_updated_at) is therefore a per-row
-- end-to-end processing delay readable straight out of the table, identically in
-- all five engines, with no latency-emit chain in the loop. See snapshot-results.sh.
CREATE TABLE IF NOT EXISTS fluss_catalog.gold.open_positions (
    account_id      BIGINT,
    symbol          STRING,
    net_quantity    BIGINT,
    net_notional    DECIMAL(38,4),
    trade_count     BIGINT,
    status          STRING,
    -- country/tier are not denormalised here. They are account attributes, and a
    -- current-state position row has no defensible temporal semantic for them.
    -- Enrich at query time:  ... LEFT JOIN silver.accounts USING (account_id)
    -- LEFT always: a fill can land before its account row (independent CDC streams),
    -- and an inner join would silently drop that position from the book.
    opened_at       TIMESTAMP(6),
    last_updated_at TIMESTAMP(6),
    commit_ts       TIMESTAMP(6),
    PRIMARY KEY (account_id, symbol) NOT ENFORCED
) WITH (
    'table.datalake.enabled'          = 'true',
    'table.datalake.freshness'        = '30s',
    -- RETENTION + LAKE MAINTENANCE (declared, not inherited).
    -- table.log.ttl defaults to 7d, so the log was already bounded, but a hot tier
    -- should hold a working set, not a week: the lake holds history. local-ttl keeps
    -- only the recent window on the tablet servers' disks; older segments live in
    -- remote storage once tiered.
    'table.log.ttl'                    = '3d',
    'table.log.local-ttl'              = '6h',
    -- BOTH DEFAULT TO FALSE. Without them the tiered Paimon tables get no compaction
    -- and no snapshot expiry EVER, unbounded small files and unbounded snapshot
    -- metadata, the one genuinely unbounded thing on the Fluss path.
    'table.datalake.auto-compaction'   = 'true',
    'table.datalake.auto-expire-snapshot' = 'true'${FLUSS_ICEBERG_OPTS}
);
