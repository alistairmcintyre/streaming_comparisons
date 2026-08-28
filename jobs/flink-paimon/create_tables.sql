-- Flink SQL: create the Paimon catalog, databases and TRADES / open-positions tables.
-- Run once by submit.sh before the pipeline jobs. Safe to re-run (IF NOT EXISTS).
--
-- This file is a TEMPLATE. submit.sh runs `envsubst` over it, substituting these
-- placeholders (named WITHOUT the $-brace here, so envsubst leaves this comment
-- intact — multi-line values would otherwise spill raw SQL into the file):
--   PAIMON_S3_OPTS               catalog S3 access (endpoint+creds locally; empty on AWS)
--   PAIMON_ICEBERG_OPTS          Iceberg-compat metadata (hadoop-catalog local; Glue on AWS)
--   PAIMON_FULL_COMPACT_INTERVAL full-compaction cadence = Iceberg-view freshness knob

CREATE CATALOG paimon WITH (
    'type'      = 'paimon',
    'warehouse' = '${PAIMON_WAREHOUSE}'${PAIMON_S3_OPTS}
);

CREATE DATABASE IF NOT EXISTS paimon.bronze;
CREATE DATABASE IF NOT EXISTS paimon.silver;
CREATE DATABASE IF NOT EXISTS paimon.gold;

-- ─── Bronze trades (append-only fills) ──────────────────────────────────────
CREATE TABLE IF NOT EXISTS paimon.bronze.trades (
    op                STRING,
    trade_id          BIGINT,
    account_id        BIGINT,
    symbol            STRING,
    side              STRING,
    quantity          INT,
    price             DECIMAL(12,4),
    executed_at       TIMESTAMP(6),
    event_ts          TIMESTAMP(6),
    ingest_ts         TIMESTAMP(6),
    kafka_offset      BIGINT,
    kafka_partition   INT,
    -- strict total order across the CDC stream; kafka_offset is per-partition only
    source_lsn        BIGINT
) WITH (
    'bucket'      = '-1',
    'file.format' = 'parquet',
    -- Same explicit snapshot retention as the other three tables; bronze is the
    -- highest-commit-rate table here, so leaving it to defaults is the worst case.
    'snapshot.num-retained.min'        = '10',
    'snapshot.num-retained.max'        = '50',
    'snapshot.time-retained'           = '1h'${PAIMON_ICEBERG_OPTS}
);

-- ─── Silver trades (cleaned + deduped fills, stream-readable) ────────────────
-- PK on trade_id dedupes exact re-deliveries; each fill is a unique key.
--
-- merge-engine=first-row, NOT deduplicate. A trade is an immutable execution, so
-- first-row-wins IS the correct dedupe — and it decides the state profile of the gold
-- fold downstream. BaseDataTableSource.getChangelogMode() (verified in
-- paimon-flink-1.20-1.4.2.jar) returns insertOnly() for a FIRST_ROW table but
-- ChangelogMode.all() whenever changelog-producer != none — which is what 'input'
-- selected. The old comment here reasoned that the changelog was insert-only "by
-- construction", which was true of the CONTENT and irrelevant: Flink plans on the
-- DECLARED mode. Over a declared-retracting input it cannot un-apply MIN/MAX, so the
-- gold fold kept a per-key multiset of every value ever seen — O(total rows).
--
-- Two constraints come with it, both enforced by SchemaValidation:
--   "Only support 'none' and 'lookup' changelog-producer on FIRST_ROW merge engine"
--       -> none, which is also cheapest: a first-row table's file stream is already
--          insert-only, so no changelog files need writing at all.
--   "Do not support use sequence field on FIRST_ROW merge engine"
--       -> sequence.field dropped. It ordered duplicate trade_ids by event_ts under
--          deduplicate; under first-row the first arrival wins, and for an immutable
--          execution a genuine re-delivery is byte-identical anyway.
CREATE TABLE IF NOT EXISTS paimon.silver.trades (
    trade_id          BIGINT NOT NULL,
    account_id        BIGINT,
    symbol            STRING,
    side              STRING,
    quantity          INT,
    price             DECIMAL(12,4),
    executed_at       TIMESTAMP(6),
    event_ts          TIMESTAMP(6),
    ingest_ts         TIMESTAMP(6),
    -- the ordering key for last-wins; see sequence.field below
    source_lsn        BIGINT,
    PRIMARY KEY (trade_id) NOT ENFORCED
) WITH (
    'bucket'                           = '4',
    -- FIRST-WINS, and that is the correct model for this entity. A trade is an
    -- IMMUTABLE EXECUTION: the source never updates it (gen_trades.py is INSERT-only,
    -- and real venues model amendments and busts as NEW events carrying the original
    -- trade id, not as updates in place). So the only duplicates that reach here are
    -- at-least-once CDC RE-DELIVERIES, which are byte-identical — first-wins and
    -- last-wins are therefore equivalent, and first-wins is cheaper.
    --
    -- Cheaper for a specific reason: FIRST_ROW makes the Paimon source declare
    -- ChangelogMode.insertOnly(), so the gold GROUP BY plans MIN/MAX as one scalar per
    -- key instead of a retract multiset of every value ever seen. Last-wins would need
    -- a retracting changelog and put gold state back to O(total rows).
    --
    -- Last-wins belongs on MUTABLE entities. silver.accounts is that entity here — it
    -- takes genuine UPDATEs and is maintained as a current view.
    'merge-engine'                     = 'first-row',
    -- 'lookup', not 'none'. SchemaValidation accepts either with FIRST_ROW, but a
    -- streaming read of first-row + none fails at RUNTIME: "First row streaming reading
    -- is not supported. You can use 'lookup' or 'full-compaction'". The gold job streams
    -- this table, so 'none' silently cost us the gold job on the first live run —
    -- schema-valid, runtime-invalid (DEPLOY_LOG #85).
    'changelog-producer'               = 'lookup',
    'file.format'                      = 'parquet',
    'compaction.optimization-interval' = '${PAIMON_FULL_COMPACT_INTERVAL}',
    -- SNAPSHOT RETENTION, declared rather than inherited. Paimon expires snapshots in
    -- the writer, so no scheduled job is needed — but the bound was left to defaults.
    -- A streaming writer committing every 10s creates ~8,600 snapshots/day; stating the
    -- retention explicitly is what makes metadata growth a known quantity instead of a
    -- property of whichever Paimon version happens to be in the image.
    'snapshot.num-retained.min'        = '10',
    'snapshot.num-retained.max'        = '50',
    'snapshot.time-retained'           = '1h'${PAIMON_ICEBERG_OPTS}
);

-- ─── Silver accounts (SCD1 current-view dimension, HARD deletes) ─────────────
CREATE TABLE IF NOT EXISTS paimon.silver.accounts (
    account_id        BIGINT NOT NULL,
    name              STRING,
    country           STRING,
    tier              STRING,
    source_updated_at TIMESTAMP(6),
    event_ts          TIMESTAMP(6),
    event_date        DATE,
    ingest_ts         TIMESTAMP(6),
    commit_ts         TIMESTAMP(6),
    row_kind          STRING,
    PRIMARY KEY (account_id) NOT ENFORCED
) WITH (
    'bucket'                           = '1',
    'merge-engine'                     = 'deduplicate',
    'sequence.field'                   = 'event_ts',
    'rowkind.field'                    = 'row_kind',
    'changelog-producer'               = 'lookup',
    'file.format'                      = 'parquet',
    'compaction.optimization-interval' = '${PAIMON_FULL_COMPACT_INTERVAL}',
    -- SNAPSHOT RETENTION, declared rather than inherited. Paimon expires snapshots in
    -- the writer, so no scheduled job is needed — but the bound was left to defaults.
    -- A streaming writer committing every 10s creates ~8,600 snapshots/day; stating the
    -- retention explicitly is what makes metadata growth a known quantity instead of a
    -- property of whichever Paimon version happens to be in the image.
    'snapshot.num-retained.min'        = '10',
    'snapshot.num-retained.max'        = '50',
    'snapshot.time-retained'           = '1h'${PAIMON_ICEBERG_OPTS}
);

-- ─── Gold open positions (net book per account+symbol) ──────────────────────
-- Maintained by Flink's streaming SUM ... GROUP BY over silver.trades —
-- retraction-aware, exactly-once from Flink state; upserted into this PK table.
--
-- opened_at / last_updated_at are EVENT time (from executed_at); commit_ts is
-- PROCESSING time, so (commit_ts - last_updated_at) is a per-row processing delay
-- readable straight out of the table, computed identically in all five engines.
-- See snapshot-results.sh, which reports its percentiles as the headline metric.
CREATE TABLE IF NOT EXISTS paimon.gold.open_positions (
    account_id      BIGINT NOT NULL,
    symbol          STRING NOT NULL,
    net_quantity    BIGINT,
    net_notional    DECIMAL(38,4),
    trade_count     BIGINT,
    status          STRING,
    -- country/tier are NOT denormalised here. They are account attributes, and a
    -- current-state position row has no defensible temporal semantic for them.
    -- Enrich at query time:  ... LEFT JOIN silver.accounts USING (account_id)
    -- LEFT always: a fill can land before its account row (independent CDC streams),
    -- and an inner join would silently drop that position from the book.
    opened_at       TIMESTAMP(6),
    last_updated_at TIMESTAMP(6),
    commit_ts       TIMESTAMP(6),
    PRIMARY KEY (account_id, symbol) NOT ENFORCED
) WITH (
    'bucket'                           = '4',
    'merge-engine'                     = 'deduplicate',
    'file.format'                      = 'parquet',
    'compaction.optimization-interval' = '${PAIMON_FULL_COMPACT_INTERVAL}',
    -- SNAPSHOT RETENTION, declared rather than inherited. Paimon expires snapshots in
    -- the writer, so no scheduled job is needed — but the bound was left to defaults.
    -- A streaming writer committing every 10s creates ~8,600 snapshots/day; stating the
    -- retention explicitly is what makes metadata growth a known quantity instead of a
    -- property of whichever Paimon version happens to be in the image.
    'snapshot.num-retained.min'        = '10',
    'snapshot.num-retained.max'        = '50',
    'snapshot.time-retained'           = '1h'${PAIMON_ICEBERG_OPTS}
);
