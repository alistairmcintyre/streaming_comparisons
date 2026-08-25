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
    price             DOUBLE,
    executed_at       TIMESTAMP(6),
    event_ts          TIMESTAMP(6),
    ingest_ts         TIMESTAMP(6),
    kafka_offset      BIGINT,
    kafka_partition   INT
) WITH (
    'bucket'      = '-1',
    'file.format' = 'parquet'${PAIMON_ICEBERG_OPTS}
);

-- ─── Silver trades (cleaned + deduped fills, stream-readable) ────────────────
-- PK on trade_id dedupes exact re-deliveries; each fill is a unique key so the
-- changelog is insert-only → changelog-producer=input (cheapest).
CREATE TABLE IF NOT EXISTS paimon.silver.trades (
    trade_id          BIGINT NOT NULL,
    account_id        BIGINT,
    symbol            STRING,
    side              STRING,
    quantity          INT,
    price             DOUBLE,
    executed_at       TIMESTAMP(6),
    event_ts          TIMESTAMP(6),
    ingest_ts         TIMESTAMP(6),
    PRIMARY KEY (trade_id) NOT ENFORCED
) WITH (
    'bucket'                           = '4',
    'merge-engine'                     = 'deduplicate',
    'sequence.field'                   = 'event_ts',
    'changelog-producer'               = 'input',
    'file.format'                      = 'parquet',
    'compaction.optimization-interval' = '${PAIMON_FULL_COMPACT_INTERVAL}'${PAIMON_ICEBERG_OPTS}
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
    'compaction.optimization-interval' = '${PAIMON_FULL_COMPACT_INTERVAL}'${PAIMON_ICEBERG_OPTS}
);

-- ─── Gold open positions (net book per account+symbol) ──────────────────────
-- Maintained by Flink's streaming SUM ... GROUP BY over silver.trades —
-- retraction-aware, exactly-once from Flink state; upserted into this PK table.
CREATE TABLE IF NOT EXISTS paimon.gold.open_positions (
    account_id    BIGINT NOT NULL,
    symbol        STRING NOT NULL,
    net_quantity  BIGINT,
    net_notional  DOUBLE,
    trade_count   BIGINT,
    PRIMARY KEY (account_id, symbol) NOT ENFORCED
) WITH (
    'bucket'                           = '4',
    'merge-engine'                     = 'deduplicate',
    'file.format'                      = 'parquet',
    'compaction.optimization-interval' = '${PAIMON_FULL_COMPACT_INTERVAL}'${PAIMON_ICEBERG_OPTS}
);
