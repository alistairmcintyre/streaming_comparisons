-- Fluss hot-tier tables for the trades / open-positions pipeline.
-- Both are datalake-enabled → the Fluss tiering service continuously moves them
-- into Paimon, which exposes the Iceberg view for Athena/Trino/spark-iceberg.
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

CREATE DATABASE IF NOT EXISTS fluss_catalog.trades_db;

-- silver.trades — PK table (dedupes re-deliveries on trade_id), tiers to Paimon.
CREATE TABLE IF NOT EXISTS fluss_catalog.trades_db.trades (
    trade_id    BIGINT,
    account_id  BIGINT,
    symbol      STRING,
    side        STRING,
    quantity    INT,
    price       DECIMAL(12,4),
    -- TIMESTAMP(6): Iceberg's native micro precision. Paimon's Iceberg-compat
    -- REJECTS precision < 4 (TIMESTAMP(3) → the tiering commit throws
    -- "only support timestamp type with precision from 4 to 9" and no Iceberg
    -- metadata.json is ever written → the table is invisible to Athena/Spark).
    executed_at TIMESTAMP(6),
    PRIMARY KEY (trade_id) NOT ENFORCED
) WITH (
    'table.datalake.enabled'          = 'true',
    'table.datalake.freshness'        = '30s'${FLUSS_ICEBERG_OPTS}
);

-- gold.open_positions — net book per (account_id, symbol), tiers to Paimon.
CREATE TABLE IF NOT EXISTS fluss_catalog.trades_db.open_positions (
    account_id   BIGINT,
    symbol       STRING,
    net_quantity BIGINT,
    net_notional DECIMAL(38,4),
    trade_count  BIGINT,
    PRIMARY KEY (account_id, symbol) NOT ENFORCED
) WITH (
    'table.datalake.enabled'          = 'true',
    'table.datalake.freshness'        = '30s'${FLUSS_ICEBERG_OPTS}
);
