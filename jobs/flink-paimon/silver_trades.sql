-- Flink SQL: paimon bronze.trades → paimon silver.trades
-- Cleaned + deduped fills. PK on trade_id dedupes re-deliveries in Paimon; drops
-- the Kafka envelope/offsets. Still append (immutable events, not a current-view).
-- This is where derived/enriched fields would be added — none required yet.
-- TEMPLATE: submit.sh runs envsubst.

SET 'execution.runtime-mode'            = 'streaming';
SET 'execution.checkpointing.interval'  = '10 s';
SET 'execution.checkpointing.mode'      = 'EXACTLY_ONCE';
SET 'execution.checkpointing.timeout'   = '60 s';
SET 'state.backend'                     = 'rocksdb';
SET 'state.checkpoints.dir'             = '${FLINK_CHECKPOINT_BASE}/silver_trades_paimon/';
SET 'parallelism.default'               = '1';

CREATE CATALOG paimon WITH (
    'type'      = 'paimon',
    'warehouse' = '${PAIMON_WAREHOUSE}'${PAIMON_S3_OPTS}
);

INSERT INTO paimon.silver.trades
SELECT
    trade_id, account_id, symbol, side, quantity, price, executed_at, event_ts, ingest_ts,
    source_lsn
FROM paimon.bronze.trades
    /*+ OPTIONS('scan.mode' = 'latest-full') */
WHERE trade_id IS NOT NULL;
