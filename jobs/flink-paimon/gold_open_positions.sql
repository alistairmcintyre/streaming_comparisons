-- Flink SQL: paimon silver.trades → paimon gold.open_positions
--
-- The native fold: a streaming SUM ... GROUP BY over silver.trades' changelog.
-- Flink keeps the running net per (account_id, symbol) in checkpointed state and
-- emits retractions, so the result is exactly-once with NO hand-written MERGE and
-- NO txn/idempotency trick — the PK gold table just upserts the current aggregate.
--   BUY  -> +quantity      SELL -> -quantity
-- (Enrichment with account country/tier via a lookup join is a follow-up; this is
-- the pure fold.) TEMPLATE: submit.sh runs envsubst.

SET 'execution.runtime-mode'            = 'streaming';
SET 'execution.checkpointing.interval'  = '10 s';
SET 'execution.checkpointing.mode'      = 'EXACTLY_ONCE';
SET 'execution.checkpointing.timeout'   = '60 s';
SET 'state.backend'                     = 'rocksdb';
SET 'state.checkpoints.dir'             = '${FLINK_CHECKPOINT_BASE}/gold_open_positions_paimon/';
SET 'parallelism.default'               = '1';

CREATE CATALOG paimon WITH (
    'type'      = 'paimon',
    'warehouse' = '${PAIMON_WAREHOUSE}'${PAIMON_S3_OPTS}
);

INSERT INTO paimon.gold.open_positions
SELECT
    account_id,
    symbol,
    SUM(CASE WHEN side = 'BUY' THEN CAST(quantity AS BIGINT) ELSE -CAST(quantity AS BIGINT) END) AS net_quantity,
    SUM(CASE WHEN side = 'BUY' THEN quantity * price ELSE -(quantity * price) END)                AS net_notional,
    COUNT(*)                                                                                       AS trade_count
FROM paimon.silver.trades
    /*+ OPTIONS('scan.mode' = 'latest-full') */
GROUP BY account_id, symbol;
