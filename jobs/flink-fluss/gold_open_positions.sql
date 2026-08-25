-- Fluss silver trades → Fluss gold open_positions (the native fold, on Fluss).
-- Identical shape to the flink-paimon gold — streaming SUM ... GROUP BY over the
-- Fluss changelog, exactly-once from Flink state, upserted into the PK book.
-- Both tables are datalake-enabled, so the tiering service mirrors them to Paimon.
-- Run in the Fluss Flink SQL client (see README).

SET 'execution.checkpointing.interval' = '10 s';
SET 'execution.checkpointing.mode'     = 'EXACTLY_ONCE';

CREATE CATALOG fluss_catalog WITH (
    'type'              = 'fluss',
    'bootstrap.servers' = '${FLUSS_BOOTSTRAP}'
);

INSERT INTO fluss_catalog.trades_db.open_positions
SELECT
    account_id,
    symbol,
    SUM(CASE WHEN side = 'BUY' THEN CAST(quantity AS BIGINT) ELSE -CAST(quantity AS BIGINT) END) AS net_quantity,
    SUM(CASE WHEN side = 'BUY' THEN quantity * price ELSE -(quantity * price) END)                AS net_notional,
    COUNT(*)                                                                                       AS trade_count
FROM fluss_catalog.trades_db.trades
GROUP BY account_id, symbol;
