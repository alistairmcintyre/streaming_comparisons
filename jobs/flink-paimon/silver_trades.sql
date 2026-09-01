-- Flink SQL: paimon bronze.trades → paimon silver.trades
-- Cleaned + deduped fills. PK on trade_id dedupes re-deliveries in Paimon; drops
-- the Kafka envelope/offsets. Still append (immutable events, not a current-view).
-- This is where derived/enriched fields would be added, none required yet.
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

-- LATENCY EMIT for the bronze->silver hop. Only bronze and gold emitted before, so the
-- dashboard showed end-to-end and gold but not where time goes in the middle, on four of
-- five engines. Silver is where the dedupe happens, so it is the least useful hop to be
-- missing. Sampled on trade_id, matching the bronze emit's rate.
--
-- Two sinks in one statement set is safe HERE and was not in silver_accounts.sql: there,
-- both INSERTs targeted the same Paimon table, so two committers raced on the Iceberg
-- metadata. This writes one Paimon table and one Kafka topic, separate committers, no
-- shared file. bronze_trades.sql has always done exactly this.
CREATE TEMPORARY TABLE latency_sink (
    `value` STRING
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = 'pipeline_latency',
    'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP}',
    'format'                       = 'raw'${KAFKA_EXTRA_OPTS}
);

EXECUTE STATEMENT SET
BEGIN

INSERT INTO paimon.silver.trades
SELECT
    trade_id, account_id, symbol, side, quantity, price, executed_at, event_ts, ingest_ts,
    source_lsn
FROM paimon.bronze.trades
    /*+ OPTIONS('scan.mode' = 'latest-full') */
WHERE trade_id IS NOT NULL;

INSERT INTO latency_sink
SELECT
    '{"pipeline":"paimon-silver","executed_at_ms":'
    || CAST(UNIX_TIMESTAMP(DATE_FORMAT(executed_at, 'yyyy-MM-dd HH:mm:ss')) * 1000 AS STRING)
    || ',"ingest_ts_ms":' || CAST(UNIX_TIMESTAMP() * 1000 AS STRING)
    || ',"sample_kind":"flink_record"}'
FROM paimon.bronze.trades
    /*+ OPTIONS('scan.mode' = 'latest-full') */
WHERE trade_id IS NOT NULL AND MOD(trade_id, 997) = 0;

END;
