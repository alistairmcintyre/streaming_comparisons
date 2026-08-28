-- Kafka (Debezium trades) → Fluss hot table silver.trades.
-- Fluss is the hot tier; the PK table dedupes re-deliveries on trade_id and
-- serves sub-second reads. It is named `silver`, not `bronze`, because it IS the
-- cleaned deduped view — Fluss has no separate landing table to re-read, and that
-- one-hop-fewer topology is exactly what the benchmark is measuring (hops are
-- recorded per engine in results.json). Run in the Fluss Flink SQL client (see README).

SET 'execution.checkpointing.interval' = '10 s';
SET 'execution.checkpointing.mode'     = 'EXACTLY_ONCE';

CREATE CATALOG fluss_catalog WITH (
    'type'              = 'fluss',
    'bootstrap.servers' = '${FLUSS_BOOTSTRAP}'
);

-- Rendered by submit.sh: ${KAFKA_BOOTSTRAP} (kafka:9092 local / MSK on AWS) and
-- ${KAFKA_EXTRA_OPTS} (empty local / MSK IAM SASL on AWS).
CREATE TEMPORARY TABLE kafka_trades_src (
    op       STRING,
    `after`  ROW<trade_id BIGINT, account_id BIGINT, symbol STRING, side STRING,
                 quantity INT, price STRING, executed_at STRING>,   -- price: exact decimal as STRING
    -- lsn: strict TOTAL order across the CDC stream (kafka_offset is per-partition
    -- only, and valid as a tiebreaker solely because Debezium keys by PK).
    `source` ROW<ts_ms BIGINT, lsn BIGINT>,
    ts_ms    BIGINT
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = 'app.public.trades',
    'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP}',
    'properties.group.id'          = 'flink-fluss-silver-trades',
    'scan.startup.mode'            = 'earliest-offset',
    'format'                       = 'json',
    'json.ignore-parse-errors'     = 'false'${KAFKA_EXTRA_OPTS}
);

-- ── Latency emit sink ───────────────────────────────────────────────────────
-- Feeds the `pipeline_latency` topic that docker/latency-exporter consumes, which
-- drives the live Grafana dashboard. It is NOT the headline number any more: the
-- authoritative processing delay is (commit_ts - last_updated_at) read out of
-- gold.open_positions, which is uniform across engines. See snapshot-results.sh.
--
-- CAVEAT for the live dashboard: Flink SQL has no post-commit hook, so this samples
-- at PROCESSING time, whereas the Spark engines emit after their write returns
-- (post-commit). Flink numbers here EXCLUDE the sink commit while Spark numbers
-- include it — compare within a family freely, across families with care.
-- MOD 997 keeps this to ~0.1% of rows: emitting every record would add measurement
-- traffic to the very Kafka the pipelines read from.
CREATE TEMPORARY TABLE latency_sink (
    `value` STRING
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = 'pipeline_latency',
    'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP}',
    'format'                       = 'raw'${KAFKA_EXTRA_OPTS}
);

-- One STATEMENT SET, so both sinks share a single Kafka source operator. As two
-- standalone INSERTs the sql-client submits two jobs, each reading the whole topic —
-- doubling load on the very pipeline being measured.
EXECUTE STATEMENT SET
BEGIN

INSERT INTO fluss_catalog.silver.trades
SELECT
    `after`.trade_id,
    `after`.account_id,
    `after`.symbol,
    `after`.side,
    `after`.quantity,
    CAST(`after`.price AS DECIMAL(12,4)),
    TO_TIMESTAMP(`after`.executed_at, 'yyyy-MM-dd''T''HH:mm:ss.SSSSSS''Z'''),
    -- event_ts = source commit time; ingest_ts = when this pipeline wrote the row. Fluss
    -- has no bronze hop, so ingest_ts is stamped here. Both were missing entirely.
    CAST(TO_TIMESTAMP_LTZ(`source`.ts_ms, 3) AS TIMESTAMP(6)),
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP(6)),
    `source`.lsn
FROM kafka_trades_src
WHERE `after`.trade_id IS NOT NULL;

INSERT INTO latency_sink
SELECT
    '{"pipeline":"fluss-silver","executed_at_ms":'
    || CAST(UNIX_TIMESTAMP(`after`.executed_at, 'yyyy-MM-dd''T''HH:mm:ss.SSSSSS''Z''') * 1000 AS STRING)
    || ',"ingest_ts_ms":' || CAST(UNIX_TIMESTAMP() * 1000 AS STRING) || '}'
FROM kafka_trades_src
WHERE `after`.trade_id IS NOT NULL AND MOD(`after`.trade_id, 997) = 0;

END;
