-- Flink SQL: Kafka (Debezium envelope JSON) → paimon bronze.trades
-- Bronze is the raw, append-only landing table. TEMPLATE: submit.sh runs envsubst.

SET 'execution.runtime-mode'            = 'streaming';
SET 'execution.checkpointing.interval'  = '10 s';
SET 'execution.checkpointing.mode'      = 'EXACTLY_ONCE';
SET 'execution.checkpointing.timeout'   = '60 s';
SET 'state.backend'                     = 'rocksdb';
SET 'state.checkpoints.dir'             = '${FLINK_CHECKPOINT_BASE}/bronze_trades_paimon/';
SET 'parallelism.default'               = '1';

CREATE CATALOG paimon WITH (
    'type'      = 'paimon',
    'warehouse' = '${PAIMON_WAREHOUSE}'${PAIMON_S3_OPTS}
);

CREATE TEMPORARY TABLE kafka_trades_src (
    op       STRING,
    `after`  ROW<
        trade_id    BIGINT,
        account_id  BIGINT,
        symbol      STRING,
        side        STRING,
        quantity    INT,
        price       STRING,          -- exact decimal as STRING (decimal.handling.mode=string)
        executed_at STRING
    >,
    `source` ROW<
        ts_ms   BIGINT,
        db      STRING,
        `table` STRING
    >,
    ts_ms    BIGINT
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = 'app.public.trades',
    'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP}',
    'properties.group.id'          = 'flink-paimon-bronze-trades',
    'scan.startup.mode'            = 'earliest-offset',
    'format'                       = 'json',
    'json.ignore-parse-errors'     = 'false'${KAFKA_EXTRA_OPTS}
);

INSERT INTO paimon.bronze.trades
SELECT
    op,
    `after`.trade_id                                                    AS trade_id,
    `after`.account_id                                                  AS account_id,
    `after`.symbol                                                      AS symbol,
    `after`.side                                                        AS side,
    `after`.quantity                                                    AS quantity,
    CAST(`after`.price AS DECIMAL(12,4))                                AS price,
    TO_TIMESTAMP(`after`.executed_at, 'yyyy-MM-dd''T''HH:mm:ss.SSSSSS''Z''') AS executed_at,
    TO_TIMESTAMP_LTZ(`source`.ts_ms, 3)                                AS event_ts,
    CURRENT_TIMESTAMP                                                   AS ingest_ts,
    CAST(NULL AS BIGINT)                                               AS kafka_offset,
    CAST(NULL AS INT)                                                  AS kafka_partition
FROM kafka_trades_src
WHERE `after`.trade_id IS NOT NULL;

-- ── Latency emit ────────────────────────────────────────────────────────────
-- Feeds the `pipeline_latency` topic that docker/latency-exporter consumes.
-- CAVEAT that matters when reading results: Flink SQL has no post-commit hook, so
-- this samples at PROCESSING time, whereas the Spark engines emit after their write
-- returns (post-commit). Flink numbers therefore EXCLUDE the sink commit while Spark
-- numbers include it — compare within a family freely, across families with care.
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

INSERT INTO latency_sink
SELECT
    '{"pipeline":"paimon-bronze","executed_at_ms":'
    || CAST(UNIX_TIMESTAMP(`after`.executed_at, 'yyyy-MM-dd''T''HH:mm:ss.SSSSSS''Z''') * 1000 AS STRING)
    || ',"ingest_ts_ms":' || CAST(UNIX_TIMESTAMP() * 1000 AS STRING) || '}'
FROM kafka_trades_src
WHERE `after`.trade_id IS NOT NULL AND MOD(`after`.trade_id, 997) = 0;
