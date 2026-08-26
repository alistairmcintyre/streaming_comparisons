-- Kafka (Debezium trades) → Fluss hot table trades_db.trades.
-- Fluss is the hot tier; the PK table dedupes re-deliveries on trade_id and
-- serves sub-second reads. Run in the Fluss Flink SQL client (see README).

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
                 quantity INT, price DECIMAL(12,4), executed_at STRING>,
    `source` ROW<ts_ms BIGINT>,
    ts_ms    BIGINT
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = 'app.public.trades',
    'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP}',
    'properties.group.id'          = 'flink-fluss-bronze-trades',
    'scan.startup.mode'            = 'earliest-offset',
    'format'                       = 'json',
    'json.ignore-parse-errors'     = 'true'${KAFKA_EXTRA_OPTS}
);

INSERT INTO fluss_catalog.trades_db.trades
SELECT
    `after`.trade_id,
    `after`.account_id,
    `after`.symbol,
    `after`.side,
    `after`.quantity,
    `after`.price,
    TO_TIMESTAMP(`after`.executed_at, 'yyyy-MM-dd''T''HH:mm:ss.SSSSSS''Z''')
FROM kafka_trades_src
WHERE `after`.trade_id IS NOT NULL;
