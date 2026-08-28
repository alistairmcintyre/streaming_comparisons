-- Flink SQL: Kafka accounts (Debezium) → paimon silver.accounts (SCD1 dimension)
-- Current-view per account_id, latest wins; HARD deletes via rowkind.field.
-- Reads Kafka directly (the dimension has no separate bronze landing). It's
-- batch-read downstream by gold for enrichment. TEMPLATE: submit.sh runs envsubst.

SET 'execution.runtime-mode'            = 'streaming';
SET 'execution.checkpointing.interval'  = '10 s';
SET 'execution.checkpointing.mode'      = 'EXACTLY_ONCE';
SET 'execution.checkpointing.timeout'   = '60 s';
SET 'state.backend'                     = 'rocksdb';
SET 'state.checkpoints.dir'             = '${FLINK_CHECKPOINT_BASE}/silver_accounts_paimon/';
SET 'parallelism.default'               = '1';

CREATE CATALOG paimon WITH (
    'type'      = 'paimon',
    'warehouse' = '${PAIMON_WAREHOUSE}'${PAIMON_S3_OPTS}
);

CREATE TEMPORARY TABLE kafka_accounts_src (
    op       STRING,
    `before` ROW<account_id BIGINT, name STRING, country STRING, tier STRING, updated_at STRING>,
    `after`  ROW<account_id BIGINT, name STRING, country STRING, tier STRING, updated_at STRING>,
    `source` ROW<ts_ms BIGINT, db STRING, `table` STRING, lsn BIGINT>,
    ts_ms    BIGINT
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = 'app.public.accounts',
    'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP}',
    'properties.group.id'          = 'flink-paimon-silver-accounts',
    'scan.startup.mode'            = 'earliest-offset',
    'format'                       = 'json',
    'json.ignore-parse-errors'     = 'false'${KAFKA_EXTRA_OPTS}
);

INSERT INTO paimon.silver.accounts
SELECT
    COALESCE(`after`.account_id, `before`.account_id)                    AS account_id,
    `after`.name                                                         AS name,
    `after`.country                                                      AS country,
    `after`.tier                                                         AS tier,
    TO_TIMESTAMP(COALESCE(`after`.updated_at, `before`.updated_at), 'yyyy-MM-dd''T''HH:mm:ss.SSSSSS''Z''') AS source_updated_at,
    TO_TIMESTAMP_LTZ(`source`.ts_ms, 3)                                 AS event_ts,
    CAST(TO_TIMESTAMP_LTZ(`source`.ts_ms, 3) AS DATE)                   AS event_date,
    CURRENT_TIMESTAMP                                                    AS ingest_ts,
    CURRENT_TIMESTAMP                                                    AS commit_ts,
    CASE WHEN op = 'd' THEN '-D' ELSE '+I' END                          AS row_kind,
    TO_TIMESTAMP_LTZ(`source`.ts_ms, 3)                                 AS effective_from,
    `source`.lsn                                                        AS source_lsn
FROM kafka_accounts_src
WHERE op IS NOT NULL;
