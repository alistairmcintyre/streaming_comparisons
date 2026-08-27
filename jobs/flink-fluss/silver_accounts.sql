-- Kafka accounts (Debezium) → Fluss silver.accounts (SCD1 dimension).
-- Current view per account_id, latest wins. Gold joins this for country/tier so the
-- Fluss fold does the same work as the other four engines' folds.
--
-- DELETES: Paimon carries hard deletes via 'rowkind.field'; Fluss has no equivalent,
-- so a deleted account is filtered out here and its dimension row simply stops being
-- refreshed (last-known country/tier persists). The generator never deletes accounts,
-- so this does not affect the benchmark — noted so the divergence isn't mistaken for
-- a bug when comparing silver.accounts row counts across engines.
--
-- Run in the Fluss Flink SQL client (see README). TEMPLATE: submit.sh runs envsubst.

SET 'execution.checkpointing.interval' = '10 s';
SET 'execution.checkpointing.mode'     = 'EXACTLY_ONCE';

CREATE CATALOG fluss_catalog WITH (
    'type'              = 'fluss',
    'bootstrap.servers' = '${FLUSS_BOOTSTRAP}'
);

CREATE TEMPORARY TABLE kafka_accounts_src (
    op       STRING,
    `before` ROW<account_id BIGINT, name STRING, country STRING, tier STRING, updated_at STRING>,
    `after`  ROW<account_id BIGINT, name STRING, country STRING, tier STRING, updated_at STRING>,
    `source` ROW<ts_ms BIGINT>,
    ts_ms    BIGINT
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = 'app.public.accounts',
    'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP}',
    'properties.group.id'          = 'flink-fluss-silver-accounts',
    'scan.startup.mode'            = 'earliest-offset',
    'format'                       = 'json',
    'json.ignore-parse-errors'     = 'false'${KAFKA_EXTRA_OPTS}
);

INSERT INTO fluss_catalog.silver.accounts
SELECT
    `after`.account_id,
    `after`.name,
    `after`.country,
    `after`.tier,
    TO_TIMESTAMP(`after`.updated_at, 'yyyy-MM-dd''T''HH:mm:ss.SSSSSS''Z''')
FROM kafka_accounts_src
WHERE op <> 'd' AND `after`.account_id IS NOT NULL;
