-- Kafka accounts (Debezium) → Fluss silver.accounts as SCD2, with an ATOMIC CLOSE-OUT.
--
-- Every version is retained and its validity is MATERIALISED: when version N+1 arrives
-- this job writes TWO rows — the new version (effective_to NULL, is_current TRUE) and
-- version N again with effective_to set and is_current FALSE. The PK is
-- (account_id, source_lsn), so rewriting version N merges onto the existing row.
-- Both INSERTs share one STATEMENT SET: one job, one source read.
--
-- ORDER BY PROCTIME(), not event time. A rowtime OVER window cannot emit until the
-- WATERMARK passes the row, and the watermark only advances as further events arrive — so
-- on a low-volume dimension a change on day 1 would not reach silver until the NEXT change
-- arrived, days later. Processing time emits immediately and drops nothing. The condition
-- becomes "arrival order matches LSN order", true for one Debezium connector on one
-- replication slot, and GUARDED by source_lsn > prev_lsn rather than assumed.
--
-- DELETES: Fluss has no rowkind.field, so a delete is filtered at the source (see the
-- WHERE below) rather than written as a tombstone version. That is a real divergence from
-- the Paimon pipeline and is recorded as such — the generator never deletes accounts, so
-- it does not affect the benchmark, but it would matter for a source that does.
--
-- Run in the Fluss Flink SQL client. TEMPLATE: submit.sh runs envsubst.

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
    `source` ROW<ts_ms BIGINT, lsn BIGINT>,
    ts_ms    BIGINT,
    proc AS PROCTIME()
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = 'app.public.accounts',
    'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP}',
    'properties.group.id'          = 'flink-fluss-silver-accounts',
    'scan.startup.mode'            = 'earliest-offset',
    'format'                       = 'json',
    'json.ignore-parse-errors'     = 'false'${KAFKA_EXTRA_OPTS}
);

-- Each change carried with its PREDECESSOR. Every attribute is LAGged because the
-- close-out rewrites the whole previous row — Fluss has no partial-update merge engine.
CREATE TEMPORARY VIEW acct_changes AS
SELECT
    `after`.account_id                                                  AS account_id,
    `after`.name                                                        AS name,
    `after`.country                                                     AS country,
    `after`.tier                                                        AS tier,
    TO_TIMESTAMP(`after`.updated_at, 'yyyy-MM-dd''T''HH:mm:ss.SSSSSS''Z''') AS source_updated_at,
    TO_TIMESTAMP_LTZ(`source`.ts_ms, 3)                                 AS effective_from,
    `source`.lsn                                                        AS source_lsn,
    LAG(`after`.name)    OVER w                                         AS prev_name,
    LAG(`after`.country) OVER w                                         AS prev_country,
    LAG(`after`.tier)    OVER w                                         AS prev_tier,
    LAG(TO_TIMESTAMP(`after`.updated_at, 'yyyy-MM-dd''T''HH:mm:ss.SSSSSS''Z''')) OVER w AS prev_source_updated_at,
    LAG(TO_TIMESTAMP_LTZ(`source`.ts_ms, 3)) OVER w                     AS prev_effective_from,
    LAG(`source`.lsn)    OVER w                                         AS prev_lsn
FROM kafka_accounts_src
WHERE op <> 'd' AND `after`.account_id IS NOT NULL
WINDOW w AS (PARTITION BY `after`.account_id ORDER BY proc);

EXECUTE STATEMENT SET
BEGIN

INSERT INTO fluss_catalog.silver.accounts
SELECT account_id, name, country, tier, source_updated_at,
       
       effective_from,
       -- An OUT-OF-ORDER arrival is not current: it is a late historical version, valid
       -- until the row that already superseded it. Without this it would be written with
       -- is_current TRUE and the account would have TWO current rows — which fans out
       -- every join. Caught by tests/scd2-behaviour.sh; it compiles perfectly.
       CASE WHEN prev_lsn IS NULL OR source_lsn > prev_lsn
            THEN CAST(NULL AS TIMESTAMP(6)) ELSE prev_effective_from END,
       (prev_lsn IS NULL OR source_lsn > prev_lsn),
       source_lsn
FROM acct_changes
-- A re-delivery (identical lsn to the record before it) must be a NO-OP. Without this
-- guard it falls into the "not newer" branch and is emitted with is_current = FALSE — and
-- because the target is a PK table on (account_id, source_lsn) that write MERGES onto the
-- live row, leaving the account with ZERO current versions. Verified by
-- tests/scd2-behaviour.sh; the same bug in the Spark staging was found on Hudi.
WHERE prev_lsn IS NULL OR source_lsn <> prev_lsn;

INSERT INTO fluss_catalog.silver.accounts
SELECT account_id, prev_name, prev_country, prev_tier, prev_source_updated_at,
       prev_effective_from, effective_from, FALSE, prev_lsn
FROM acct_changes
WHERE prev_lsn IS NOT NULL AND source_lsn > prev_lsn;

END;
