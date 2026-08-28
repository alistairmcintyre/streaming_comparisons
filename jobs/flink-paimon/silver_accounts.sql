-- Kafka accounts (Debezium) → paimon silver.accounts as SCD2, with an ATOMIC CLOSE-OUT.
--
-- Every version is retained and its validity is MATERIALISED: when version N+1 arrives
-- this job writes TWO rows — the new version (effective_to NULL, is_current TRUE) and
-- version N again with effective_to set and is_current FALSE. The PK is
-- (account_id, source_lsn), so rewriting version N merges onto the existing row rather
-- than duplicating it. Both INSERTs share one STATEMENT SET: one job, one source read.
--
-- ORDER BY PROCTIME(), not event time, and that choice is load-bearing. A rowtime OVER
-- window cannot emit a row until the WATERMARK passes it, and the watermark only advances
-- as further events arrive. On a low-volume dimension that means a status change on day 1
-- would not reach silver until the NEXT change arrived — days later. Processing time
-- emits immediately and drops nothing.
--
-- The correctness condition therefore shifts from "arrives within the watermark" to
-- "arrival order matches LSN order", which holds for a single Debezium connector on one
-- replication slot feeding a topic keyed by account_id. It is GUARDED, not assumed: the
-- close-out only fires when source_lsn > prev_lsn, so an out-of-order delivery is skipped
-- instead of silently writing a validity range that runs backwards.
--
-- DELETES are kept as a version with row_kind '-D' rather than removed — a closed account
-- must stay joinable for trades that happened while it was open.
--
-- TEMPLATE: submit.sh runs envsubst.

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
    ts_ms    BIGINT,
    proc AS PROCTIME()
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = 'app.public.accounts',
    'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP}',
    'properties.group.id'          = 'flink-paimon-silver-accounts',
    'scan.startup.mode'            = 'earliest-offset',
    'format'                       = 'json',
    'json.ignore-parse-errors'     = 'false'${KAFKA_EXTRA_OPTS}
);

-- Each change carried alongside its PREDECESSOR. Every attribute is LAGged because the
-- close-out rewrites the whole previous row: Fluss has no partial-update merge engine, and
-- doing it identically in both engines keeps them comparable.
CREATE TEMPORARY VIEW acct_changes AS
SELECT
    COALESCE(`after`.account_id, `before`.account_id)                   AS account_id,
    `after`.name                                                        AS name,
    `after`.country                                                     AS country,
    `after`.tier                                                        AS tier,
    TO_TIMESTAMP(COALESCE(`after`.updated_at, `before`.updated_at), 'yyyy-MM-dd''T''HH:mm:ss.SSSSSS''Z''') AS source_updated_at,
    TO_TIMESTAMP_LTZ(`source`.ts_ms, 3)                                 AS event_ts,
    CAST(TO_TIMESTAMP_LTZ(`source`.ts_ms, 3) AS DATE)                   AS event_date,
    CASE WHEN op = 'd' THEN '-D' ELSE '+I' END                          AS row_kind,
    TO_TIMESTAMP_LTZ(`source`.ts_ms, 3)                                 AS effective_from,
    `source`.lsn                                                        AS source_lsn,
    LAG(`after`.name)    OVER w                                         AS prev_name,
    LAG(`after`.country) OVER w                                         AS prev_country,
    LAG(`after`.tier)    OVER w                                         AS prev_tier,
    LAG(TO_TIMESTAMP(COALESCE(`after`.updated_at, `before`.updated_at), 'yyyy-MM-dd''T''HH:mm:ss.SSSSSS''Z''')) OVER w AS prev_source_updated_at,
    LAG(TO_TIMESTAMP_LTZ(`source`.ts_ms, 3)) OVER w                     AS prev_effective_from,
    LAG(CASE WHEN op = 'd' THEN '-D' ELSE '+I' END) OVER w              AS prev_row_kind,
    LAG(`source`.lsn)    OVER w                                         AS prev_lsn
FROM kafka_accounts_src
WHERE op IS NOT NULL
WINDOW w AS (PARTITION BY COALESCE(`after`.account_id, `before`.account_id) ORDER BY proc);

EXECUTE STATEMENT SET
BEGIN

INSERT INTO paimon.silver.accounts
SELECT account_id, name, country, tier, source_updated_at, event_ts, event_date,
       CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, row_kind,
       
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

INSERT INTO paimon.silver.accounts
SELECT account_id, prev_name, prev_country, prev_tier, prev_source_updated_at,
       event_ts, event_date, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, prev_row_kind,
       prev_effective_from, effective_from, FALSE, prev_lsn
FROM acct_changes
WHERE prev_lsn IS NOT NULL AND source_lsn > prev_lsn;

END;
