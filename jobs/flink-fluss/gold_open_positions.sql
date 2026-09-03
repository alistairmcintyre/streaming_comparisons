-- Fluss silver.trades → Fluss gold.open_positions (the native fold, on Fluss).
-- Identical shape to the flink-paimon gold, streaming SUM ... GROUP BY over the
-- Fluss changelog, exactly-once from Flink state, upserted into the PK book.
-- Both tables are datalake-enabled, so tiering mirrors them to Paimon.
--
-- No dimension join. This previously LEFT JOINed silver.accounts to stamp country/tier
-- onto the book, which cost a full second copy of the book in join state (the probe
-- side of a regular streaming join is materialised too) plus the accounts dimension, 
-- AND, because a regular join PROPAGATES, every accounts UPDATE retracted AND re-emitted
-- every position for that account. gen_accounts.py trickles those all run long, so gold
-- writes were partly driven by dimension churn rather than by trades, and the Spark
-- golds (which snapshot the dimension instead) did not behave the same way. country/tier
-- now come from a LEFT JOIN to silver.accounts at query time, identically everywhere.
--
-- TIMESTAMP SEMANTICS (uniform across all five engines):
--   opened_at       = MIN(executed_at), the fill that first established the position
--   last_updated_at = MAX(executed_at), the newest fill affecting the book
--   commit_ts       = processing time when the gold row is produced
-- so (commit_ts - last_updated_at) is a per-row processing delay readable straight
-- out of the table. See snapshot-results.sh, which reports its percentiles.
--
-- opened_at is deliberately MIN over the position's whole history, not "reset when
-- the position goes flat and reopens". Reset-on-flat is expressible in the Spark
-- MERGE (CASE WHEN t.net_quantity = 0 ...) but not as a pure Flink fold, so adopting
-- it would make Flink and Spark golds hold different values for the same input and
-- break the cross-engine reconciliation. Uniformity wins; see README.
--
-- STATE PROFILE, bounded, because silver.trades is insert-only.
-- MIN and MAX cannot be un-applied the way SUM can, so over a RETRACTING input Flink
-- keeps a per-key multiset of every value ever seen: O(total rows). Over an INSERT-ONLY
-- input it keeps one scalar per key: O(distinct account x symbol), bounded by the size
-- of the book rather than by uptime.
--
-- silver.trades declares insert-only because it uses the first_row/first-row merge
-- engine, which is correct on its own terms: a trade is an immutable execution, so the
-- only duplicates are at-least-once CDC re-deliveries and those are byte-identical.
-- Last-wins would be the right model for a MUTABLE entity (silver.accounts) but on
-- this table it would buy nothing and cost a retracting changelog.
--
-- Sink-side aggregation (stateless projection into an aggregation merge engine) was
-- considered as the way to bound state under last-wins. It cannot work: verified
-- against the implementations, Fluss FieldSumAgg has no retract method at all and
-- Paimon's min/max cannot retract. Sink aggregation is only sound over insert-only
-- input, which is what we have, so the simpler stateful fold is fine.
--
-- State TTL is not an option. Expiring a key drops its running SUM, so a dormant
-- account's next trade would rebuild net_quantity from zero and corrupt the book.

SET 'execution.checkpointing.interval' = '10 s';
SET 'execution.checkpointing.mode'     = 'EXACTLY_ONCE';
-- Without these two the job checkpoints into the JobManager heap, which caps a single
-- checkpoint at 5MB and failed 14 times on the 2026-09-01 run. The Paimon jobs have had
-- both since they were written; these did not. See 70-flink-fluss.yaml.
SET 'state.backend'                    = 'rocksdb';
SET 'state.checkpoints.dir'            = '${FLINK_CHECKPOINT_BASE}/gold_open_positions_fluss/';
-- UTC, EXPLICITLY. UNIX_TIMESTAMP(string) parses in the session time zone while the
-- executed_at strings are UTC, so an unpinned zone silently offsets every latency sample
-- by the host's zone. Pinning it costs nothing and removes the variable.
SET 'table.local-time-zone'            = 'UTC';

CREATE CATALOG fluss_catalog WITH (
    'type'              = 'fluss',
    'bootstrap.servers' = '${FLUSS_BOOTSTRAP}'
);


CREATE TEMPORARY VIEW gold_book AS
SELECT
    p.account_id,
    p.symbol,
    p.net_quantity,
    p.net_notional,
    p.trade_count,
    CASE WHEN p.net_quantity <> 0 THEN 'OPEN' ELSE 'CLOSED' END        AS status,
    p.opened_at,
    p.last_updated_at,
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP(6))                            AS commit_ts
FROM (
    SELECT
        account_id,
        symbol,
        SUM(CASE WHEN side = 'BUY' THEN CAST(quantity AS BIGINT) ELSE -CAST(quantity AS BIGINT) END) AS net_quantity,
        SUM(CASE WHEN side = 'BUY' THEN quantity * price ELSE -(quantity * price) END)                AS net_notional,
        COUNT(*)                                                                                      AS trade_count,
        MIN(executed_at)                                                                              AS opened_at,
        MAX(executed_at)                                                                              AS last_updated_at
    FROM fluss_catalog.silver.trades
    GROUP BY account_id, symbol
) p;

-- Gold-hop latency. upsert-kafka, not a raw kafka sink: gold_book comes from a GROUP BY
-- and is therefore an UPDATING changelog, which an append-only sink rejects outright
--. upsert-kafka is built for exactly this. Two details it needs:
--   value.fields-include = EXCEPT_KEY, otherwise the value format also receives the key
--     column and the 'raw' format fails: "only supports single physical column".
--   the exporter must tolerate NULL values, upsert-kafka writes a tombstone on
--     retraction, and json.loads on None killed the whole consumer loop.
-- Sampled on account_id (the fold key); a gold row is an aggregate with no single trade.
CREATE TEMPORARY TABLE latency_sink (
    k       STRING,
    `value` STRING,
    PRIMARY KEY (k) NOT ENFORCED
) WITH (
    'connector'                    = 'upsert-kafka',
    'topic'                        = 'pipeline_latency',
    'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP}',
    'key.format'                   = 'raw',
    'value.format'                 = 'raw',
    'value.fields-include'         = 'EXCEPT_KEY'
);
EXECUTE STATEMENT SET
BEGIN

INSERT INTO fluss_catalog.gold.open_positions SELECT * FROM gold_book;

INSERT INTO latency_sink
SELECT CAST(account_id AS STRING),
    '{"pipeline":"fluss-gold","executed_at_ms":'
    -- Milliseconds on BOTH terms. DATE_FORMAT to seconds dropped the ms field and
    -- UNIX_TIMESTAMP() truncates to the second, so every sample carried up to a
    -- second of error at each end while Spark emitted full ms precision.
    || CAST(UNIX_TIMESTAMP(DATE_FORMAT(last_updated_at, 'yyyy-MM-dd HH:mm:ss')) * 1000
          + CAST(DATE_FORMAT(last_updated_at, 'SSS') AS BIGINT) AS STRING)
    || ',"ingest_ts_ms":' || CAST(
           UNIX_TIMESTAMP(DATE_FORMAT(CURRENT_ROW_TIMESTAMP(), 'yyyy-MM-dd HH:mm:ss')) * 1000
         + CAST(DATE_FORMAT(CURRENT_ROW_TIMESTAMP(), 'SSS') AS BIGINT) AS STRING)
    || ',"sample_kind":"flink_record"}'
FROM gold_book
WHERE MOD(account_id, 97) = 0;

END;
