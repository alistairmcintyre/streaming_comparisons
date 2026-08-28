-- Flink SQL: paimon silver.trades → paimon gold.open_positions
--
-- The native fold: a streaming SUM ... GROUP BY over silver.trades' changelog.
-- Flink keeps the running net per (account_id, symbol) in checkpointed state and
-- emits retractions, so the result is exactly-once with NO hand-written MERGE and
-- NO txn/idempotency trick — the PK gold table just upserts the current aggregate.
--   BUY  -> +quantity      SELL -> -quantity
-- NO dimension join. This previously LEFT JOINed silver.accounts to stamp country/tier
-- onto the book, which cost a full second copy of the book in join state (a regular
-- streaming join materialises BOTH inputs) plus the accounts dimension — and, because a
-- regular join PROPAGATES, every accounts UPDATE retracted and re-emitted every position
-- for that account, while the Spark golds snapshotting the dimension did not. country/
-- tier now come from a LEFT JOIN to silver.accounts at query time, identically in all
-- five engines.
--
-- TIMESTAMP SEMANTICS (uniform across all five engines):
--   opened_at       = MIN(executed_at) — the fill that first established the position
--   last_updated_at = MAX(executed_at) — the newest fill affecting the book
--   commit_ts       = processing time when the gold row is produced
-- so (commit_ts - last_updated_at) is a per-row processing delay readable straight
-- out of the table. See snapshot-results.sh, which reports its percentiles.
--
-- opened_at is MIN over the position's whole history, NOT "reset when the position
-- goes flat and reopens". Reset-on-flat is expressible in the Spark MERGE but not as
-- a pure Flink fold, so adopting it would make the two engine families hold
-- different values for identical input and break cross-engine reconciliation.
--
-- STATE PROFILE — bounded, because silver.trades is insert-only.
-- MIN and MAX cannot be un-applied the way SUM can, so over a RETRACTING input Flink
-- keeps a per-key multiset of every value ever seen: O(total rows). Over an INSERT-ONLY
-- input it keeps one scalar per key: O(distinct account x symbol), bounded by the size
-- of the book rather than by uptime.
--
-- silver.trades declares insert-only because it uses the first_row/first-row merge
-- engine, which is correct on its own terms: a trade is an immutable execution, so the
-- only duplicates are at-least-once CDC re-deliveries and those are byte-identical.
-- Last-wins would be the right model for a MUTABLE entity — silver.accounts — but on
-- this table it would buy nothing and cost a retracting changelog.
--
-- Sink-side aggregation (stateless projection into an aggregation merge engine) was
-- considered as the way to bound state under last-wins. It cannot work: verified
-- against the implementations, Fluss FieldSumAgg has no retract method at all and
-- Paimon's min/max cannot retract. Sink aggregation is only sound over insert-only
-- input — which is what we have, so the simpler stateful fold is fine.
--
-- State TTL is NOT an option. Expiring a key drops its running SUM, so a dormant
-- account's next trade would rebuild net_quantity from zero and corrupt the book.

SET 'execution.runtime-mode'            = 'streaming';
SET 'execution.checkpointing.interval'  = '10 s';
SET 'execution.checkpointing.mode'      = 'EXACTLY_ONCE';
SET 'execution.checkpointing.timeout'   = '60 s';
SET 'state.backend'                     = 'rocksdb';
SET 'state.checkpoints.dir'             = '${FLINK_CHECKPOINT_BASE}/gold_open_positions_paimon/';
SET 'parallelism.default'               = '1';

CREATE CATALOG paimon WITH (
    'type'      = 'paimon',
    'warehouse' = '${PAIMON_WAREHOUSE}'${PAIMON_S3_OPTS}
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
    FROM paimon.silver.trades
        /*+ OPTIONS('scan.mode' = 'latest-full') */
    GROUP BY account_id, symbol
) p;

-- Gold-hop latency. upsert-kafka, NOT a raw kafka sink: gold_book comes from a GROUP BY
-- and is therefore an UPDATING changelog, which an append-only sink rejects outright
-- (DEPLOY_LOG #80). upsert-kafka is built for exactly this. Two details it needs:
--   value.fields-include = EXCEPT_KEY — otherwise the value format also receives the key
--     column and the 'raw' format fails: "only supports single physical column".
--   the exporter must tolerate NULL values — upsert-kafka writes a tombstone on
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

INSERT INTO paimon.gold.open_positions SELECT * FROM gold_book;

INSERT INTO latency_sink
SELECT CAST(account_id AS STRING),
    '{"pipeline":"paimon-gold","executed_at_ms":'
    || CAST(UNIX_TIMESTAMP(DATE_FORMAT(last_updated_at, 'yyyy-MM-dd HH:mm:ss')) * 1000 AS STRING)
    || ',"ingest_ts_ms":' || CAST(UNIX_TIMESTAMP() * 1000 AS STRING) || '}'
FROM gold_book
WHERE MOD(account_id, 97) = 0;

END;
