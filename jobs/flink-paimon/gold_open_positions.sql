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
-- STATE PROFILE (this is what makes the fold safe to run for months, not hours):
-- MIN/MAX cannot be un-applied the way SUM can, so over a RETRACTING input Flink keeps a
-- per-key multiset of every value ever seen — O(total rows), unbounded.
--
-- This WAS the case here and is now fixed at the source. Reading
-- BaseDataTableSource.getChangelogMode() out of paimon-flink-1.20-1.4.2.jar: it returns
-- insertOnly() for a FIRST_ROW table, all() when changelog-producer != none, and upsert()
-- otherwise. silver.trades used changelog-producer='input' and so declared all() — a full
-- retract stream — regardless of the changelog being insert-only in content. It now uses
-- merge-engine='first-row' + changelog-producer='none' and declares insertOnly(), so Flink
-- plans MIN/MAX as a single scalar per key. All gold state is O(distinct account x symbol),
-- bounded by the size of the book rather than by uptime.
--
--
-- ACCESS PATTERN (verified against the jars in the image, not assumed):
--   * The GROUP BY is a keyed streaming aggregate, NOT a scan. GroupAggFunction
--     (flink-table-runtime-1.20.5.jar) holds the accumulator in ValueState<RowData>;
--     its constant pool references ValueState/update and no iterator, entries or
--     keys(). Per record that is one RocksDB point-get + point-put on
--     (account_id, symbol) — cost independent of key count and of rows processed.
--   * WHICH accumulator is the whole ballgame. AggFunctionFactory
--     (flink-table-planner_2.12-1.20.5.jar) branches on aggCallNeedRetractions and
--     for a TIMESTAMP picks either MinAggFunction$TimestampMinAggFunction (one value)
--     or MinWithRetractAggFunction, whose accumulator holds MapView<T, Long> — every
--     distinct timestamp ever seen for that key, with a count. Neither scans; one is
--     bounded and one is not, and the source's DECLARED changelog mode picks it.
--   * There is NO dimension join any more. When there was, it cost a second full
--     copy of the book: a regular streaming join materialises BOTH inputs
--     (StreamingJoinOperator keeps leftRecordStateView + rightRecordStateView),
--     so gold state was 2x book + accounts. Enrichment moved to query time.
--   * The source is read ONCE in full, then incrementally. Paimon's scan.mode='latest-full' is
--     "produces a snapshot upon first startup, and continue to read the latest
--     changes" (CoreOptions$StartupMode).
--     That bootstrap is unavoidable — a stateful fold has to be seeded — but it is
--     per job start, not per key or per record. It is also why retained checkpoints
--     matter (70-/92-flink-*.yaml): a restart WITHOUT a valid checkpoint re-reads the
--     whole table and rebuilds all state from scratch.
--
-- State TTL is deliberately NOT used here. Expiring a position's key would drop its
-- running SUM, so the next trade on a dormant account would rebuild net_quantity from
-- zero and silently corrupt the book. Bounded-by-key is the correct bound; TTL is not.
--
-- TEMPLATE: submit.sh runs envsubst.

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

-- Live-dashboard telemetry only. The authoritative processing delay is
-- (commit_ts - last_updated_at) read out of gold.open_positions; this feeds the
-- Grafana panel and samples at PROCESSING time (Flink SQL has no post-commit hook).
CREATE TEMPORARY TABLE latency_sink (
    `value` STRING
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = 'pipeline_latency',
    'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP}',
    'format'                       = 'raw'${KAFKA_EXTRA_OPTS}
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

-- One STATEMENT SET so the fold is computed once and shared by both sinks.
EXECUTE STATEMENT SET
BEGIN

INSERT INTO paimon.gold.open_positions SELECT * FROM gold_book;

-- Gold-hop latency. Sampled on account_id (the fold's key) rather than trade_id,
-- because a gold row is an aggregate and has no single trade to sample on.
INSERT INTO latency_sink
SELECT
    '{"pipeline":"paimon-gold","executed_at_ms":'
    || CAST(UNIX_TIMESTAMP(DATE_FORMAT(last_updated_at, 'yyyy-MM-dd HH:mm:ss')) * 1000 AS STRING)
    || ',"ingest_ts_ms":' || CAST(UNIX_TIMESTAMP() * 1000 AS STRING) || '}'
FROM gold_book
WHERE MOD(account_id, 97) = 0;

END;
