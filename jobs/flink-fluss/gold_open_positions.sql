-- Fluss silver.trades → Fluss gold.open_positions (the native fold, on Fluss).
-- Identical shape to the flink-paimon gold — streaming SUM ... GROUP BY over the
-- Fluss changelog, exactly-once from Flink state, upserted into the PK book.
-- Both tables are datalake-enabled, so tiering mirrors them to Paimon.
--
-- NO dimension join. This previously LEFT JOINed silver.accounts to stamp country/tier
-- onto the book, which cost a full second copy of the book in join state (the probe
-- side of a regular streaming join is materialised too) plus the accounts dimension —
-- and, because a regular join PROPAGATES, every accounts UPDATE retracted and re-emitted
-- every position for that account. gen_accounts.py trickles those all run long, so gold
-- writes were partly driven by dimension churn rather than by trades, and the Spark
-- golds (which snapshot the dimension instead) did not behave the same way. country/tier
-- now come from a LEFT JOIN to silver.accounts at query time, identically everywhere.
--
-- TIMESTAMP SEMANTICS (uniform across all five engines):
--   opened_at       = MIN(executed_at) — the fill that first established the position
--   last_updated_at = MAX(executed_at) — the newest fill affecting the book
--   commit_ts       = processing time when the gold row is produced
-- so (commit_ts - last_updated_at) is a per-row processing delay readable straight
-- out of the table. See snapshot-results.sh, which reports its percentiles.
--
-- opened_at is deliberately MIN over the position's whole history, NOT "reset when
-- the position goes flat and reopens". Reset-on-flat is expressible in the Spark
-- MERGE (CASE WHEN t.net_quantity = 0 ...) but NOT as a pure Flink fold, so adopting
-- it would make Flink and Spark golds hold different values for the same input and
-- break the cross-engine reconciliation. Uniformity wins; see README.
--
-- STATE PROFILE (this is what makes the fold safe to run for months, not hours):
-- MIN/MAX cannot be un-applied the way SUM can, so over a RETRACTING input Flink keeps a
-- per-key multiset of every value ever seen — O(total rows), which grows without bound.
-- silver.trades therefore declares 'table.merge-engine' = 'first_row', which makes the
-- Fluss source report ChangelogMode.insertOnly(); Flink then plans MIN/MAX as a single
-- scalar per key. All gold state is now O(distinct account x symbol) — bounded by the
-- book's size, not by how long the pipeline has been running.
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
--   * The source is read ONCE in full, then incrementally. Fluss's default scan.startup.mode is FULL: "performs a full snapshot on
--     the table upon first startup, and continue to read the latest changelog"
--     (FlinkConnectorOptions.ScanStartupMode).
--     That bootstrap is unavoidable — a stateful fold has to be seeded — but it is
--     per job start, not per key or per record. It is also why retained checkpoints
--     matter (70-/92-flink-*.yaml): a restart WITHOUT a valid checkpoint re-reads the
--     whole table and rebuilds all state from scratch.
--
-- State TTL is deliberately NOT used here. Expiring a position's key would drop its
-- running SUM, so the next trade on a dormant account would rebuild net_quantity from
-- zero and silently corrupt the book. Bounded-by-key is the correct bound; TTL is not.
--
-- Run in the Fluss Flink SQL client (see README). TEMPLATE: submit.sh runs envsubst.

SET 'execution.checkpointing.interval' = '10 s';
SET 'execution.checkpointing.mode'     = 'EXACTLY_ONCE';

CREATE CATALOG fluss_catalog WITH (
    'type'              = 'fluss',
    'bootstrap.servers' = '${FLUSS_BOOTSTRAP}'
);

CREATE TEMPORARY TABLE latency_sink (
    `value` STRING
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = 'pipeline_latency',
    'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP}',
    'format'                       = 'raw'${KAFKA_EXTRA_OPTS}
);

-- The fold feeds both the gold table and the live-dashboard latency emit from one
-- STATEMENT SET, so the aggregate is computed once and the telemetry costs nothing
-- beyond the emit itself.
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

EXECUTE STATEMENT SET
BEGIN

INSERT INTO fluss_catalog.gold.open_positions SELECT * FROM gold_book;

-- Gold-hop latency. Sampled on account_id (the fold's key) rather than trade_id,
-- because a gold row is an aggregate and has no single trade to sample on.
INSERT INTO latency_sink
SELECT
    '{"pipeline":"fluss-gold","executed_at_ms":'
    || CAST(UNIX_TIMESTAMP(DATE_FORMAT(last_updated_at, 'yyyy-MM-dd HH:mm:ss')) * 1000 AS STRING)
    || ',"ingest_ts_ms":' || CAST(UNIX_TIMESTAMP() * 1000 AS STRING) || '}'
FROM gold_book
WHERE MOD(account_id, 97) = 0;

END;
