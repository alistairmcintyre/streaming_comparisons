-- Count the LIVE Fluss tables, not the lake mirror.
--
-- WHY THIS FILE EXISTS. Every other count in this benchmark goes through Athena, and
-- Athena cannot read Fluss. What it reads for `fluss` is silver.trades_fluss, a Glue
-- table pointing at fluss/paimon/iceberg/silver/trades: the Iceberg view of the Paimon
-- copy that the Fluss tiering service writes asynchronously. That mirror lags the hot
-- table by design, so comparing it to a Kafka end offset measures tiering throughput and
-- calls it completeness. On the 2026-09-01 run it came back 2,025,500 rows below source
-- and was briefly read as data loss, which it was not.
--
-- Fluss has no reader outside its own client, and the only client in this stack is the
-- Flink connector, so this is a Flink SQL job by necessity rather than preference. DuckDB
-- reads Delta, Iceberg and Paimon here perfectly well (query-lake-local.sh) and cannot
-- help with this one.
--
-- THE THREE SETTINGS ARE ALL LOAD-BEARING, each verified by watching it fail without:
--   runtime-mode=batch    a streaming SELECT never terminates, so the count never prints
--   result-mode=TABLEAU   the SQL client refuses to render a query result in -f mode
--                         otherwise: "In non-interactive mode, it only supports to use
--                         TABLEAU as value of sql-client.execution.result-mode"
--   table.dml-sync        defensive. This file only reads, but a caller that appends an
--                         INSERT would otherwise get a job id back immediately and race
--                         its own write; that is exactly what the first probe did
--
-- Verified against a real Fluss cluster (zookeeper + coordinator + tablet server in
-- Docker, the same harness validate-fluss-sql.sh uses): 250 rows written, 250 counted.
-- tests/fluss_live_count_test.sh is that check.
--
-- TEMPLATE: submit.sh / the caller runs envsubst for ${FLUSS_BOOTSTRAP}.
SET 'execution.runtime-mode'           = 'batch';
SET 'sql-client.execution.result-mode' = 'TABLEAU';
SET 'table.dml-sync'                   = 'true';
SET 'table.local-time-zone'            = 'UTC';

CREATE CATALOG fluss_catalog WITH (
    'type'              = 'fluss',
    'bootstrap.servers' = '${FLUSS_BOOTSTRAP}'
);

-- One row per table, tagged, so a caller can parse both from a single run. The tag is a
-- literal rather than a column name because the TABLEAU output is what gets scraped.
SELECT 'silver.trades' AS fluss_table, CAST(count(*) AS BIGINT) AS live_rows
FROM fluss_catalog.silver.trades;

SELECT 'gold.open_positions' AS fluss_table, CAST(count(*) AS BIGINT) AS live_rows
FROM fluss_catalog.gold.open_positions;
