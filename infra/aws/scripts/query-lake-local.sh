#!/usr/bin/env bash
# Query the lake tables from your laptop with DuckDB, no Athena, no cluster access.
#
# Verified 2026-08-26 against a live run: Delta (incl. deletion vectors), Iceberg,
# and Paimon (via the Iceberg metadata it writes) all read correctly, and DECIMAL
# precision survives (net_notional comes back as decimal(38,4)).
#
# Why DuckDB rather than Athena: it reads Delta tables with DELETION VECTORS, which
# Athena's DDL engine rejects (minReaderVersion 3). That matters because deletion
# vectors are Delta's merge-on-read equivalent, disabling them to satisfy Athena
# would force Delta into copy-on-write and bias it against Iceberg MOR.
#
#   ./query-lake-local.sh                      # opens an interactive shell
#   ./query-lake-local.sh "SELECT ..."         # runs one query
set -euo pipefail

: "${AWS_PROFILE:=streaming-comparisons}" ; export AWS_PROFILE
WAREHOUSE="${WAREHOUSE_BUCKET:-streaming-comparison-amc-warehouse}"
PAIMON="${PAIMON_BUCKET:-streaming-comparison-amc-paimon}"
REGION="${AWS_REGION:-eu-west-1}"
DUCKDB="${DUCKDB:-$(command -v duckdb || echo ./duckdb)}"

[ -x "$DUCKDB" ] || { echo "duckdb not found. Get it with:
  curl -sfL -o duckdb.zip https://github.com/duckdb/duckdb/releases/latest/download/duckdb_cli-linux-amd64.zip
  unzip -o duckdb.zip && chmod +x duckdb" >&2; exit 1; }

# SSO creds are short-lived; export them explicitly rather than relying on the chain.
CREDS=$(aws configure export-credentials --format env-no-export)
AK=$(grep AWS_ACCESS_KEY_ID     <<<"$CREDS" | cut -d= -f2)
SK=$(grep AWS_SECRET_ACCESS_KEY <<<"$CREDS" | cut -d= -f2)
ST=$(grep AWS_SESSION_TOKEN     <<<"$CREDS" | cut -d= -f2 || true)

# Paimon rotates vN.metadata.json, so resolve the newest at call time. On tables
# created before metadata.iceberg.delete-after-commit.enabled=false, the pointer can
# still go stale between listing and query, just re-run.
latest_meta() { aws s3 ls "$1" 2>/dev/null | grep -oE '[^ ]+\.metadata\.json$' | sort -V | tail -1; }

# Resolve and re-verify: a stale pointer makes iceberg_scan 404, and DuckDB aborts the
# whole init file on error, which would take the Delta views down with it. Retry, and
# skip the view entirely rather than break the session.
resolve_meta() {
  local dir="$1" m
  for _ in 1 2 3; do
    m=$(latest_meta "$dir") || true
    [ -z "$m" ] && return 1
    aws s3 ls "${dir}${m}" >/dev/null 2>&1 && { echo "$m"; return 0; }
  done
  return 1
}
PAI_TRADES=$(resolve_meta "s3://$PAIMON/paimon/iceberg/bronze/trades/metadata/" || true)
ICE_TRADES=$(resolve_meta "s3://$WAREHOUSE/iceberg/bronze.db/trades_spark/metadata/" || true)
[ -z "${PAI_TRADES:-}" ] && echo "note: paimon metadata rotated away; skipping that view (re-run, or apply metadata.iceberg.delete-after-commit.enabled=false)" >&2

INIT=$(mktemp)
cat > "$INIT" <<SQL
INSTALL httpfs;  LOAD httpfs;
INSTALL delta;   LOAD delta;
INSTALL iceberg; LOAD iceberg;
CREATE SECRET s3sec (TYPE s3, KEY_ID '$AK', SECRET '$SK', SESSION_TOKEN '$ST', REGION '$REGION');

-- Delta: read straight from the table root (handles deletion vectors).
CREATE OR REPLACE VIEW delta_bronze_trades   AS SELECT * FROM delta_scan('s3://$WAREHOUSE/delta/bronze/trades');
CREATE OR REPLACE VIEW delta_silver_trades   AS SELECT * FROM delta_scan('s3://$WAREHOUSE/delta/silver/trades');
CREATE OR REPLACE VIEW delta_silver_accounts AS SELECT * FROM delta_scan('s3://$WAREHOUSE/delta/silver/accounts');
CREATE OR REPLACE VIEW delta_gold_positions  AS SELECT * FROM delta_scan('s3://$WAREHOUSE/delta/gold/open_positions');

-- Iceberg / Paimon: point at a metadata.json.
$( [ -n "$ICE_TRADES" ] && echo "CREATE OR REPLACE VIEW iceberg_bronze_trades AS SELECT * FROM iceberg_scan('s3://$WAREHOUSE/iceberg/bronze.db/trades_spark/metadata/$ICE_TRADES');" )
-- Paimon: a MACRO, not a view. On a live table Paimon commits every few seconds and
-- deletes the old vN.metadata.json immediately, so any eagerly-created view loses the
-- race and 404s, and DuckDB aborts the whole init file on error, which would take the
-- Delta views with it. A macro touches S3 only when called.
--   SELECT count(*) FROM paimon_trades('$(basename "${PAI_TRADES:-vNNN.metadata.json}")');
CREATE OR REPLACE MACRO paimon_trades(m) AS TABLE
  SELECT * FROM iceberg_scan('s3://$PAIMON/paimon/iceberg/bronze/trades/metadata/' || m);
SQL

if [ $# -gt 0 ]; then
  "$DUCKDB" -init "$INIT" -c "$1"
else
  echo "views:  delta_bronze_trades, delta_silver_trades, delta_silver_accounts,"
  echo "        delta_gold_positions, iceberg_bronze_trades"
  echo "macros: paimon_trades(m), paimon_gold(m), fluss_gold(m)   # m = a metadata.json name"
  echo "        newest at launch: '${PAI_TRADES:-vNNN.metadata.json}'"
  echo "e.g.    SELECT count(*) FROM delta_gold_positions;"
  echo "scd2:   silver.accounts keeps every version. effective_to / is_current are REAL"
  echo "        COLUMNS, materialised by the close-out, no window function needed:"
  echo "        SELECT * FROM delta_silver_accounts WHERE is_current;"
  echo "as-of:  the point of SCD2, what was this account at the time of the trade:"
  echo "        SELECT t.*, a.country, a.tier FROM delta_silver_trades t"
  echo "        LEFT JOIN delta_silver_accounts a ON a.account_id = t.account_id"
  echo "          and t.executed_at >= a.effective_from"
  echo "          and (a.effective_to IS NULL OR t.executed_at < a.effective_to);"
  echo "now:    gold enrichment is CURRENT-state, so it uses is_current instead:"
  echo "        SELECT p.*, a.country, a.tier FROM delta_gold_positions p"
  echo "        LEFT JOIN delta_silver_accounts a ON a.account_id = p.account_id"
  echo "          and a.is_current;   -- LEFT + is_current: never drops or fans out"
  echo "         and t.last_updated_at >= a.effective_from"
  echo "         and (a.effective_to IS NULL OR t.last_updated_at < a.effective_to);"
  echo "enrich: country/tier are not in gold (account attributes, no temporal meaning"
  echo "        on a current-state row). LEFT JOIN them at read time. LEFT because a"
  echo "        fill can land before its account row, and INNER would drop the position:"
  echo "        SELECT p.*, a.country, a.tier FROM delta_gold_positions p"
  echo "        LEFT JOIN delta_silver_accounts a USING (account_id);"
  echo "delay:  the headline metric, straight out of gold, no emit chain involved:"
  echo "        SELECT quantile_cont(epoch_ms(commit_ts)-epoch_ms(last_updated_at), [0.5,0.95,0.99])"
  echo "        FROM delta_gold_positions WHERE last_updated_at IS NOT NULL;"
  echo "        SELECT count(*) FROM paimon_trades('${PAI_TRADES:-vNNN.metadata.json}');"
  "$DUCKDB" -init "$INIT"
fi
