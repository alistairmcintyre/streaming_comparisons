#!/usr/bin/env bash
# Register the non-Spark lake tables in Glue so Athena can query them.
#
# WHY THIS EXISTS
# Only the Spark/Iceberg pipelines register themselves (they write through the Glue
# catalog directly). Delta and Paimon write to S3 without touching Glue, so their
# tables are invisible to Athena even though the data is there. That breaks
# Objective 3 ("pipelines producing Iceberg-compatible tables, Glue + Athena").
#
# WHY EXTERNAL REGISTRATION RATHER THAN IN-WRITER
#   Paimon: metadata.iceberg.storage=hive-catalog WOULD register itself, but it needs
#   com.amazonaws.glue.catalog.metastore.AWSCatalogMetastoreClient, which AWS does not
#   publish to Maven Central (source-only on GitHub), and Paimon 1.4.2 ships no
#   Glue-native committer — IcebergHiveMetadataCommitter is the only one. So we keep
#   hadoop-catalog (Paimon writes real Iceberg metadata beside the data) and point a
#   Glue table at it here. Same result, no source builds in the image.
#   Delta: Athena engine v3 reads the _delta_log natively; it only needs a Glue table
#   with table_type=DELTA pointing at the table root. No conversion, no manifests.
#
# Idempotent: safe to re-run; updates metadata_location as Paimon commits advance.
set -euo pipefail

: "${AWS_REGION:?}" ; : "${WAREHOUSE_BUCKET:?}" ; : "${PAIMON_BUCKET:?}"
ATHENA_OUT="s3://${WAREHOUSE_BUCKET}/athena-results/"

athena_sql() {
  local sql="$1"
  local qid
  qid=$(aws athena start-query-execution --region "$AWS_REGION" \
        --query-string "$sql" --result-configuration "OutputLocation=${ATHENA_OUT}" \
        --query QueryExecutionId --output text)
  for _ in $(seq 1 60); do
    local st
    st=$(aws athena get-query-execution --region "$AWS_REGION" --query-execution-id "$qid" \
         --query 'QueryExecution.Status.State' --output text)
    case "$st" in
      SUCCEEDED) return 0 ;;
      FAILED|CANCELLED)
        aws athena get-query-execution --region "$AWS_REGION" --query-execution-id "$qid" \
          --query 'QueryExecution.Status.StateChangeReason' --output text >&2
        return 1 ;;
    esac
    sleep 2
  done
  echo "athena query timed out" >&2; return 1
}

echo "== databases =="
for db in bronze silver gold; do
  aws glue get-database --name "$db" >/dev/null 2>&1 || \
    aws glue create-database --database-input "Name=$db" >/dev/null 2>&1 || true
done

# ── Delta → Glue (Athena reads _delta_log natively) ──────────────────────────
echo "== delta =="
# db:table:s3-subpath
for spec in "bronze:trades_delta:delta/bronze/trades" \
            "silver:trades_delta:delta/silver/trades" \
            "silver:accounts_delta:delta/silver/accounts" \
            "gold:open_positions_delta:delta/gold/open_positions"; do
  db="${spec%%:*}"; rest="${spec#*:}"; tbl="${rest%%:*}"; path="${rest#*:}"
  loc="s3://${WAREHOUSE_BUCKET}/${path}"
  if ! aws s3 ls "${loc}/_delta_log/" >/dev/null 2>&1; then
    echo "  skip ${db}.${tbl} — no _delta_log yet"; continue
  fi
  # Athena infers the schema from the Delta log, so no column list is given.
  athena_sql "CREATE EXTERNAL TABLE IF NOT EXISTS \`${db}\`.\`${tbl}\`
              LOCATION '${loc}'
              TBLPROPERTIES ('table_type' = 'DELTA')" \
    && echo "  registered ${db}.${tbl}" || echo "  FAILED ${db}.${tbl}"
done

# ── Hudi: registers ITSELF, so only verify ───────────────────────────────────
# Hudi syncs to Glue on every commit via AwsGlueCatalogSyncTool (hudi-aws-bundle,
# enabled in jobs/_shared/hudi_tables.py). That is why there is no CREATE TABLE here:
# unlike Delta (Athena DDL) and Paimon (external metadata_location pointer), Hudi
# needs no external registration and cannot go stale.
# A MERGE_ON_READ table syncs as TWO Glue tables — _ro (base files only, STALE) and
# _rt (merges log files, CURRENT). Query _rt; _ro will look fast and return old data.
echo "== hudi (self-registered via Glue sync) =="
for spec in "bronze:trades_hudi" "silver:trades_hudi" "silver:accounts_hudi" \
            "gold:open_positions_hudi"; do
  db="${spec%%:*}"; tbl="${spec#*:}"
  FOUND=$(aws glue get-tables --database-name "$db" \
          --query "TableList[?starts_with(Name, '${tbl}')].Name" --output text 2>/dev/null)
  if [ -n "$FOUND" ]; then echo "  ok ${db}: $FOUND"
  else echo "  MISSING ${db}.${tbl}(_ro/_rt) — Hudi Glue sync did not run: check hudi-aws-bundle is in the image and the workload role has glue:*Table*"; fi
done

# ── Paimon's Iceberg metadata → Glue ─────────────────────────────────────────
# Paimon (hadoop-catalog) writes Iceberg metadata under <table>/metadata/vN.metadata.json.
# A Glue table with table_type=ICEBERG + metadata_location makes Athena read it.
echo "== paimon (as iceberg) =="
# NOTE the path: hadoop-catalog writes to <warehouse>/iceberg/<db>/<table>/metadata/,
# NOT beside the Paimon data under <db>.db/<table>/.
for spec in "bronze:trades_paimon:paimon/iceberg/bronze/trades" \
            "silver:trades_paimon:paimon/iceberg/silver/trades" \
            "silver:accounts_paimon:paimon/iceberg/silver/accounts" \
            "gold:open_positions_paimon:paimon/iceberg/gold/open_positions"; do
  db="${spec%%:*}"; rest="${spec#*:}"; tbl="${rest%%:*}"; path="${rest#*:}"
  latest=$(aws s3 ls "s3://${PAIMON_BUCKET}/${path}/metadata/" 2>/dev/null \
           | grep -oE '[^ ]+\.metadata\.json$' | grep -v '^\.' | sort -V | tail -1 || true)
  if [ -z "$latest" ]; then echo "  skip ${db}.${tbl} — no iceberg metadata yet"; continue; fi
  meta="s3://${PAIMON_BUCKET}/${path}/metadata/${latest}"
  aws glue create-table --database-name "$db" --table-input "{
      \"Name\": \"${tbl}\",
      \"TableType\": \"EXTERNAL_TABLE\",
      \"Parameters\": {\"table_type\": \"ICEBERG\", \"metadata_location\": \"${meta}\"},
      \"StorageDescriptor\": {\"Location\": \"s3://${PAIMON_BUCKET}/${path}\", \"Columns\": []}
    }" >/dev/null 2>&1 \
  || aws glue update-table --database-name "$db" --table-input "{
      \"Name\": \"${tbl}\",
      \"TableType\": \"EXTERNAL_TABLE\",
      \"Parameters\": {\"table_type\": \"ICEBERG\", \"metadata_location\": \"${meta}\"},
      \"StorageDescriptor\": {\"Location\": \"s3://${PAIMON_BUCKET}/${path}\", \"Columns\": []}
    }" >/dev/null 2>&1 \
  && echo "  registered ${db}.${tbl} -> ${latest}" || echo "  FAILED ${db}.${tbl}"
done

# ── Fluss's tiered Paimon tables → Glue ──────────────────────────────────────
# The Fluss tiering service mirrors silver.trades / gold.open_positions into Paimon
# under s3://$PAIMON_BUCKET/fluss/paimon, which (hadoop-catalog) writes Iceberg
# metadata to .../iceberg/<db>/<table>/metadata/ exactly like the flink-paimon
# warehouse. Fluss was previously registered NOWHERE, so it was absent from Athena
# and from the results capture entirely.
#
# The Glue names carry a _fluss suffix even though the Fluss tables are plain
# silver.trades / gold.open_positions: Fluss has its own catalog namespace, but Glue's
# silver/gold databases are shared by all five engines, so the suffix is what keeps
# `gold.open_positions_fluss` distinguishable from _delta/_paimon/_spark/_hudi_rt.
echo "== fluss (tiered paimon, as iceberg) =="
for spec in "silver:trades_fluss:fluss/paimon/iceberg/silver/trades" \
            "gold:open_positions_fluss:fluss/paimon/iceberg/gold/open_positions"; do
  db="${spec%%:*}"; rest="${spec#*:}"; tbl="${rest%%:*}"; path="${rest#*:}"
  latest=$(aws s3 ls "s3://${PAIMON_BUCKET}/${path}/metadata/" 2>/dev/null \
           | grep -oE '[^ ]+\.metadata\.json$' | grep -v '^\.' | sort -V | tail -1 || true)
  if [ -z "$latest" ]; then echo "  skip ${db}.${tbl} — no iceberg metadata yet"; continue; fi
  meta="s3://${PAIMON_BUCKET}/${path}/metadata/${latest}"
  aws glue create-table --database-name "$db" --table-input "{
      \"Name\": \"${tbl}\",
      \"TableType\": \"EXTERNAL_TABLE\",
      \"Parameters\": {\"table_type\": \"ICEBERG\", \"metadata_location\": \"${meta}\"},
      \"StorageDescriptor\": {\"Location\": \"s3://${PAIMON_BUCKET}/${path}\", \"Columns\": []}
    }" >/dev/null 2>&1 \
  || aws glue update-table --database-name "$db" --table-input "{
      \"Name\": \"${tbl}\",
      \"TableType\": \"EXTERNAL_TABLE\",
      \"Parameters\": {\"table_type\": \"ICEBERG\", \"metadata_location\": \"${meta}\"},
      \"StorageDescriptor\": {\"Location\": \"s3://${PAIMON_BUCKET}/${path}\", \"Columns\": []}
    }" >/dev/null 2>&1 \
  && echo "  registered ${db}.${tbl} -> ${latest}" || echo "  FAILED ${db}.${tbl}"
done

# ── SCD2 validity views ──────────────────────────────────────────────────────
# silver.accounts stores EVERY version, keyed (account_id, source_lsn), with only
# effective_from materialised. effective_to and is_current are derived here rather than
# written, because materialised close-out is not uniformly achievable: Paimon could do it
# with partial-update, but Fluss has only first_row/versioned/aggregation. Materialising
# in three engines and deriving in two would put a PIPELINE difference into the DATA
# MODEL, which is exactly what this project is trying not to do.
#
# A view costs nothing to maintain and gives analysts the standard SCD2 shape, so nobody
# has to hand-write LEAD() and get the boundary condition wrong. The base table remains
# the audit record; this is the lens.
#
#   current row      : WHERE is_current
#   as-of a trade    : JOIN ... ON a.account_id = t.account_id
#                      AND t.executed_at >= a.effective_from
#                      AND (a.effective_to IS NULL OR t.executed_at < a.effective_to)
echo "== scd2 views =="
for tbl in accounts_delta accounts_spark accounts_paimon accounts_hudi_rt accounts_fluss; do
  aws glue get-table --database-name silver --name "$tbl" >/dev/null 2>&1 || { echo "  skip ${tbl} — not registered"; continue; }
  athena_sql "CREATE OR REPLACE VIEW silver.${tbl}_scd2 AS
    SELECT *,
           LEAD(effective_from) OVER (PARTITION BY account_id ORDER BY effective_from) AS effective_to,
           LEAD(effective_from) OVER (PARTITION BY account_id ORDER BY effective_from) IS NULL AS is_current
    FROM silver.${tbl}" \
    && echo "  view silver.${tbl}_scd2" || echo "  FAILED view for ${tbl}"
done

echo "== registered tables =="
for db in bronze silver gold; do
  echo "  $db: $(aws glue get-tables --database-name "$db" --query 'TableList[].Name' --output text 2>/dev/null)"
done
