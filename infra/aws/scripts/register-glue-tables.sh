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
  #
  # EXPECT TWO OF THESE FOUR TO FAIL, and it is not a bug in this script.
  # delta_tables.py sets delta.enableDeletionVectors=true on silver.accounts and
  # gold.open_positions (Delta's merge-on-read analogue, chosen so its write profile is
  # comparable to Iceberg MOR and Hudi MOR) and false on the two append-only trades
  # tables. Athena's Delta reader does not support deletion vectors, and the failures
  # correlate EXACTLY with that setting:
  #     bronze.trades   DV=false -> registered
  #     silver.trades   DV=false -> registered
  #     silver.accounts DV=true  -> FAILED
  #     gold.open_positions DV=true -> FAILED
  # Turning DVs off would make those two Athena-readable at the cost of changing what the
  # benchmark measures on the update-heavy tables. Read them through Spark instead.
  # The reason is echoed rather than swallowed so this is diagnosable next time.
  if reason=$(athena_sql "CREATE EXTERNAL TABLE IF NOT EXISTS \`${db}\`.\`${tbl}\`
              LOCATION '${loc}'
              TBLPROPERTIES ('table_type' = 'DELTA')" 2>&1); then
    echo "  registered ${db}.${tbl}"
  else
    echo "  FAILED ${db}.${tbl}: $(echo "$reason" | tr '\n' ' ' | head -c 160)"
  fi
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

# Athena reads an Iceberg table's COLUMNS FROM THE GLUE TABLE DEFINITION, not from the
# metadata file the pointer names. Registering with "Columns": [] therefore produced a
# table where `SELECT count(*)` WORKED (that only needs the snapshot) while `SELECT *`
# failed with
#     COLUMN_NOT_FOUND: line 1:8: Relation contains no accessible columns
# for paimon AND fluss — three of five engines effectively unqueryable, and their rows in
# snapshot-results recorded as query_failed as if the pipelines were broken.
# Verified directly: re-registering the SAME metadata pointer with columns populated
# returned real rows (account_id=1, symbol=AMD, net_quantity=1633, status=OPEN).
# Derived from the metadata's own schema rather than hardcoded, so it follows the table.
glue_columns_from_metadata() {   # $1 = s3://.../vN.metadata.json  -> Glue Columns JSON
  aws s3 cp "$1" - 2>/dev/null | python3 -c '
import json, re, sys
M = {"long":"bigint","int":"int","string":"string","boolean":"boolean","double":"double",
     "float":"float","date":"date","binary":"binary","uuid":"string",
     "timestamp":"timestamp","timestamptz":"timestamp"}
def conv(t):
    if isinstance(t, str):
        if t.startswith("decimal"):
            return re.sub(r"\s+", "", t)
        if t.startswith("fixed"):
            return "binary"
        return M.get(t, "string")
    return "string"   # struct/list/map: not used by these tables
try:
    d = json.load(sys.stdin)
    sid = d.get("current-schema-id", 0)
    sch = next((x for x in d.get("schemas", []) if x.get("schema-id") == sid), None) or d["schemas"][0]
    print(json.dumps([{"Name": f["name"], "Type": conv(f["type"])} for f in sch["fields"]]))
except Exception:
    print("[]")
'
}

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
  COLS=$(glue_columns_from_metadata "$meta"); [ -z "$COLS" ] && COLS="[]"
  aws glue create-table --database-name "$db" --table-input "{
      \"Name\": \"${tbl}\",
      \"TableType\": \"EXTERNAL_TABLE\",
      \"Parameters\": {\"table_type\": \"ICEBERG\", \"metadata_location\": \"${meta}\"},
      \"StorageDescriptor\": {\"Location\": \"s3://${PAIMON_BUCKET}/${path}\", \"Columns\": ${COLS}}
    }" >/dev/null 2>&1 \
  || aws glue update-table --database-name "$db" --table-input "{
      \"Name\": \"${tbl}\",
      \"TableType\": \"EXTERNAL_TABLE\",
      \"Parameters\": {\"table_type\": \"ICEBERG\", \"metadata_location\": \"${meta}\"},
      \"StorageDescriptor\": {\"Location\": \"s3://${PAIMON_BUCKET}/${path}\", \"Columns\": ${COLS}}
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
  COLS=$(glue_columns_from_metadata "$meta"); [ -z "$COLS" ] && COLS="[]"
  aws glue create-table --database-name "$db" --table-input "{
      \"Name\": \"${tbl}\",
      \"TableType\": \"EXTERNAL_TABLE\",
      \"Parameters\": {\"table_type\": \"ICEBERG\", \"metadata_location\": \"${meta}\"},
      \"StorageDescriptor\": {\"Location\": \"s3://${PAIMON_BUCKET}/${path}\", \"Columns\": ${COLS}}
    }" >/dev/null 2>&1 \
  || aws glue update-table --database-name "$db" --table-input "{
      \"Name\": \"${tbl}\",
      \"TableType\": \"EXTERNAL_TABLE\",
      \"Parameters\": {\"table_type\": \"ICEBERG\", \"metadata_location\": \"${meta}\"},
      \"StorageDescriptor\": {\"Location\": \"s3://${PAIMON_BUCKET}/${path}\", \"Columns\": ${COLS}}
    }" >/dev/null 2>&1 \
  && echo "  registered ${db}.${tbl} -> ${latest}" || echo "  FAILED ${db}.${tbl}"
done

# NOTE: the SCD2 validity views that used to live here are GONE. effective_to and
# is_current are now MATERIALISED by an atomic close-out in every engine's
# silver_accounts job, so deriving them per query would be redundant work and a second
# definition of the same thing. The as-of join is now a plain range predicate on stored
# columns — or, better, an equality join on (account_id, source_lsn) when the fact carries
# the version pointer.

echo "== registered tables =="
for db in bronze silver gold; do
  echo "  $db: $(aws glue get-tables --database-name "$db" --query 'TableList[].Name' --output text 2>/dev/null)"
done
