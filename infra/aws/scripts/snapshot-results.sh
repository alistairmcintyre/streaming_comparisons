#!/usr/bin/env bash
# Capture a run's results to S3 so they outlive the cluster.
#
# WRITES TO:
#   s3://$WAREHOUSE_BUCKET/benchmarks/$RUN_ID/wire_format=$WIRE_FORMAT/results.json
#   ...                                                        /latency_percentiles.csv
#   ...                                                        /correctness.csv
#
# Without this a run leaves only a live Grafana dashboard that vanishes at teardown.
#
# RESILIENT BY DESIGN: every metric is attempted independently and a failure is
# recorded as null rather than aborting. Hudi in particular is EXPECTED to fail its
# count — Athena supports Hudi 0.14/0.15 and we write 1.2.0 — and that must not cost
# us the other four engines' results.
set -uo pipefail

: "${AWS_REGION:?}" ; : "${WAREHOUSE_BUCKET:?}" ; : "${RUN_ID:?}"
WIRE_FORMAT="${WIRE_FORMAT:-json}"
TRADES_PER_SEC="${TRADES_PER_SEC:-unknown}"
RUN_MINUTES="${RUN_MINUTES:-unknown}"
DEST="s3://${WAREHOUSE_BUCKET}/benchmarks/${RUN_ID}/wire_format=${WIRE_FORMAT}"
ATHENA_OUT="s3://${WAREHOUSE_BUCKET}/athena-results/"
WORK=$(mktemp -d)

echo "results destination: ${DEST}/"

athena() {  # $1 = SQL -> prints first column of first data row, or empty
  local qid st
  qid=$(aws athena start-query-execution --region "$AWS_REGION" --query-string "$1" \
        --result-configuration "OutputLocation=${ATHENA_OUT}" --query QueryExecutionId --output text 2>/dev/null) || return 1
  for _ in $(seq 1 45); do
    st=$(aws athena get-query-execution --region "$AWS_REGION" --query-execution-id "$qid" \
         --query 'QueryExecution.Status.State' --output text 2>/dev/null)
    case "$st" in
      SUCCEEDED) aws athena get-query-results --region "$AWS_REGION" --query-execution-id "$qid" \
                   --query 'ResultSet.Rows[1].Data[0].VarCharValue' --output text 2>/dev/null; return 0 ;;
      FAILED|CANCELLED) return 1 ;;
    esac
    sleep 2
  done
  return 1
}

# ── correctness: source events vs what each engine actually stored ───────────
# Source of truth is the Kafka end offset: every CDC event the pipelines could have
# seen. A shortfall means data loss; an excess means duplication.
SRC=""
if command -v kubectl >/dev/null 2>&1; then
  SRC=$(kubectl -n kafka exec trades-dual-role-0 -- bin/kafka-get-offsets.sh \
        --bootstrap-server localhost:9092 --topic app.public.trades 2>/dev/null \
        | awk -F: '{s+=$3} END {print s+0}')
fi
echo "source_events (kafka end offset): ${SRC:-unavailable}"

echo "engine,table,rows,status" > "$WORK/correctness.csv"
for spec in "iceberg:bronze.trades_spark" \
            "delta:bronze.trades_delta" \
            "paimon:bronze.trades_paimon" \
            "hudi:bronze.trades_hudi_ro"; do
  eng="${spec%%:*}"; tbl="${spec##*:}"
  if rows=$(athena "SELECT count(*) FROM ${tbl}"); then
    echo "${eng},${tbl},${rows},ok" >> "$WORK/correctness.csv"
    echo "  ${eng}: ${rows} rows"
  else
    echo "${eng},${tbl},,query_failed" >> "$WORK/correctness.csv"
    echo "  ${eng}: query FAILED (expected for hudi — Athena supports 0.14/0.15, we write 1.2.0)"
  fi
done

# ── latency percentiles from the Parquet the exporter already wrote ──────────
# Registering it as an external table lets Athena do the percentiles, so this needs
# no Spark/DuckDB in the runner.
LAT_LOC="s3://${WAREHOUSE_BUCKET}/benchmarks/${RUN_ID}/latency"
echo "pipeline,p50_ms,p95_ms,p99_ms,events" > "$WORK/latency_percentiles.csv"
if aws s3 ls "${LAT_LOC}/" >/dev/null 2>&1; then
  athena "CREATE DATABASE IF NOT EXISTS benchmarks" >/dev/null 2>&1
  athena "DROP TABLE IF EXISTS benchmarks.latency_${RUN_ID}" >/dev/null 2>&1
  athena "CREATE EXTERNAL TABLE benchmarks.latency_${RUN_ID} (
            pipeline string, executed_at_ms bigint, ingest_ts_ms bigint)
          STORED AS PARQUET LOCATION '${LAT_LOC}/'" >/dev/null 2>&1
  ROWS=$(athena "SELECT count(*) FROM benchmarks.latency_${RUN_ID}" || echo "")
  echo "latency events captured: ${ROWS:-0}"
  for p in $(athena "SELECT array_join(array_agg(DISTINCT pipeline), ' ') FROM benchmarks.latency_${RUN_ID}" || echo ""); do
    R=$(athena "SELECT array_join(ARRAY[
                  CAST(approx_percentile(ingest_ts_ms - executed_at_ms, 0.50) AS varchar),
                  CAST(approx_percentile(ingest_ts_ms - executed_at_ms, 0.95) AS varchar),
                  CAST(approx_percentile(ingest_ts_ms - executed_at_ms, 0.99) AS varchar),
                  CAST(count(*) AS varchar)], ',')
                FROM benchmarks.latency_${RUN_ID} WHERE pipeline = '${p}'" || echo "")
    [ -n "$R" ] && { echo "${p},${R}" >> "$WORK/latency_percentiles.csv"; echo "  ${p}: ${R}"; }
  done
else
  echo "  no latency Parquet at ${LAT_LOC}/ — exporter did not run, or nothing emitted"
fi

# ── run context, so two result sets are comparable or provably are not ──────
cat > "$WORK/results.json" <<JSON
{
  "run_id": "${RUN_ID}",
  "captured_at": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "wire_format": "${WIRE_FORMAT}",
  "trades_per_sec": "${TRADES_PER_SEC}",
  "run_minutes": "${RUN_MINUTES}",
  "source_events_kafka": "${SRC:-null}",
  "notes": [
    "Spark engines emit latency POST-COMMIT (observe + StreamingQueryListener).",
    "Flink engines (fluss, paimon) emit at PROCESSING time — SQL has no post-commit hook — so their latency EXCLUDES the sink commit. Compare within a family freely, across families with care.",
    "Hudi counts are expected to fail: Athena supports Hudi 0.14/0.15 and this project writes 1.2.0.",
    "Only compare runs differing in wire_format alone — same rate, duration, cluster size."
  ]
}
JSON

aws s3 cp "$WORK/results.json"              "${DEST}/results.json"              --only-show-errors
aws s3 cp "$WORK/correctness.csv"           "${DEST}/correctness.csv"           --only-show-errors
aws s3 cp "$WORK/latency_percentiles.csv"   "${DEST}/latency_percentiles.csv"   --only-show-errors
echo "wrote:"
aws s3 ls "${DEST}/" | sed 's|^|  |'
