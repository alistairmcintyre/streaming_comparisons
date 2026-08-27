#!/usr/bin/env bash
# Capture a run's results to S3 so they outlive the cluster.
#
# WRITES TO:
#   s3://$WAREHOUSE_BUCKET/benchmarks/$RUN_ID/wire_format=$WIRE_FORMAT/results.json
#   ...                                                        /processing_delay.csv   <- headline metric
#   ...                                                        /invariants.csv         <- fold correctness
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
# Fluss's landing table is silver.trades, not bronze: its PK table IS the cleaned
# deduped view, so it has one hop fewer by design (see `hops` in results.json).
for spec in "iceberg:bronze.trades_spark" \
            "delta:bronze.trades_delta" \
            "paimon:bronze.trades_paimon" \
            "fluss:silver.trades_fluss" \
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

# ── FOLD INVARIANT (does gold agree with silver?) ────────────────────────────
# Every trade in silver must be folded into gold exactly once, so:
#
#     sum(gold.trade_count) == count(silver.trades)
#
# This is the check that makes the at-least-once gold folds honest. Delta guards its
# MERGE with txnAppId/txnVersion so a replayed micro-batch is a no-op; Iceberg and Hudi
# have no idempotent-write primitive, so a replayed batch DOUBLE-COUNTS and the book
# drifts permanently. Flink's engines fold in checkpointed state and are exactly-once.
#
# We DETECT drift rather than repairing it, deliberately. A reconcile job that rewrote
# the book would add write amplification to whichever engine drifted — contaminating
# the very numbers this run exists to measure. "Did this engine drift under sustained
# load?" is itself a result, and it belongs in the output next to the latency figures.
#
# Only meaningful after quiesce-run.sh: mid-flight, gold legitimately trails silver by
# whatever is in the current micro-batch. `quiesced` records whether that ran, so a
# drift figure can never be read as data loss when it was really just a race.
QUIESCED="${QUIESCED:-unknown}"
echo "invariant: sum(gold.trade_count) vs count(silver.trades)   [quiesced=${QUIESCED}]"
echo "engine,silver_trades,gold_trade_count_sum,drift,drift_pct,quiesced,status" > "$WORK/invariants.csv"
for spec in "iceberg:silver.trades_spark:gold.open_positions_spark" \
            "delta:silver.trades_delta:gold.open_positions_delta" \
            "paimon:silver.trades_paimon:gold.open_positions_paimon" \
            "fluss:silver.trades_fluss:gold.open_positions_fluss" \
            "hudi:silver.trades_hudi_ro:gold.open_positions_hudi_rt"; do
  eng="${spec%%:*}"; rest="${spec#*:}"; sil="${rest%%:*}"; gld="${rest#*:}"
  SN=$(athena "SELECT CAST(count(*) AS varchar) FROM ${sil}") || SN=""
  GN=$(athena "SELECT CAST(COALESCE(sum(trade_count), 0) AS varchar) FROM ${gld}") || GN=""
  if [ -n "$SN" ] && [ -n "$GN" ]; then
    D=$(( GN - SN ))
    PCT=$(awk -v d="$D" -v s="$SN" 'BEGIN{ if (s+0==0) print "n/a"; else printf "%.6f", (d/s)*100 }')
    ST=$([ "$D" -eq 0 ] && echo ok || echo DRIFT)
    echo "${eng},${SN},${GN},${D},${PCT},${QUIESCED},${ST}" >> "$WORK/invariants.csv"
    echo "  ${eng}: silver=${SN} gold=${GN} drift=${D} (${PCT}%) ${ST}"
  else
    echo "${eng},${SN},${GN},,,${QUIESCED},query_failed" >> "$WORK/invariants.csv"
    echo "  ${eng}: query FAILED (hudi is expected — Athena supports 0.14/0.15, we write 1.2.0)"
  fi
done

# ── PROCESSING DELAY (the headline metric) ───────────────────────────────────
# Read straight out of each engine's gold table: commit_ts (processing time, stamped
# when the gold row is produced) minus last_updated_at (event time of the newest fill
# folded into it). Every engine computes both the same way, so this is uniform in a
# way the Kafka emit chain is not — Spark emits post-commit via a StreamingQueryListener
# while Flink SQL has no post-commit hook and samples at processing time.
#
# It also cannot go silently missing: if the emit chain breaks, the topic is empty and
# there is nothing to report; these values are a property of the data that was written.
# Where they disagree with latency_percentiles.csv, THIS is the number to trust.
#
# Caveat worth stating: it measures the delay of rows that were WRITTEN. A pipeline so
# far behind that a position never reaches gold contributes nothing here — read it
# alongside correctness.csv, which catches exactly that.
echo "processing_delay: gold.commit_ts - gold.last_updated_at"
echo "engine,table,p50_ms,p95_ms,p99_ms,rows,status" > "$WORK/processing_delay.csv"
for spec in "iceberg:gold.open_positions_spark" \
            "delta:gold.open_positions_delta" \
            "paimon:gold.open_positions_paimon" \
            "fluss:gold.open_positions_fluss" \
            "hudi:gold.open_positions_hudi_rt"; do
  eng="${spec%%:*}"; tbl="${spec##*:}"
  R=$(athena "SELECT array_join(ARRAY[
        CAST(approx_percentile(date_diff('millisecond', last_updated_at, commit_ts), 0.50) AS varchar),
        CAST(approx_percentile(date_diff('millisecond', last_updated_at, commit_ts), 0.95) AS varchar),
        CAST(approx_percentile(date_diff('millisecond', last_updated_at, commit_ts), 0.99) AS varchar),
        CAST(count(*) AS varchar)], ',')
      FROM ${tbl}
      WHERE last_updated_at IS NOT NULL AND commit_ts IS NOT NULL") || R=""
  if [ -n "$R" ]; then
    echo "${eng},${tbl},${R},ok" >> "$WORK/processing_delay.csv"
    echo "  ${eng}: p50/p95/p99/rows = ${R}"
  else
    echo "${eng},${tbl},,,,,query_failed" >> "$WORK/processing_delay.csv"
    echo "  ${eng}: query FAILED (hudi is expected — Athena supports 0.14/0.15, we write 1.2.0)"
  fi
done

# ── latency percentiles from the Parquet the exporter already wrote ──────────
# SECONDARY to processing_delay.csv above. This is the Kafka emit chain that drives
# the live Grafana dashboard: it decomposes the delay per HOP (…-bronze / -silver /
# -gold), which the in-table metric cannot do, but the Spark and Flink families stop
# their clocks at different points so cross-family numbers here are not comparable.
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
  "primary_metric": "processing_delay.csv — gold.commit_ts minus gold.last_updated_at, computed identically by every engine and read out of the tables themselves",
  "quiesced": "${QUIESCED:-unknown}",
  "engines": {
    "iceberg": { "hops": 3, "layers": "kafka -> bronze.trades_spark -> silver.trades_spark -> gold.open_positions_spark",  "engine": "spark" },
    "delta":   { "hops": 3, "layers": "kafka -> bronze.trades_delta -> silver.trades_delta -> gold.open_positions_delta",  "engine": "spark" },
    "hudi":    { "hops": 3, "layers": "kafka -> bronze.trades_hudi -> silver.trades_hudi -> gold.open_positions_hudi",     "engine": "spark" },
    "paimon":  { "hops": 3, "layers": "kafka -> bronze.trades -> silver.trades -> gold.open_positions",                    "engine": "flink" },
    "fluss":   { "hops": 2, "layers": "kafka -> silver.trades -> gold.open_positions",                                     "engine": "flink" }
  },
  "notes": [
    "HOPS ARE NOT EQUAL. Fluss has 2 write hops where the others have 3: its PK table is already the cleaned deduped view, so there is no separate bronze landing to re-read. That is a real structural advantage of the hot-tier design, not a measurement artefact — but a latency comparison is only meaningful read against the `hops` above.",
    "PRIMARY: processing_delay.csv. gold.commit_ts (processing time, stamped as the gold row is produced) minus gold.last_updated_at (event time of the newest fill folded in). Uniform across all five engines and a property of the data written, so it cannot silently vanish the way an emit chain can.",
    "SECONDARY: latency_percentiles.csv, from the Kafka emit chain that feeds the live dashboard. It decomposes delay per hop (-bronze/-silver/-gold), which the in-table metric cannot, but Spark emits POST-COMMIT (observe + StreamingQueryListener) while Flink SQL has no post-commit hook and samples at PROCESSING time — so its cross-family numbers are not comparable.",
    "gold.opened_at is MIN(executed_at) over the position, NOT reset when a flat position reopens. Reset-on-flat is expressible in the Spark MERGE but not as a pure Flink fold, so it would make the two families disagree on identical input.",
    "CORRECTNESS IS DETECTED, NOT REPAIRED. invariants.csv reports sum(gold.trade_count) - count(silver.trades) per engine. Delta guards its MERGE with txnAppId/txnVersion and the Flink engines fold in checkpointed state, so both are exactly-once; Iceberg and Hudi have no idempotent-write primitive and a replayed micro-batch double-counts permanently. A reconcile job was deliberately NOT added: rewriting the book adds write amplification to whichever engine drifted, contaminating the measurement. Non-zero drift is a published result, not a hidden repair.",
    "invariants.csv and correctness.csv are only meaningful when quiesced=true (quiesce-run.sh stopped the load and waited for zero consumer lag). Mid-flight, gold legitimately trails silver.",
    "ENRICHMENT IS NOT IN GOLD. country/tier are account attributes with no defensible temporal semantic on a current-state position row, so gold holds position facts only and enrichment is a read-time LEFT JOIN to silver.accounts. LEFT always: trades and accounts are independent CDC streams, so a fill can land before its account row and an inner join would silently drop that position from the book. All five golds therefore have identical 9-column schemas.",
    "HUDI HAS NO PER-BATCH ADMISSION CONTROL. Delta caps its stream with maxFilesPerTrigger (200) and Iceberg with streaming-max-files-per-micro-batch (500); Hudi 1.2.0 exposes no equivalent option. Under steady state all three behave similarly, but after a stall Hudi consumes its whole backlog in one micro-batch, so batch-size-sensitive figures are not comparable during recovery.",
    "Hudi queries are expected to fail: Athena supports Hudi 0.14/0.15 and this project writes 1.2.0.",
    "Only compare runs differing in wire_format alone — same rate, duration, cluster size."
  ]
}
JSON

aws s3 cp "$WORK/results.json"              "${DEST}/results.json"              --only-show-errors
aws s3 cp "$WORK/correctness.csv"           "${DEST}/correctness.csv"           --only-show-errors
aws s3 cp "$WORK/processing_delay.csv"      "${DEST}/processing_delay.csv"      --only-show-errors
aws s3 cp "$WORK/invariants.csv"           "${DEST}/invariants.csv"           --only-show-errors
aws s3 cp "$WORK/latency_percentiles.csv"   "${DEST}/latency_percentiles.csv"   --only-show-errors
echo "wrote:"
aws s3 ls "${DEST}/" | sed 's|^|  |'
