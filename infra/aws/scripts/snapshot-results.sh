#!/usr/bin/env bash
# Capture a run's results to S3 so they outlive the cluster.
#
# WRITES TO:
#   s3://$WAREHOUSE_BUCKET/benchmarks/$RUN_ID/wire_format=$WIRE_FORMAT/results.json
#   ...                                                        /processing_delay.csv   <- headline metric
#   ...                                                        /invariants.csv         <- fold correctness
#   ...                                                        /dedupe.csv             <- silver dedupe correctness
#   ...                                                        /latency_percentiles.csv
#   ...                                                        /correctness.csv
#
# Without this a run leaves only a live Grafana dashboard that vanishes at teardown.
#
# RESILIENT BY DESIGN: every metric is attempted independently and a failure is
# recorded as null rather than aborting.
#
# THREE ENGINES ARE EXPECTED TO FAIL THEIR ATHENA QUERIES, each for a different and
# VERIFIED reason. None is a pipeline defect; all were confirmed against a live run.
#
#   hudi  — READS FINE in a controlled test, contrary to what this comment used to say.
#           A 1-row MOR table with the production options (crucially
#           hoodie.write.table.version=6), synced to Glue and upserted until log files
#           existed, returned correct data from Athena on the partitioned table, its _rt
#           and its _ro (stale, as RO should be), and on an unpartitioned one. `symbol`
#           resolves from the partition key. Without write.table.version=6 every query
#           failed with a bare HIVE_UNKNOWN_ERROR, so that setting is what makes Hudi
#           readable at all.
#           A live gold table nonetheless failed with
#             HIVE_CURSOR_ERROR: LongWritable cannot be cast to HiveDecimalWritable
#           which is the signature of a one-column shift. Unexplained: the controlled test
#           has the same Glue/parquet shape and reads correctly. That table had accumulated
#           files over five job restarts — compare per-partition parquet schemas against
#           the Glue column list before concluding anything.
#           NOTE the un-suffixed table (open_positions_hudi) is the REAL-TIME view.
#
#   paimon and fluss — FIXED. Their Glue tables were registered with "Columns": [], and
#           Athena reads an Iceberg table's columns FROM THE GLUE DEFINITION, not from the
#           metadata file the pointer names. So `SELECT count(*)` worked (it only needs the
#           snapshot) while `SELECT *` failed with
#             COLUMN_NOT_FOUND: Relation contains no accessible columns
#           register-glue-tables.sh now derives the columns from the metadata's own schema.
#           Verified against the live run: all four paimon/fluss tables return rows.
#
#           NOTE for anyone who reads the metadata and gets suspicious: Paimon numbers
#           Iceberg field ids from 0 where a native Iceberg writer starts at 1. That is a
#           real difference and it is NOT the cause — re-registering with ids shifted to
#           1-based changed nothing, and the original 0-based metadata reads fine once the
#           Glue columns are present. Tested both ways rather than assumed.
#
#   delta — FIXED. silver.accounts and gold.open_positions could not be registered by
#           Athena DDL: enabling deletion vectors (Delta's merge-on-read analogue, on
#           exactly those two) raises the protocol past what the DDL engine accepts —
#             CREATE EXTERNAL TABLE ... table_type=DELTA
#             -> Delta protocol version is too new for Athena DDL engine
#           Athena's QUERY engine reads deletion vectors fine (since July 2024); only DDL
#           refuses. register-glue-tables.sh now registers them through the Glue API in the
#           NATIVE DELTA shape instead. Verified on a table carrying 5 deletion-vector
#           files: Athena returns 20000, exactly matching Spark.
#
#   paimon and fluss — FIXED. Their Glue tables were registered with "Columns": [], and
#           Athena reads an Iceberg table's columns FROM THE GLUE DEFINITION, not from the
#           metadata file the pointer names. So `SELECT count(*)` worked (it only needs the
#           snapshot) while `SELECT *` failed with
#             COLUMN_NOT_FOUND: Relation contains no accessible columns
#           register-glue-tables.sh now derives the columns from the metadata's own schema.
#           Verified against the live run: all four paimon/fluss tables return rows.
#
#           NOTE for anyone who reads the metadata and gets suspicious: Paimon numbers
#           Iceberg field ids from 0 where a native Iceberg writer starts at 1. That is a
#           real difference and it is NOT the cause — re-registering with ids shifted to
#           1-based changed nothing, and the original 0-based metadata reads fine once the
#           Glue columns are present. Tested both ways rather than assumed.
#
#   delta — silver.accounts and gold.open_positions failed to REGISTER on the last run
#           (register-glue-tables.sh), so they are absent from Athena rather than
#           unreadable. Cause UNKNOWN: the script printed a bare "FAILED" and the reason
#           was lost. Deletion vectors were the obvious suspect — they are enabled on
#           exactly those two tables — but Athena engine v3 has READ deletion vectors
#           since July 2024, so that is ruled out. They are also the 3rd and 4th DDL
#           statements issued back-to-back, which fits equally well. The script now echoes
#           the reason and retries transient DDL errors, so the next run settles it.
#
# So Athena covers spark-iceberg, paimon, fluss and two of the four delta tables. Hudi's
# PARTITIONED tables remain unreadable for the positional reason above; query those
# through Spark.
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
# Hudi is queried through _rt, NOT _ro. A MOR table's _ro view reads base files only and
# does not merge the log files, so it returns STALE counts — jobs/_shared/hudi_tables.py
# states the rule outright. The fold invariant below was counting silver through _ro and
# gold through _rt, which would have reported drift that was purely the query view.
for spec in "iceberg:bronze.trades_spark" \
            "delta:bronze.trades_delta" \
            "paimon:bronze.trades_paimon" \
            "fluss:silver.trades_fluss" \
            "hudi:bronze.trades_hudi_rt"; do
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
            "hudi:silver.trades_hudi_rt:gold.open_positions_hudi_rt"; do
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

# ── DEDUPE INVARIANT (is silver actually one row per trade?) ─────────────────
#
#     count(*) == count(DISTINCT trade_id)   on silver.trades
#
# COMPLEMENTARY to the fold invariant above, which cannot see this: a duplicate row in
# silver IS folded into gold, so sum(gold.trade_count) still equals count(silver.trades)
# and the drift figure stays zero while the book is wrong.
#
# It matters because the five engines dedupe by different mechanisms. Paimon and Fluss
# hold silver.trades as a PK table with the first-row merge engine and Hudi as an upsert
# on trade_id, so on those three a duplicate CANNOT become a row — the dedupe is
# structural and unbounded. Delta and Iceberg keep silver.trades APPEND-ONLY and dedupe
# in operator state (dropDuplicatesWithinWatermark), which is necessarily BOUNDED: the
# window is 2h, chosen to span a whole run. A re-delivery arriving outside it is appended
# as a genuine second row and nothing downstream can undo it — the gold fold is `+=` over
# (account_id, symbol) and never sees trade_id at all.
#
# So this is the number that says whether that bound held. DETECTED, not repaired, for
# the same reason as the fold invariant: a reconcile job would add write amplification to
# whichever engine drifted and contaminate the measurement.
echo "invariant: count(*) vs count(DISTINCT trade_id) on silver.trades"
echo "engine,silver_rows,distinct_trade_ids,duplicates,status" > "$WORK/dedupe.csv"
for spec in "iceberg:silver.trades_spark" \
            "delta:silver.trades_delta" \
            "paimon:silver.trades_paimon" \
            "fluss:silver.trades_fluss" \
            "hudi:silver.trades_hudi_rt"; do
  eng="${spec%%:*}"; tbl="${spec##*:}"
  R=$(athena "SELECT CAST(count(*) AS varchar) FROM ${tbl}") || R=""
  U=$(athena "SELECT CAST(count(DISTINCT trade_id) AS varchar) FROM ${tbl}") || U=""
  if [ -n "$R" ] && [ -n "$U" ]; then
    DUP=$(( R - U ))
    ST=$([ "$DUP" -eq 0 ] && echo ok || echo DUPLICATES)
    echo "${eng},${R},${U},${DUP},${ST}" >> "$WORK/dedupe.csv"
    echo "  ${eng}: rows=${R} distinct=${U} duplicates=${DUP} ${ST}"
  else
    echo "${eng},${R},${U},,query_failed" >> "$WORK/dedupe.csv"
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
  # REGISTERED THROUGH THE GLUE API, not Athena DDL. Athena's DDL engine is a second,
  # weaker path to the same catalog: it rejected the Delta tables outright ("protocol
  # version is too new") while its QUERY engine reads them fine, and it can only express
  # what its own dialect covers. The Glue API states the table directly, works for every
  # format here, and is testable offline against a local emulator
  # (tests/glue-registration-test.sh). Nothing in this repo creates catalog objects with
  # Athena DDL any more; Athena is used only to SELECT.
  aws glue create-database --database-input "Name=benchmarks" >/dev/null 2>&1 || true
  aws glue delete-table --database-name benchmarks --name "latency_${RUN_ID}" >/dev/null 2>&1 || true
  # A plain Parquet external table — the exporter writes ordinary Parquet here, so the
  # Hive Parquet SerDe is the CORRECT one. (It is emphatically not correct for Delta: over
  # a Delta location it ignores _delta_log and serves deleted rows as live data.)
  aws glue create-table --database-name benchmarks --table-input "{
      \"Name\": \"latency_${RUN_ID}\", \"TableType\": \"EXTERNAL_TABLE\",
      \"Parameters\": {\"EXTERNAL\": \"TRUE\", \"classification\": \"parquet\"},
      \"StorageDescriptor\": {
        \"Location\": \"${LAT_LOC}/\",
        \"Columns\": [{\"Name\":\"pipeline\",\"Type\":\"string\"},
                     {\"Name\":\"executed_at_ms\",\"Type\":\"bigint\"},
                     {\"Name\":\"ingest_ts_ms\",\"Type\":\"bigint\"}],
        \"InputFormat\": \"org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat\",
        \"OutputFormat\": \"org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat\",
        \"SerdeInfo\": {\"SerializationLibrary\": \"org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe\"}}
    }" >/dev/null 2>&1 || true
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
aws s3 cp "$WORK/dedupe.csv"                "${DEST}/dedupe.csv"                --only-show-errors
aws s3 cp "$WORK/latency_percentiles.csv"   "${DEST}/latency_percentiles.csv"   --only-show-errors
echo "wrote:"
aws s3 ls "${DEST}/" | sed 's|^|  |'
