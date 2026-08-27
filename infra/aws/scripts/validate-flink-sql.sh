#!/usr/bin/env bash
# Compile the Flink SQL locally, before it can cost a cluster.
#
# WHY: two of 2026-08-27's failures were Flink SQL PLANNING errors that only appeared
# after ~25 minutes of cluster build, and both presented as a silently missing job:
#
#   TableException: Table sink 'latency_sink' doesn't support consuming update changes
#     which is produced by node GroupAggregate(...)          -> no gold job, either engine
#   RuntimeException: First row streaming reading is not supported. You can use
#     'lookup' or 'full-compaction' changelog producer       -> no paimon gold job
#
# Neither needs Kafka, S3, or AWS. They are thrown while the planner COMPILES the
# INSERT, before a job is submitted. So: run a throwaway Flink locally, point the
# Paimon catalog at a local filesystem warehouse, and submit the real SQL. A job that
# compiles and then dies at runtime (no Kafka here) still proves the SQL is valid —
# we only fail on planning/validation errors.
set -uo pipefail
IMG="${FLINK_PAIMON_IMAGE:-167217327348.dkr.ecr.eu-west-1.amazonaws.com/streaming-comparison/flink-paimon:latest}"
WORK=$(mktemp -d); trap 'rm -rf "$WORK"' EXIT

# Self-contained: the validate job has no AWS credentials, so if the ECR image is not
# already present locally, build it from the Dockerfile in this repo. That also pins the
# check to the CURRENT sources rather than whatever happens to sit in ECR — the same
# staleness trap that cost a day (DEPLOY_LOG #87) — and incidentally proves the
# Dockerfile still builds.
if ! docker image inspect "$IMG" >/dev/null 2>&1; then
  echo "  image not present locally — building from docker/flink-paimon/Dockerfile"
  IMG=flink-paimon-sqlcheck:local
  docker build -q -t "$IMG" -f docker/flink-paimon/Dockerfile docker/flink-paimon >/dev/null \
    || { echo "  could not build the validation image"; exit 1; }
fi

# Local stand-ins. Kafka is deliberately unreachable: creating a Kafka table does not
# connect, and the planner still validates the connector's CHANGELOG MODE — which is
# exactly what rejected latency_sink.
export PAIMON_WAREHOUSE="file:///tmp/paimon-validate"
export PAIMON_S3_OPTS=""
export PAIMON_ICEBERG_OPTS=""
export PAIMON_FULL_COMPACT_INTERVAL="1min"
export FLINK_CHECKPOINT_BASE="file:///tmp/paimon-validate/_chk"
export KAFKA_BOOTSTRAP="localhost:9092"
export KAFKA_EXTRA_OPTS=""
SUBST='${PAIMON_WAREHOUSE} ${PAIMON_S3_OPTS} ${PAIMON_ICEBERG_OPTS} ${PAIMON_FULL_COMPACT_INTERVAL} ${FLINK_CHECKPOINT_BASE} ${KAFKA_BOOTSTRAP} ${KAFKA_EXTRA_OPTS}'

# JOBS_DIR is parameterised so the test-suite can point this at a deliberately broken
# fixture and assert the checker FAILS. A checker nobody has watched fail is a checker
# nobody knows works — which is how "at least one job submitted" shipped and passed
# with 3 of 4.
JOBS_DIR="${JOBS_DIR:-jobs/flink-paimon}"
mkdir -p "$WORK/sql"
for f in "$JOBS_DIR"/*.sql; do
  envsubst "$SUBST" < "$f" > "$WORK/sql/$(basename "$f")"
done

cat > "$WORK/run.sh" <<'INNER'
set -u
export FLINK_HOME=/opt/flink
# A local cluster so the SQL client has somewhere to submit. Planning happens in the
# client and fails BEFORE submission, which is what we are testing.
"$FLINK_HOME/bin/start-cluster.sh" >/dev/null 2>&1
for i in $(seq 1 30); do curl -sf localhost:8081/overview >/dev/null 2>&1 && break; sleep 2; done

run_sql() {
  echo "=== $1 ==="
  "$FLINK_HOME/bin/sql-client.sh" -f "/sql/$1" 2>&1
}
run_sql create_tables.sql
# Each job file separately, so a file that submits NOTHING is visible. Counting jobs in
# aggregate hides exactly the failure this exists to catch: on 2026-08-27 paimon ran
# bronze + both silvers and silently never started gold, and "some jobs submitted"
# looked like success.
for f in bronze_trades.sql silver_trades.sql silver_accounts.sql gold_open_positions.sql; do
  [ -f "/sql/$f" ] || continue
  echo "@@@BEGIN $f@@@"
  run_sql "$f"
  echo "@@@END $f@@@"
done
INNER

echo "== compiling $JOBS_DIR/*.sql against a local Flink =="
docker run --rm -v "$WORK/sql:/sql:ro" -v "$WORK/run.sh:/run.sh:ro" \
  --entrypoint bash "$IMG" /run.sh > "$WORK/out.txt" 2>&1

# Planning/validation failures. Runtime failures (no Kafka broker here) are EXPECTED
# and must not fail the check — a job that compiled is a job whose SQL is valid.
# The SQL client ECHOES its input prefixed with '>' , and these files contain comments
# quoting the very errors we grep for (written to document them). Matching those would
# fail every run on its own documentation — the same trap as scanning YAML comments.
# Strip echoed input and SQL comments first, then look for real planner output.
PLAN_ERR=$(grep -vE '^\s*>|^\s*--' "$WORK/out.txt" \
           | grep -nE 'TableException|ValidationException|SqlParserException|Could not execute SQL' \
           | grep -viE 'kafka|broker|timeout|UnknownHost|Connection refused' || true)
if [ -n "$PLAN_ERR" ]; then
  echo "SQL PLANNING ERRORS — these would have cost a cluster:"
  echo "$PLAN_ERR" | head -20 | sed 's/^/  /'
  echo
  echo "(full output: rerun with KEEP=1)"; [ "${KEEP:-0}" = 1 ] && cp "$WORK/out.txt" ./flink-sql-validate.log
  exit 1
fi
# EVERY job file must submit at least one job. An aggregate count is not enough: it
# was ">= 1" first, and reintroducing the real changelog-producer bug still "passed"
# with 3 of 4 — the same silent hole the live run had.
FAILED=""
for f in bronze_trades.sql silver_trades.sql silver_accounts.sql gold_open_positions.sql; do
  grep -q "@@@BEGIN $f@@@" "$WORK/out.txt" || continue
  N=$(sed -n "/@@@BEGIN $f@@@/,/@@@END $f@@@/p" "$WORK/out.txt" | grep -c 'Job ID:')
  if [ "$N" -ge 1 ]; then
    printf '  ok       %-26s %s job(s)\n' "$f" "$N"
  else
    printf '  NO JOB   %-26s submitted nothing\n' "$f"
    sed -n "/@@@BEGIN $f@@@/,/@@@END $f@@@/p" "$WORK/out.txt" \
      | grep -vE '^\s*>|^\s*--' | grep -iE 'error|exception|not supported' | head -3 | sed 's/^/      /'
    FAILED="$FAILED $f"
  fi
done
if [ -n "$FAILED" ]; then
  echo; echo "these job files compile to NOTHING:$FAILED"
  [ "${KEEP:-0}" = 1 ] && cp "$WORK/out.txt" ./flink-sql-validate.log
  exit 1
fi
echo "flink SQL compiles — every job file produced a job"
