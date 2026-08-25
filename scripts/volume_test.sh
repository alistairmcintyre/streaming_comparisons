#!/usr/bin/env bash
# Volume / soak test — does the Delta gold streaming-read survive a silver that
# emits genuine MERGE update/delete commits under continuous load?
#
# The integration test's one-shot 13-event backfill folds into a SINGLE silver
# insert commit, so gold's readStream on silver never meets an update/delete
# commit. This runs the CONTINUOUS generator (25% insert / 65% update / 10% delete
# against customer_id 1..1000) against a 1000-row base, so silver keeps emitting
# MERGE UPDATE/DELETE commits after the initial insert. Delta's streaming source
# rejects those by default: "Detected a data update ... set ignoreChanges".
#
# Usage: scripts/volume_test.sh [--clean] [duration_seconds] [events_per_sec]
#   defaults: duration=240s, events_per_sec=50
# Signal: gold job Exited/Restarting or logs "Detected a data update" => the
#         streaming-read-of-MERGE-silver problem is CONFIRMED at volume.
set -uo pipefail
cd "$(dirname "$0")/.."

CLEAN=0; [ "${1:-}" = "--clean" ] && { CLEAN=1; shift; }
DURATION="${1:-240}"; EPS="${2:-50}"; BASE_ROWS=1000
TEST_DIR="$(pwd)/scripts/integration"
export CUSTOMERS_EVENTS_PER_SEC="$EPS"

echo "### volume test: delta, duration=${DURATION}s, ${EPS} evt/s, base=${BASE_ROWS} rows"
[ "$CLEAN" = 1 ] && { echo "### down -v"; docker compose down -v; }

echo "### infra"
docker compose up -d postgres-app postgres-catalog kafka minio mc-init iceberg-rest kafka-connect
for _ in $(seq 1 48); do
  docker compose ps kafka-connect --format '{{.Status}}' 2>/dev/null | grep -q healthy \
   && docker compose ps iceberg-rest --format '{{.Status}}' 2>/dev/null | grep -q healthy && break
  sleep 5
done
bash scripts/register_connectors.sh >/dev/null 2>&1 || true

echo "### seed ${BASE_ROWS} base customers (matches generator's MAX_CUSTOMER_ID)"
docker compose exec -T postgres-app psql -U app -d appdb -c "
  INSERT INTO customers (customer_id, name, country, segment)
  SELECT i, 'Cust-'||i,
    (ARRAY['GB','US','DE','FR','ES','IE','IN','SG'])[floor(random()*8+1)],
    (ARRAY['retail','premier','business','wealth'])[floor(random()*4+1)]
  FROM generate_series(1,${BASE_ROWS}) AS s(i) ON CONFLICT DO NOTHING;"

echo "### start delta bronze/silver/gold"
docker compose up -d ddl-init-delta; echo "### 15s for delta DDL init"; sleep 15
docker compose up -d delta-bronze-customers delta-silver-customers delta-gold-customers
echo "### 45s to let the base flow bronze->silver (first insert commit)"; sleep 45

echo "### start CONTINUOUS generator at ${EPS} evt/s (updates/deletes existing rows)"
docker compose up -d generator-customers

echo "### running ${DURATION}s under load..."; sleep "$DURATION"

echo "### === gold job state (Exited/Restarting = crashed = hypothesis confirmed) ==="
docker compose ps delta-gold-customers --format 'table {{.Name}}\t{{.State}}\t{{.Status}}'
echo "### === gold log scan for the streaming-read failure ==="
docker compose logs delta-gold-customers 2>&1 \
  | grep -iE 'Detected a data update|UnsupportedOperation|ignoreChanges|skipChangeCommits|Query .* terminated|StreamingQueryException' \
  | tail -15 || true

echo "### stop generator; settle 40s"
docker compose stop generator-customers >/dev/null 2>&1; sleep 40

echo "### === correctness + silver commit-history check ==="
docker compose run --rm -v "$TEST_DIR":/opt/integration \
  -e JOB_FILE=/opt/integration/volume_check_delta.py delta-bronze-customers 2>&1 \
  | grep -vE 'INFO|WARN|^[0-9]{2}/[0-9]{2}'
rc=${PIPESTATUS[0]}

docker compose stop delta-bronze-customers delta-silver-customers delta-gold-customers >/dev/null 2>&1
echo "### volume test done (checker rc=$rc)"
exit "$rc"
