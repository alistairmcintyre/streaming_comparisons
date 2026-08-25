#!/usr/bin/env bash
# End-to-end integration test for the customers pipelines.
#
# Seeds a fixed CDC dataset into Postgres — 10 inserts, 2 country updates
# (customers 3,4 → SG), 1 GDPR delete (customer 10) — runs a chosen stack's
# bronze→silver pipeline, and asserts the silver current view matches the oracle:
#   9 customers (10 deleted); per-country DE=2 FR=2 GB=2 SG=2 US=1.
#
# The assertion is deterministic ONLY against exactly that dataset, so run with
# --clean (wipes volumes first) and with the generators stopped.
#
# Usage:
#   scripts/integration_test.sh [--clean] [paimon|flink-iceberg|flink-paimon|delta|hudi]   (default: paimon)
#
# Exit code 0 = PASS, non-zero = FAIL. This has caught real bugs (commons-pool2
# version, event_ts delete-dedup, flink java17, iceberg-rest image) — keep it green.
set -uo pipefail
cd "$(dirname "$0")/.."

CLEAN=0
[ "${1:-}" = "--clean" ] && { CLEAN=1; shift; }
STACK="${1:-paimon}"
TEST_DIR="$(pwd)/scripts/integration"

echo "### integration test: stack=$STACK clean=$CLEAN"

if [ "$CLEAN" = 1 ]; then
  echo "### wiping volumes (down -v)"; docker compose down -v
fi

echo "### bring up infra"
docker compose up -d postgres-app postgres-catalog kafka minio mc-init iceberg-rest kafka-connect
for _ in $(seq 1 48); do
  ch=$(docker compose ps kafka-connect --format '{{.Status}}' 2>/dev/null)
  ih=$(docker compose ps iceberg-rest --format '{{.Status}}' 2>/dev/null)
  echo "$ch" | grep -q healthy && echo "$ih" | grep -q healthy && { echo "### infra healthy"; break; }
  sleep 5
done

echo "### register Debezium connector"
bash scripts/register_connectors.sh >/dev/null 2>&1 || true

cnt=$(docker compose exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh \
      --bootstrap-server localhost:9092 --topic app.public.customers 2>/dev/null \
      | awk -F: '{s+=$3} END{print s+0}')
if [ "${cnt:-0}" -lt 13 ]; then
  echo "### seeding fixed dataset (10 inserts, 2 updates, 1 delete)"
  docker compose exec -T postgres-app psql -U app -d appdb -c "
    INSERT INTO customers (customer_id,name,country,segment) VALUES
     (1,'A','GB','retail'),(2,'B','GB','premier'),(3,'C','US','retail'),(4,'D','US','business'),
     (5,'E','DE','retail'),(6,'F','DE','wealth'),(7,'G','FR','retail'),(8,'H','FR','premier'),
     (9,'I','US','retail'),(10,'J','GB','retail') ON CONFLICT (customer_id) DO NOTHING;
    UPDATE customers SET country='SG',updated_at=now() WHERE customer_id IN (3,4);
    DELETE FROM customers WHERE customer_id=10;"
else
  echo "### kafka already has $cnt customer events — reusing (use --clean for a deterministic run)"
fi

run_integration_test() {  # $1 = image service, $2 = integration test script name
  docker compose run --rm -v "$TEST_DIR":/opt/integration \
    -e JOB_FILE="/opt/integration/$2" "$1" 2>&1 | grep -vE 'INFO|WARN|^[0-9]{2}/[0-9]{2}'
  return "${PIPESTATUS[0]}"
}

case "$STACK" in
  paimon)
    echo "### start spark-paimon bronze+silver"
    docker compose up -d paimon-bronze-customers paimon-silver-customers
    echo "### waiting 120s for processing"; sleep 120
    run_integration_test paimon-bronze-customers integration_test_paimon.py; rc=$?
    docker compose stop paimon-bronze-customers paimon-silver-customers >/dev/null 2>&1
    ;;
  flink-iceberg)
    echo "### start flink-iceberg cluster + submitter (creates flink tables, submits 5 jobs)"
    docker compose up -d flink-jobmanager flink-taskmanager; sleep 18
    docker compose up -d flink-submitter
    echo "### waiting 185s for submit + process"; sleep 185
    docker compose stop flink-jobmanager flink-taskmanager flink-submitter >/dev/null 2>&1
    run_integration_test spark-bronze-customers integration_test_flink_iceberg.py; rc=$?
    ;;
  flink-paimon)
    echo "### start flink-paimon cluster + submitter (creates paimon tables, submits bronze/silver/gold)"
    docker compose up -d flink-paimon-jobmanager flink-paimon-taskmanager; sleep 18
    docker compose up -d flink-paimon-submitter
    echo "### waiting 240s for submit + bronze→silver→gold (changelog-producer=lookup + 1min compaction)"; sleep 240
    docker compose stop flink-paimon-jobmanager flink-paimon-taskmanager flink-paimon-submitter >/dev/null 2>&1
    run_integration_test paimon-bronze-customers integration_test_gold_paimon.py; rc=$?
    ;;
  delta)
    echo "### start spark-delta ddl-init + bronze/silver/gold"
    docker compose up -d ddl-init-delta; echo "### waiting 15s for delta DDL init"; sleep 15
    docker compose up -d delta-bronze-customers delta-silver-customers delta-gold-customers
    echo "### waiting 200s for bronze→silver→gold (30s micro-batch triggers)"; sleep 200
    docker compose stop delta-bronze-customers delta-silver-customers delta-gold-customers >/dev/null 2>&1
    run_integration_test delta-bronze-customers integration_test_delta.py; rc=$?
    ;;
  hudi)
    echo "### start spark-hudi bronze/silver/gold"
    docker compose up -d hudi-bronze-customers hudi-silver-customers hudi-gold-customers
    echo "### waiting 200s for bronze→silver→gold (30s micro-batch triggers)"; sleep 200
    docker compose stop hudi-bronze-customers hudi-silver-customers hudi-gold-customers >/dev/null 2>&1
    run_integration_test hudi-bronze-customers integration_test_hudi.py; rc=$?
    ;;
  *)
    echo "unknown stack: $STACK (supported: paimon, flink-iceberg, flink-paimon, delta, hudi)"; exit 2
    ;;
esac

echo "### integration test result: rc=$rc"
[ "$rc" = 0 ] && echo "### ✅ PASS" || echo "### ❌ FAIL"
exit "$rc"
