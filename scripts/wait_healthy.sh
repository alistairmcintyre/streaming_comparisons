#!/bin/bash
# Wait for all critical services to be healthy before proceeding.
# Run this after 'make up' and before 'make create-tables'.

set -e

check() {
  local name=$1
  local url=$2
  echo -n "Waiting for ${name}..."
  until curl -sf "${url}" > /dev/null 2>&1; do
    echo -n "."
    sleep 2
  done
  echo " OK"
}

check "Iceberg REST catalog" "http://localhost:8181/v1/config"
check "Kafka Connect"        "http://localhost:8083/connectors"
check "MinIO"                "http://localhost:9000/minio/health/live"

# Only wait for Flink if it is actually running
if docker ps --format '{{.Names}}' | grep -q '^flink-jobmanager$'; then
  check "Flink JobManager" "http://localhost:8081/overview"
else
  echo "Flink JobManager not started — skipping"
fi

echo ""
echo "All services healthy."
