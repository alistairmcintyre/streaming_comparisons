#!/bin/bash
# Submit all Flink SQL jobs via sql-client.
# Each job runs as a separate Flink application (separate job graph).
# Runs inside the flink-submitter container, after JM+TM are healthy.

set -e

FLINK_HOME=${FLINK_HOME:-/opt/flink}
JOB_DIR=/opt/flink-jobs
JM_HOST=${JOBMANAGER_RPC_ADDRESS:-flink-jobmanager}
JM_REST=${FLINK_JOBMANAGER_PORT:-8081}

echo "Waiting for Flink JobManager at http://${JM_HOST}:${JM_REST}..."
until curl -sf "http://${JM_HOST}:${JM_REST}/overview" > /dev/null; do
  echo "  JobManager not ready, retrying in 3s..."
  sleep 3
done
echo "JobManager is ready."

# Point sql-client at the remote JobManager. Flink 1.20's config.yaml is nested
# (jobmanager.rpc.address / rest.address live under nested maps), so the old
# flat-key sed silently no-ops. Instead write a minimal flat config — the
# submitter only builds and submits job graphs to the JM, it runs no tasks, so
# it needs nothing beyond the JM's REST address.
cat > "${FLINK_HOME}/conf/config.yaml" <<EOF
jobmanager.rpc.address: ${JM_HOST}
rest.address: ${JM_HOST}
rest.port: ${JM_REST}
EOF
echo "Wrote submitter config pointing sql-client at ${JM_HOST}:${JM_REST}"

# Run Flink DDL synchronously before submitting jobs.
# iceberg-flink-runtime is in lib/ so no --jar flag needed — passing it via
# --jar and having it in lib/ causes a ClassCastException from duplicate classloading.
SQL_CLIENT="${FLINK_HOME}/bin/sql-client.sh"

echo "Creating Flink tables..."
${SQL_CLIENT} -f /opt/ddl/create_tables_flink.sql
echo "Flink tables ready."

# sql-client in embedded mode runs SQL from a file and exits.
# Run each file in background so all jobs are submitted concurrently.
submit_job() {
  local sql_file=$1
  echo "Submitting: ${sql_file}"
  ${SQL_CLIENT} -f "${sql_file}" &
}

submit_job "${JOB_DIR}/bronze_customers.sql"
submit_job "${JOB_DIR}/silver_customers.sql"
submit_job "${JOB_DIR}/gold_customers_per_country.sql"
submit_job "${JOB_DIR}/direct/silver_customers.sql"
submit_job "${JOB_DIR}/direct/gold_customers_per_country.sql"

# Wait for all submission processes
wait
echo "All Flink jobs submitted."

# Keep container alive so docker compose doesn't restart it
tail -f /dev/null
