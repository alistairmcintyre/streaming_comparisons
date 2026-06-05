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

# Write RPC address into config so sql-client picks it up.
# The submitter uses a custom entrypoint that bypasses the Flink docker
# entrypoint which normally processes FLINK_PROPERTIES into the config file.
for CONF_FILE in "${FLINK_HOME}/conf/flink-conf.yaml" "${FLINK_HOME}/conf/config.yaml"; do
  if [ -f "${CONF_FILE}" ]; then
    sed -i "s/^jobmanager.rpc.address:.*/jobmanager.rpc.address: ${JM_HOST}/" "${CONF_FILE}"
    sed -i "s/^rest.address:.*/rest.address: ${JM_HOST}/" "${CONF_FILE}"
    echo "Set jobmanager.rpc.address and rest.address to ${JM_HOST} in ${CONF_FILE}"
  fi
done

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

submit_job "${JOB_DIR}/bronze_item_attributes.sql"
submit_job "${JOB_DIR}/silver_item_attributes.sql"
submit_job "${JOB_DIR}/silver_item_attributes_v2.sql"
submit_job "${JOB_DIR}/gold_item_category_count.sql"
submit_job "${JOB_DIR}/gold_item_category_count_v2.sql"

# Wait for all submission processes
wait
echo "All Flink jobs submitted."

# Keep container alive so docker compose doesn't restart it
tail -f /dev/null
