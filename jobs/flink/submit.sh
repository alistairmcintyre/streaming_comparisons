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

# sql-client in embedded mode runs SQL from a file and exits.
# Run each file in background so all jobs are submitted concurrently.
submit_job() {
  local sql_file=$1
  echo "Submitting: ${sql_file}"
  ${FLINK_HOME}/bin/sql-client.sh \
    --jar ${FLINK_HOME}/lib/iceberg-flink-runtime-1.18-1.5.2.jar \
    -f "${sql_file}" &
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
