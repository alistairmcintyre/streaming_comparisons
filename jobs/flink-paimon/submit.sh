#!/bin/bash
# Submit the Flink Paimon SQL jobs (bronze → silver → gold).
#
# The .sql files are TEMPLATES with ${...} placeholders. This script renders
# them with envsubst for the active DEPLOY_ENV, then submits each as a separate
# Flink job via sql-client. Scalar env vars come from env/<env>.env (provided by
# docker-compose env_file, or `source` when run by hand); the environment-specific
# SQL fragments are built here from those scalars.
set -e

FLINK_HOME=${FLINK_HOME:-/opt/flink}
JOB_DIR=/opt/flink-jobs
RENDER_DIR=/tmp/rendered
JM_HOST=${JOBMANAGER_RPC_ADDRESS:-flink-paimon-jobmanager}
JM_REST=${FLINK_JOBMANAGER_PORT:-8081}
DEPLOY_ENV=${DEPLOY_ENV:-local}

echo "Waiting for Flink JobManager at http://${JM_HOST}:${JM_REST}..."
until curl -sf "http://${JM_HOST}:${JM_REST}/overview" > /dev/null; do
  echo "  JobManager not ready, retrying in 3s..."
  sleep 3
done
echo "JobManager is ready."

# Point sql-client at the remote JobManager. Flink 1.20's config.yaml is nested,
# so the old flat-key sed no-ops; write a minimal flat config instead (the
# submitter only submits job graphs to the JM, it runs no tasks).
cat > "${FLINK_HOME}/conf/config.yaml" <<EOF
jobmanager.rpc.address: ${JM_HOST}
rest.address: ${JM_HOST}
rest.port: ${JM_REST}
EOF

# ── Build environment-specific SQL fragments ────────────────────────────────
if [ "${DEPLOY_ENV}" = "aws" ]; then
  # paimon-s3 needs static keys (it can't use the IRSA IAM-role chain), injected
  # from SSM via env (S3_ACCESS_KEY/S3_SECRET_KEY). Real S3 → no endpoint/path-style.
  PAIMON_S3_OPTS=",
    's3.access-key' = '${S3_ACCESS_KEY}',
    's3.secret-key' = '${S3_SECRET_KEY}'"
  # Iceberg metadata written NEXT TO the data in S3 (hadoop-catalog). The
  # hive-catalog/Glue committer needs Hive-metastore + Glue-client jars that the
  # flink-paimon image does not carry -> every commit died with
  # NoClassDefFoundError org/apache/hadoop/hive/metastore/api/NoSuchObjectException.
  # Athena reads these via a Glue table pointing at metadata_location, registered
  # externally by infra/aws/scripts/register-glue-tables.sh, because in-writer Glue
  # registration (hive-catalog) needs com.amazonaws.glue...AWSCatalogMetastoreClient,
  # which AWS does not publish to Maven Central, and Paimon 1.4.2 ships only
  # IcebergHiveMetadataCommitter.
  # RETENTION MATTERS: Paimon rotates vN.metadata.json and deletes old ones by
  # default, so a pinned metadata_location goes stale within SECONDS on a live table
  # and Athena fails with ICEBERG_MISSING_METADATA. Keep the old versions so an
  # external pointer stays valid.
  PAIMON_ICEBERG_OPTS=",
    'metadata.iceberg.storage' = 'hadoop-catalog',
    'metadata.iceberg.manifest-legacy-version' = 'true',
    'metadata.iceberg.delete-after-commit.enabled' = 'false',
    'metadata.iceberg.previous-versions-max' = '1000'"
else
  PAIMON_S3_OPTS=",
    's3.endpoint' = '${S3_ENDPOINT:-http://minio:9000}',
    's3.path.style.access' = 'true',
    's3.access-key' = '${S3_ACCESS_KEY:-minioadmin}',
    's3.secret-key' = '${S3_SECRET_KEY:-minioadmin}'"
  # Write Iceberg metadata locally (hadoop-catalog) so the Athena-compat path is
  # exercised even without Glue.
  PAIMON_ICEBERG_OPTS=",
    'metadata.iceberg.storage' = 'hadoop-catalog'"
fi

# ── Kafka auth (decoupled from lake env), none | scram | msk_iam ────────────
# 'none' = PLAINTEXT for OSS Kafka in-cluster (Strimzi/AutoMQ on EKS); cheapest
# for load testing. 'msk_iam' only if you actually run MSK.
case "${KAFKA_AUTH:-none}" in
  msk_iam)
    KAFKA_EXTRA_OPTS=",
    'properties.security.protocol' = 'SASL_SSL',
    'properties.sasl.mechanism' = 'AWS_MSK_IAM',
    'properties.sasl.jaas.config' = 'software.amazon.msk.auth.iam.IAMLoginModule required;',
    'properties.sasl.client.callback.handler.class' = 'software.amazon.msk.auth.iam.IAMClientCallbackHandler'" ;;
  scram)
    KAFKA_EXTRA_OPTS=",
    'properties.security.protocol' = '${KAFKA_SECURITY_PROTOCOL:-SASL_PLAINTEXT}',
    'properties.sasl.mechanism' = '${KAFKA_SASL_MECHANISM:-SCRAM-SHA-512}',
    'properties.sasl.jaas.config' = 'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"${KAFKA_USER}\" password=\"${KAFKA_PASSWORD}\";'" ;;
  *)
    KAFKA_EXTRA_OPTS="" ;;
esac

export PAIMON_WAREHOUSE PAIMON_S3_OPTS PAIMON_ICEBERG_OPTS PAIMON_FULL_COMPACT_INTERVAL \
       FLINK_CHECKPOINT_BASE KAFKA_BOOTSTRAP KAFKA_EXTRA_OPTS

# Only substitute our known vars; leave any other $ in the SQL untouched.
SUBST_VARS='${PAIMON_WAREHOUSE} ${PAIMON_S3_OPTS} ${PAIMON_ICEBERG_OPTS} ${PAIMON_FULL_COMPACT_INTERVAL} ${FLINK_CHECKPOINT_BASE} ${KAFKA_BOOTSTRAP} ${KAFKA_EXTRA_OPTS}'
mkdir -p "${RENDER_DIR}"
render() { envsubst "${SUBST_VARS}" < "${JOB_DIR}/$1" > "${RENDER_DIR}/$1"; }

SQL_CLIENT="${FLINK_HOME}/bin/sql-client.sh"

echo "Creating Paimon tables (DEPLOY_ENV=${DEPLOY_ENV})..."
render "create_tables.sql"
${SQL_CLIENT} -f "${RENDER_DIR}/create_tables.sql"
echo "Paimon tables ready."

submit_job() {
  render "$1"
  echo "Submitting: $1"
  ${SQL_CLIENT} -f "${RENDER_DIR}/$1" &
}

submit_job "bronze_trades.sql"
submit_job "silver_trades.sql"
submit_job "silver_accounts.sql"
submit_job "gold_open_positions.sql"

wait
echo "All Flink Paimon jobs submitted."

# Keep the container alive so docker compose doesn't restart it.
tail -f /dev/null
