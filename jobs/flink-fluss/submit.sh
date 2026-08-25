#!/bin/bash
# Submit the Fluss pipeline: create tables → bronze (Kafka→Fluss) → gold fold →
# start the Fluss→Paimon tiering service (which exposes the Iceberg view).
#
# The .sql files are TEMPLATES with ${...} placeholders, rendered with envsubst
# for the active DEPLOY_ENV. Scalars come from env/<env>.env (docker-compose
# env_file, or `source` by hand); the env-specific fragments are built here.
#
# LOCAL: Paimon warehouse on MinIO, Iceberg via hadoop-catalog, plain Kafka.
# AWS:   Paimon warehouse on S3 (IAM), Iceberg via hive-catalog → Glue so Athena
#        can query it, Kafka via MSK IAM. S3 remote data works on real S3 (STS).
set -e

FLINK_HOME=${FLINK_HOME:-/opt/flink}
JOB_DIR=${JOB_DIR:-/opt/sql}
RENDER_DIR=/tmp/rendered
JM_HOST=${JOBMANAGER_RPC_ADDRESS:-fluss-flink-jobmanager}
JM_REST=${FLINK_JOBMANAGER_PORT:-8081}
DEPLOY_ENV=${DEPLOY_ENV:-local}

echo "Waiting for Flink JobManager at http://${JM_HOST}:${JM_REST}..."
until curl -sf "http://${JM_HOST}:${JM_REST}/overview" > /dev/null; do sleep 3; done
echo "JobManager is ready."

# NOTE: this overwrites the image's config.yaml, so we must re-add the Java 17
# module opens Flink needs — the tiering `flink run` builds a DataStream job graph
# in this client JVM and hits InaccessibleObjectException without them (the Table
# API sql-client jobs don't need them, but the tiering job does).
cat > "${FLINK_HOME}/conf/config.yaml" <<EOF
jobmanager.rpc.address: ${JM_HOST}
rest.address: ${JM_HOST}
rest.port: ${JM_REST}
env.java.opts.all: --add-exports=java.base/sun.net.util=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.text=ALL-UNNAMED --add-opens=java.base/java.time=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.locks=ALL-UNNAMED
EOF

# ── Lake env (where the Paimon warehouse + Iceberg catalog live) ────────────
if [ "${DEPLOY_ENV}" = "aws" ]; then
  # Register the tiered Paimon tables' Iceberg metadata in Glue → Athena reads them.
  FLUSS_ICEBERG_OPTS=",
    'paimon.metadata.iceberg.storage' = 'hive-catalog',
    'paimon.metadata.iceberg.hive-client-class' = 'com.amazonaws.glue.catalog.metastore.AWSCatalogMetastoreClient',
    'paimon.metadata.iceberg.manifest-legacy-version' = 'true'"
  # paimon-s3 needs static keys (no IRSA) — injected from SSM via env (S3_ACCESS_KEY/
  # S3_SECRET_KEY). Real S3 → no endpoint/path-style.
  DATALAKE_S3_ARGS="--datalake.paimon.s3.region ${AWS_REGION} \
    --datalake.paimon.s3.access-key ${S3_ACCESS_KEY} \
    --datalake.paimon.s3.secret-key ${S3_SECRET_KEY}"
else
  FLUSS_ICEBERG_OPTS=",
    'paimon.metadata.iceberg.storage' = 'hadoop-catalog'"
  DATALAKE_S3_ARGS="--datalake.paimon.s3.endpoint ${S3_ENDPOINT} \
    --datalake.paimon.s3.access-key ${S3_ACCESS_KEY} \
    --datalake.paimon.s3.secret-key ${S3_SECRET_KEY} \
    --datalake.paimon.s3.path.style.access ${S3_PATH_STYLE:-true} \
    --datalake.paimon.s3.region ${AWS_REGION}"
fi

# ── Kafka auth (decoupled from the lake env) — none | scram | msk_iam ────────
# 'none' = PLAINTEXT, for OSS Kafka in-cluster (Strimzi/AutoMQ on EKS). Cheapest
# path for load testing — see jobs/flink-fluss/PHEASE_1_README.md. 'msk_iam' only for MSK.
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
export FLUSS_BOOTSTRAP FLUSS_ICEBERG_OPTS KAFKA_BOOTSTRAP KAFKA_EXTRA_OPTS

SUBST='${FLUSS_BOOTSTRAP} ${FLUSS_ICEBERG_OPTS} ${KAFKA_BOOTSTRAP} ${KAFKA_EXTRA_OPTS}'
mkdir -p "${RENDER_DIR}"
render() { envsubst "${SUBST}" < "${JOB_DIR}/$1" > "${RENDER_DIR}/$1"; }
SQL_CLIENT="${FLINK_HOME}/bin/sql-client.sh"

# create_tables: retry until the coordinator's event processor is initialised
# (after a fresh/recreated coordinator this can lag ~40s — see README).
render "create_tables.sql"
echo "Creating Fluss tables (DEPLOY_ENV=${DEPLOY_ENV})..."
for attempt in $(seq 1 24); do
  out=$(${SQL_CLIENT} -f "${RENDER_DIR}/create_tables.sql" 2>&1) || true
  echo "$out" | grep -q 'not initialized' && { echo "  coordinator warming ($attempt)..."; sleep 6; continue; }
  echo "$out" | grep -qiE 'error|exception' && { echo "  error:"; echo "$out" | grep -iE 'error|exception' | tail -2; sleep 6; continue; }
  echo "  create_tables OK"; break
done

submit_job() { render "$1"; echo "Submitting: $1"; ${SQL_CLIENT} -f "${RENDER_DIR}/$1" & }
submit_job "bronze_trades.sql"
submit_job "gold_open_positions.sql"
wait
echo "Bronze + gold submitted."

# ── Fluss → Paimon lakehouse tiering service ────────────────────────────────
TIERING_JAR=$(ls ${FLINK_HOME}/opt/fluss-flink-tiering-*.jar | head -1)
echo "Starting tiering service: ${TIERING_JAR}"
"${FLINK_HOME}/bin/flink" run -d "${TIERING_JAR}" \
  --fluss.bootstrap.servers "${FLUSS_BOOTSTRAP}" \
  --datalake.format "${DATALAKE_FORMAT:-paimon}" \
  --datalake.paimon.metastore filesystem \
  --datalake.paimon.warehouse "${FLUSS_PAIMON_WAREHOUSE}" \
  ${DATALAKE_S3_ARGS}
echo "All Fluss jobs submitted."

tail -f /dev/null
