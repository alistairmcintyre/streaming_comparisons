#!/usr/bin/env bash
# Point the LOCAL compose stack at the REAL S3 lake and Glue catalog.
#
# Source this, then bring the stack up as usual. Kafka, Postgres, Debezium and the
# generators stay local; only the lake moves. That is the tier between "everything on a
# laptop" and "a full EKS run", and it covers the things that only break against real
# AWS: Glue registration, Athena readability, Hudi timeline and rollback behaviour,
# S3 consistency and listing costs.
#
# What it does NOT cover, so do not read a clean run here as a green light:
#   IRSA credential behaviour, which is the WebIdentityTokenCredentialsProvider path and
#   simply does not exist off-cluster; node disruption; multi-executor distribution; and
#   sustained 1000/s throughput. Those need the cluster.
#
#   . scripts/lake-aws-env.sh && make up start-delta
set -u
# Must be SOURCED, and not through a pipe. `. lake-aws-env.sh | tee` runs it in a
# subshell and the exports vanish, leaving compose on its MinIO defaults while the
# banner says otherwise. Caught doing exactly that.
if [ "${BASH_SOURCE[0]}" = "$0" ]; then
  echo "source this, do not execute it:  . scripts/lake-aws-env.sh" >&2; exit 1
fi
_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
[ -f "$_root/env/aws.env" ] || { echo "env/aws.env missing; copy env/aws.example.env" >&2; return 1 2>/dev/null || exit 1; }
set -a; . "$_root/env/aws.env"; set +a

: "${AWS_ACCOUNT_ID:?set AWS_ACCOUNT_ID in env/aws.env}"
: "${AWS_REGION:=eu-west-1}"
BUCKET="${WAREHOUSE_BUCKET:-streaming-comparison-amc-warehouse}"

# Real S3 rather than MinIO: virtual-host addressing, TLS, and the default credential
# chain so an SSO session token is honoured. SimpleAWSCredentialsProvider takes only a
# key pair and would silently ignore the token, then fail as expired.
export S3A_ENDPOINT="s3.${AWS_REGION}.amazonaws.com"
export S3A_PATH_STYLE=false
export S3A_SSL=true
export S3A_CREDS_PROVIDER=software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
export AWS_REGION

# Resolve the SSO session into the temporary keys the containers will use. Nothing is
# written to disk; these live in this shell only.
eval "$(aws configure export-credentials --profile "${AWS_PROFILE:-streaming-comparisons}" --format env)" || {
  echo "could not export credentials; run: aws sso login --profile ${AWS_PROFILE:-streaming-comparisons}" >&2
  return 1 2>/dev/null || exit 1; }

# A per-user prefix, so this never writes where a benchmark run reads.
PREFIX="${LAKE_PREFIX:-_devlake/$(whoami)}"
export DELTA_WAREHOUSE="s3a://${BUCKET}/${PREFIX}/delta"
export HUDI_WAREHOUSE="s3a://${BUCKET}/${PREFIX}/hudi"
export CHECKPOINT_BASE="s3a://${BUCKET}/${PREFIX}/_chk"

echo "lake -> s3://${BUCKET}/${PREFIX}   region ${AWS_REGION}   creds: SSO session"
echo "kafka, postgres, debezium and the generators stay local"
echo "clean up with: aws s3 rm s3://${BUCKET}/${PREFIX}/ --recursive"
