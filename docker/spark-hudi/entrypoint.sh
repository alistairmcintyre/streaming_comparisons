#!/bin/bash
set -e
# On Kubernetes the Spark Operator runs this image as `driver`/`executor` and passes
# every setting via --properties-file. Hand those args to Spark's own k8s entrypoint:
# without this, the compose-oriented spark-submit below swallows them and dies with
# "Failed to get main class in JAR ... /opt/spark/work-dir (Is a directory)", and it
# would also force --master local[2] plus MinIO creds. Compose passes no args and
# drives the run through JOB_FILE, so it falls through to the block below unchanged.
case "$1" in
  driver|executor|driver-py|driver-r) exec /opt/entrypoint.sh "$@" ;;
esac


# Hudi Spark configuration.
# KryoSerializer and HoodieSparkKryoRegistrar are mandatory for Hudi.
# Hudi takes over spark_catalog like Delta.
# Tables are path-addressed (s3a://); no warehouse catalog needed.

# S3A is configured entirely from the environment so the same image can write to MinIO
# or to real S3 without a rebuild or a branch in here. Defaults are MinIO, which is what
# `make up` gives you. Setting S3A_* for real S3 is the "lake on AWS" mode: see the
# README, it exists so catalog, timeline and Athena behaviour can be tested for pennies
# instead of a cluster.
exec /opt/spark/bin/spark-submit \
  --master "local[2]" \
  --conf "spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension" \
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog" \
  --conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" \
  --conf "spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar" \
  --conf "spark.hadoop.fs.s3a.endpoint=${S3A_ENDPOINT:-http://minio:9000}" \
  --conf "spark.hadoop.fs.s3a.path.style.access=${S3A_PATH_STYLE:-true}" \
  --conf "spark.hadoop.fs.s3a.connection.ssl.enabled=${S3A_SSL:-false}" \
  --conf "spark.hadoop.fs.s3a.aws.credentials.provider=${S3A_CREDS_PROVIDER:-org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider}" \
  --conf "spark.hadoop.fs.s3a.access.key=${AWS_ACCESS_KEY_ID:-minioadmin}" \
  --conf "spark.hadoop.fs.s3a.secret.key=${AWS_SECRET_ACCESS_KEY:-minioadmin}" \
  --conf "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem" \
  --conf "spark.hadoop.fs.s3a.endpoint.region=${AWS_REGION:-us-east-1}" \
  # 8, not Spark's default of 200. The silver and gold executors run 1 core, so 200
  # shuffle partitions means 200 serialised tasks per shuffle stage on tiny data, and
  # AQE cannot coalesce them because Spark disables AQE for streaming queries outright.
  # Measured honestly: this is tidying, not the fix for slow batches. 200 vs 4 was 12933
  # vs 12525 ms against real S3, a 3% difference; the MERGE is 54% of a batch.
  --conf "spark.sql.shuffle.partitions=${SHUFFLE_PARTITIONS:-8}" \
  --conf "spark.sql.adaptive.enabled=false" \
  --conf "hoodie.embed.timeline.server=false" \
  "${JOB_FILE}"
