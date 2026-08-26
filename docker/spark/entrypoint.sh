#!/bin/bash
set -e
# On Kubernetes the Spark Operator runs this image as `driver`/`executor` and passes
# every setting via --properties-file. Hand those args to Spark's own k8s entrypoint:
# without this, the compose-oriented spark-submit below swallows them and dies with
# "Failed to get main class in JAR ... /opt/spark/work-dir (Is a directory)" — and it
# would also force --master local[2] plus MinIO creds. Compose passes NO args and
# drives the run through JOB_FILE, so it falls through to the block below unchanged.
case "$1" in
  driver|executor|driver-py|driver-r) exec /opt/entrypoint.sh "$@" ;;
esac


# Shared Spark catalog + S3A configuration passed to every job via spark-submit.
# Two separate S3 config blocks are required:
#   1. Iceberg S3FileIO (catalog-level) — used for data files
#   2. Hadoop S3A (hadoop-level) — used for streaming checkpoints

exec /opt/spark/bin/spark-submit \
  --master "local[2]" \
  --conf "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions" \
  --conf "spark.sql.catalog.rest=org.apache.iceberg.spark.SparkCatalog" \
  --conf "spark.sql.catalog.rest.type=rest" \
  --conf "spark.sql.catalog.rest.uri=${ICEBERG_REST_URI:-http://iceberg-rest:8181}" \
  --conf "spark.sql.catalog.rest.warehouse=${ICEBERG_WAREHOUSE:-s3://warehouse/}" \
  --conf "spark.sql.catalog.rest.io-impl=org.apache.iceberg.aws.s3.S3FileIO" \
  --conf "spark.sql.catalog.rest.s3.endpoint=${MINIO_ENDPOINT:-http://minio:9000}" \
  --conf "spark.sql.catalog.rest.s3.path-style-access=true" \
  --conf "spark.sql.catalog.rest.s3.access-key-id=${AWS_ACCESS_KEY_ID:-minioadmin}" \
  --conf "spark.sql.catalog.rest.s3.secret-access-key=${AWS_SECRET_ACCESS_KEY:-minioadmin}" \
  --conf "spark.sql.defaultCatalog=rest" \
  --conf "spark.hadoop.fs.s3a.endpoint=${MINIO_ENDPOINT:-http://minio:9000}" \
  --conf "spark.hadoop.fs.s3a.path.style.access=true" \
  --conf "spark.hadoop.fs.s3a.connection.ssl.enabled=false" \
  --conf "spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider" \
  --conf "spark.hadoop.fs.s3a.access.key=${AWS_ACCESS_KEY_ID:-minioadmin}" \
  --conf "spark.hadoop.fs.s3a.secret.key=${AWS_SECRET_ACCESS_KEY:-minioadmin}" \
  --conf "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem" \
  --conf "spark.hadoop.fs.s3a.endpoint.region=${AWS_REGION:-us-east-1}" \
  --conf "spark.sql.adaptive.enabled=false" \
  --conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" \
  "${JOB_FILE}"
