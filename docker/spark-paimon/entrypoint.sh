#!/bin/bash
set -e

# Paimon Spark configuration.
# Paimon uses a named catalog (paimon) — does not override spark_catalog,
# so it coexists cleanly with other formats if needed.
# Paimon table paths use s3:// via its own paimon-s3 plugin (not s3a://).
# S3A is still needed for streaming checkpoint paths (s3a://warehouse/_chk/).
# Paimon S3 credentials are set under spark.sql.catalog.paimon.s3.* keys.

exec /opt/spark/bin/spark-submit \
  --master "local[2]" \
  --conf "spark.sql.extensions=org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions" \
  --conf "spark.sql.catalog.paimon=org.apache.paimon.spark.SparkCatalog" \
  --conf "spark.sql.catalog.paimon.warehouse=s3://warehouse/paimon" \
  --conf "spark.sql.catalog.paimon.s3.endpoint=${MINIO_ENDPOINT:-http://minio:9000}" \
  --conf "spark.sql.catalog.paimon.s3.path-style-access=true" \
  --conf "spark.sql.catalog.paimon.s3.access-key=${AWS_ACCESS_KEY_ID:-minioadmin}" \
  --conf "spark.sql.catalog.paimon.s3.secret-key=${AWS_SECRET_ACCESS_KEY:-minioadmin}" \
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
