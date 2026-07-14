#!/bin/bash
set -e

# Delta Lake Spark configuration.
# Delta takes over spark_catalog (the default catalog).
# Tables are addressed by their s3a:// path; no warehouse catalog needed.
# Hadoop S3A block is required for both Delta table paths and checkpoint paths.

exec /opt/spark/bin/spark-submit \
  --master "local[2]" \
  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
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
  --conf "spark.databricks.delta.retentionDurationCheck.enabled=false" \
  "${JOB_FILE}"
