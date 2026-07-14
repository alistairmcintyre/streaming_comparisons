#!/bin/bash
set -e

# Hudi Spark configuration.
# KryoSerializer and HoodieSparkKryoRegistrar are mandatory for Hudi.
# Hudi takes over spark_catalog like Delta.
# Tables are path-addressed (s3a://); no warehouse catalog needed.

exec /opt/spark/bin/spark-submit \
  --master "local[2]" \
  --conf "spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension" \
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog" \
  --conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" \
  --conf "spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar" \
  --conf "spark.hadoop.fs.s3a.endpoint=${MINIO_ENDPOINT:-http://minio:9000}" \
  --conf "spark.hadoop.fs.s3a.path.style.access=true" \
  --conf "spark.hadoop.fs.s3a.connection.ssl.enabled=false" \
  --conf "spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider" \
  --conf "spark.hadoop.fs.s3a.access.key=${AWS_ACCESS_KEY_ID:-minioadmin}" \
  --conf "spark.hadoop.fs.s3a.secret.key=${AWS_SECRET_ACCESS_KEY:-minioadmin}" \
  --conf "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem" \
  --conf "spark.hadoop.fs.s3a.endpoint.region=${AWS_REGION:-us-east-1}" \
  --conf "spark.sql.adaptive.enabled=false" \
  --conf "hoodie.embed.timeline.server=false" \
  "${JOB_FILE}"
