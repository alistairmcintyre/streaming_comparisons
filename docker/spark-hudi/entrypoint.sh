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
