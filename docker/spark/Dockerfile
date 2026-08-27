FROM apache/spark:4.0.4

USER root

# Spark 4.0.x → Scala 2.13, bundled Hadoop 3.4.1, AWS SDK v2.
ENV ICEBERG_VERSION=1.11.0 \
    SPARK_VERSION=4.0 \
    SCALA_VERSION=2.13 \
    KAFKA_SPARK_VERSION=4.0.4 \
    KAFKA_CLIENTS_VERSION=3.9.1 \
    HADOOP_AWS_VERSION=3.4.1 \
    AWS_SDK_BUNDLE_VERSION=2.31.78

# ── Iceberg + AWS bundle (S3FileIO) + Kafka connector JARs ─────────────────
# iceberg-aws-bundle provides the AWS SDK v2 for Iceberg's S3FileIO; hadoop-aws
# + the standalone AWS SDK v2 bundle serve S3A (streaming checkpoints).
# NOTE: both jars put software.amazon.awssdk on the classpath, so the standalone
# bundle must not be OLDER than what iceberg-aws-bundle was built against — 2.24.6
# lacks RetryMode.ADAPTIVE_V2 and GlueCatalog init died with NoSuchFieldError.
RUN curl -fsSL -o /opt/spark/jars/iceberg-spark-runtime-${SPARK_VERSION}_${SCALA_VERSION}-${ICEBERG_VERSION}.jar \
      https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-${SPARK_VERSION}_${SCALA_VERSION}/${ICEBERG_VERSION}/iceberg-spark-runtime-${SPARK_VERSION}_${SCALA_VERSION}-${ICEBERG_VERSION}.jar \
 && curl -fsSL -o /opt/spark/jars/iceberg-aws-bundle-${ICEBERG_VERSION}.jar \
      https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar \
 && curl -fsSL -o /opt/spark/jars/spark-sql-kafka-0-10_${SCALA_VERSION}-${KAFKA_SPARK_VERSION}.jar \
      https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_${SCALA_VERSION}/${KAFKA_SPARK_VERSION}/spark-sql-kafka-0-10_${SCALA_VERSION}-${KAFKA_SPARK_VERSION}.jar \
 && curl -fsSL -o /opt/spark/jars/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar \
      https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar \
 && curl -fsSL -o /opt/spark/jars/spark-token-provider-kafka-0-10_${SCALA_VERSION}-${KAFKA_SPARK_VERSION}.jar \
      https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_${SCALA_VERSION}/${KAFKA_SPARK_VERSION}/spark-token-provider-kafka-0-10_${SCALA_VERSION}-${KAFKA_SPARK_VERSION}.jar \
 && curl -fsSL -o /opt/spark/jars/commons-pool2-2.12.0.jar \
      https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.12.0/commons-pool2-2.12.0.jar \
 && curl -fsSL -o /opt/spark/jars/hadoop-aws-${HADOOP_AWS_VERSION}.jar \
      https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar \
 && curl -fsSL -o /opt/spark/jars/bundle-${AWS_SDK_BUNDLE_VERSION}.jar \
      https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/${AWS_SDK_BUNDLE_VERSION}/bundle-${AWS_SDK_BUNDLE_VERSION}.jar

# Python deps for PySpark jobs
# kafka-python: the StreamingQueryListener emits post-commit latency from the
# DRIVER, where a Spark action would risk deadlock — so it needs a plain client.
RUN pip install --no-cache-dir pyspark==4.0.4 kafka-python==2.0.2

# Entrypoint: JOB_FILE env var specifies which job to run. On k8s the SparkApplication
# mounts jobs/_shared via ConfigMaps at /opt/jobs + /opt/shared (see infra/aws/k8s),
# so the image stays job-agnostic and compose keeps mounting ./jobs unchanged.
COPY entrypoint.sh /opt/spark/entrypoint.sh
RUN chmod +x /opt/spark/entrypoint.sh

USER 185
ENTRYPOINT ["/opt/spark/entrypoint.sh"]
