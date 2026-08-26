FROM apache/spark:4.0.4
USER root

# Spark 4.0.x → Scala 2.13, bundled Hadoop 3.4.1, AWS SDK v2.
ENV HUDI_VERSION=1.2.0 \
    KAFKA_SPARK_VERSION=4.0.4 \
    KAFKA_CLIENTS_VERSION=3.9.1 \
    SCALA_VERSION=2.13 \
    HADOOP_AWS_VERSION=3.4.1 \
    AWS_SDK_BUNDLE_VERSION=2.24.6

# ── Hudi bundle (Spark 4.0 build) ──────────────────────────────────────────
RUN curl -fsSL -o /opt/spark/jars/hudi-spark4.0-bundle_${SCALA_VERSION}-${HUDI_VERSION}.jar \
      https://repo1.maven.org/maven2/org/apache/hudi/hudi-spark4.0-bundle_${SCALA_VERSION}/${HUDI_VERSION}/hudi-spark4.0-bundle_${SCALA_VERSION}-${HUDI_VERSION}.jar \
 && curl -fsSL -o /opt/spark/jars/hudi-aws-bundle-${HUDI_VERSION}.jar \
      https://repo1.maven.org/maven2/org/apache/hudi/hudi-aws-bundle/${HUDI_VERSION}/hudi-aws-bundle-${HUDI_VERSION}.jar

# hudi-aws-bundle carries org.apache.hudi.aws.sync.AwsGlueCatalogSyncTool, which the
# spark bundle does NOT contain (verified: no org/apache/hudi/aws/sync/* in it). It is
# what lets Hudi register itself in Glue on every commit, so Athena can read the
# tables. Self-registration beats an external pointer: no stale metadata_location, the
# problem that broke Paimon's Athena visibility.

# ── Kafka connector ────────────────────────────────────────────────────────
RUN curl -fsSL -o /opt/spark/jars/spark-sql-kafka-0-10_${SCALA_VERSION}-${KAFKA_SPARK_VERSION}.jar \
      https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_${SCALA_VERSION}/${KAFKA_SPARK_VERSION}/spark-sql-kafka-0-10_${SCALA_VERSION}-${KAFKA_SPARK_VERSION}.jar \
 && curl -fsSL -o /opt/spark/jars/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar \
      https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar \
 && curl -fsSL -o /opt/spark/jars/spark-token-provider-kafka-0-10_${SCALA_VERSION}-${KAFKA_SPARK_VERSION}.jar \
      https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_${SCALA_VERSION}/${KAFKA_SPARK_VERSION}/spark-token-provider-kafka-0-10_${SCALA_VERSION}-${KAFKA_SPARK_VERSION}.jar \
 && curl -fsSL -o /opt/spark/jars/commons-pool2-2.12.0.jar \
      https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.12.0/commons-pool2-2.12.0.jar

# ── Hadoop S3A (Hadoop 3.4.x uses AWS SDK v2) ──────────────────────────────
RUN curl -fsSL -o /opt/spark/jars/hadoop-aws-${HADOOP_AWS_VERSION}.jar \
      https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar \
 && curl -fsSL -o /opt/spark/jars/bundle-${AWS_SDK_BUNDLE_VERSION}.jar \
      https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/${AWS_SDK_BUNDLE_VERSION}/bundle-${AWS_SDK_BUNDLE_VERSION}.jar

RUN pip install --no-cache-dir pyspark==4.0.4

COPY entrypoint.sh /opt/spark/entrypoint.sh
RUN chmod +x /opt/spark/entrypoint.sh

USER 185
ENTRYPOINT ["/opt/spark/entrypoint.sh"]
