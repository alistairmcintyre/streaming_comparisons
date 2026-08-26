FROM apache/spark:4.0.4
USER root

# Spark 4.0.x → Scala 2.13, bundled Hadoop 3.4.1, AWS SDK v2.
# Delta 4.0.0 is the release built against Spark 4.0.x — delta-spark 4.1/4.2/4.3
# target Spark 4.1.0 and throw NoSuchMethodError (ParserInterface.$init$) on 4.0.4.
ENV DELTA_VERSION=4.0.0 \
    KAFKA_SPARK_VERSION=4.0.4 \
    KAFKA_CLIENTS_VERSION=3.9.1 \
    SCALA_VERSION=2.13 \
    HADOOP_AWS_VERSION=3.4.1 \
    AWS_SDK_BUNDLE_VERSION=2.24.6

# ── Delta Lake (delta-spark + delta-storage; storage is NOT bundled) ────────
# S3DynamoDBLogStore (delta-storage-s3-dynamodb) is deliberately NOT installed: it is
# compiled against the AWS SDK **v1**, which this SDK-v2 image does not carry, and it
# is unnecessary here — one streaming writer per table, and VACUUM writes no commits.
RUN curl -fsSL -o /opt/spark/jars/delta-spark_${SCALA_VERSION}-${DELTA_VERSION}.jar \
      https://repo1.maven.org/maven2/io/delta/delta-spark_${SCALA_VERSION}/${DELTA_VERSION}/delta-spark_${SCALA_VERSION}-${DELTA_VERSION}.jar \
 && curl -fsSL -o /opt/spark/jars/delta-storage-${DELTA_VERSION}.jar \
      https://repo1.maven.org/maven2/io/delta/delta-storage/${DELTA_VERSION}/delta-storage-${DELTA_VERSION}.jar

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

# pyspark + delta-spark's Python module (delta.tables) — the DDL uses the
# DeltaTable builder API; the JARs alone don't provide the Python package.
# Pinned to DELTA_VERSION so the Python module matches the runtime JARs.
RUN pip install --no-cache-dir pyspark==4.0.4 delta-spark==${DELTA_VERSION}

COPY entrypoint.sh /opt/spark/entrypoint.sh
RUN chmod +x /opt/spark/entrypoint.sh


USER 185
ENTRYPOINT ["/opt/spark/entrypoint.sh"]
