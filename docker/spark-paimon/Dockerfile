FROM apache/spark:3.5.6
USER root

ENV PAIMON_VERSION=1.1.0 \
    KAFKA_SPARK_VERSION=3.5.6 \
    SCALA_VERSION=2.12

# ── Paimon ─────────────────────────────────────────────────────────────────
RUN curl -fsSL -o /opt/spark/jars/paimon-spark-3.5_${SCALA_VERSION}-${PAIMON_VERSION}.jar \
      https://repo1.maven.org/maven2/org/apache/paimon/paimon-spark-3.5_${SCALA_VERSION}/${PAIMON_VERSION}/paimon-spark-3.5_${SCALA_VERSION}-${PAIMON_VERSION}.jar \
 && curl -fsSL -o /opt/spark/jars/paimon-s3-${PAIMON_VERSION}.jar \
      https://repo1.maven.org/maven2/org/apache/paimon/paimon-s3/${PAIMON_VERSION}/paimon-s3-${PAIMON_VERSION}.jar

# ── Kafka connector ────────────────────────────────────────────────────────
RUN curl -fsSL -o /opt/spark/jars/spark-sql-kafka-0-10_${SCALA_VERSION}-${KAFKA_SPARK_VERSION}.jar \
      https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_${SCALA_VERSION}/${KAFKA_SPARK_VERSION}/spark-sql-kafka-0-10_${SCALA_VERSION}-${KAFKA_SPARK_VERSION}.jar \
 && curl -fsSL -o /opt/spark/jars/kafka-clients-3.7.0.jar \
      https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.7.0/kafka-clients-3.7.0.jar \
 && curl -fsSL -o /opt/spark/jars/spark-token-provider-kafka-0-10_${SCALA_VERSION}-${KAFKA_SPARK_VERSION}.jar \
      https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_${SCALA_VERSION}/${KAFKA_SPARK_VERSION}/spark-token-provider-kafka-0-10_${SCALA_VERSION}-${KAFKA_SPARK_VERSION}.jar \
 && curl -fsSL -o /opt/spark/jars/commons-pool2-2.11.1.jar \
      https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar

# ── Hadoop S3A (for checkpoint paths — Paimon table paths use its own s3 plugin) ──
RUN curl -fsSL -o /opt/spark/jars/hadoop-aws-3.3.4.jar \
      https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
 && curl -fsSL -o /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar \
      https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

RUN pip install --no-cache-dir pyspark==3.5.6

COPY entrypoint.sh /opt/spark/entrypoint.sh
RUN chmod +x /opt/spark/entrypoint.sh

USER 185
ENTRYPOINT ["/opt/spark/entrypoint.sh"]
