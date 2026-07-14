FROM apache/spark:3.5.6
USER root

ENV DELTA_VERSION=3.3.2 \
    KAFKA_SPARK_VERSION=3.5.6 \
    SCALA_VERSION=2.12

# ── Delta Lake ─────────────────────────────────────────────────────────────
RUN curl -fsSL -o /opt/spark/jars/delta-spark_${SCALA_VERSION}-${DELTA_VERSION}.jar \
      https://repo1.maven.org/maven2/io/delta/delta-spark_${SCALA_VERSION}/${DELTA_VERSION}/delta-spark_${SCALA_VERSION}-${DELTA_VERSION}.jar

# ── Kafka connector ────────────────────────────────────────────────────────
RUN curl -fsSL -o /opt/spark/jars/spark-sql-kafka-0-10_${SCALA_VERSION}-${KAFKA_SPARK_VERSION}.jar \
      https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_${SCALA_VERSION}/${KAFKA_SPARK_VERSION}/spark-sql-kafka-0-10_${SCALA_VERSION}-${KAFKA_SPARK_VERSION}.jar \
 && curl -fsSL -o /opt/spark/jars/kafka-clients-3.7.0.jar \
      https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.7.0/kafka-clients-3.7.0.jar \
 && curl -fsSL -o /opt/spark/jars/spark-token-provider-kafka-0-10_${SCALA_VERSION}-${KAFKA_SPARK_VERSION}.jar \
      https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_${SCALA_VERSION}/${KAFKA_SPARK_VERSION}/spark-token-provider-kafka-0-10_${SCALA_VERSION}-${KAFKA_SPARK_VERSION}.jar \
 && curl -fsSL -o /opt/spark/jars/commons-pool2-2.11.1.jar \
      https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar

# ── Hadoop S3A (for checkpoint paths) ─────────────────────────────────────
RUN curl -fsSL -o /opt/spark/jars/hadoop-aws-3.3.4.jar \
      https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
 && curl -fsSL -o /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar \
      https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

RUN pip install --no-cache-dir pyspark==3.5.6

COPY entrypoint.sh /opt/spark/entrypoint.sh
RUN chmod +x /opt/spark/entrypoint.sh

USER 185
ENTRYPOINT ["/opt/spark/entrypoint.sh"]
