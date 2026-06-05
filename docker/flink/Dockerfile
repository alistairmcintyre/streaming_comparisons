FROM flink:1.18.1-scala_2.12-java11

USER root

ENV ICEBERG_VERSION=1.8.1 \
    FLINK_VERSION=1.18 \
    SCALA_VERSION=2.12

# ── Iceberg Flink runtime ──────────────────────────────────────────────────
RUN curl -fsSL -o /opt/flink/lib/iceberg-flink-runtime-${FLINK_VERSION}-${ICEBERG_VERSION}.jar \
      https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-flink-runtime-${FLINK_VERSION}/${ICEBERG_VERSION}/iceberg-flink-runtime-${FLINK_VERSION}-${ICEBERG_VERSION}.jar

# ── Flink Kafka SQL connector ──────────────────────────────────────────────
RUN curl -fsSL -o /opt/flink/lib/flink-sql-connector-kafka-3.1.0-${FLINK_VERSION}.jar \
      https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.1.0-${FLINK_VERSION}/flink-sql-connector-kafka-3.1.0-${FLINK_VERSION}.jar

# ── Hadoop AWS + S3 filesystem plugin (for s3:// checkpoints) ─────────────
# Must be in plugins/s3-fs-hadoop/ — NOT in lib/ — or Flink won't load it.
RUN mkdir -p /opt/flink/plugins/s3-fs-hadoop \
 && curl -fsSL -o /opt/flink/plugins/s3-fs-hadoop/flink-s3-fs-hadoop-1.18.1.jar \
      https://repo1.maven.org/maven2/org/apache/flink/flink-s3-fs-hadoop/1.18.1/flink-s3-fs-hadoop-1.18.1.jar

# ── Hadoop client bundle (required by Iceberg S3FileIO at runtime) ────────
# Using the shaded bundle avoids chasing individual transitive dependencies.
RUN curl -fsSL -o /opt/flink/lib/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar \
      https://repo1.maven.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/2.8.3-10.0/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar

# ── Iceberg AWS bundle (AWS SDK v2 for S3FileIO) ───────────────────────────
RUN curl -fsSL -o /opt/flink/lib/iceberg-aws-bundle-${ICEBERG_VERSION}.jar \
      https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar

# Flink config for S3/MinIO — set via FLINK_PROPERTIES env var in docker-compose
# but also set here as defaults
COPY flink-conf-extra.yaml /opt/flink/conf/flink-conf-extra.yaml
RUN cat /opt/flink/conf/flink-conf-extra.yaml >> /opt/flink/conf/config.yaml || \
    cat /opt/flink/conf/flink-conf-extra.yaml >> /opt/flink/conf/flink-conf.yaml || true

USER flink
