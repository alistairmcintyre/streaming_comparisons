FROM flink:1.20.5-scala_2.12-java17

USER root

ENV PAIMON_VERSION=1.4.2 \
    FLINK_MAJOR=1.20

# envsubst (gettext-base) is used by submit.sh to render the SQL templates.
RUN apt-get update && apt-get install -y --no-install-recommends gettext-base \
 && rm -rf /var/lib/apt/lists/*

# ── Paimon Flink runtime + S3 filesystem ────────────────────────────────────
RUN curl -fsSL -o /opt/flink/lib/paimon-flink-${FLINK_MAJOR}-${PAIMON_VERSION}.jar \
      https://repo1.maven.org/maven2/org/apache/paimon/paimon-flink-${FLINK_MAJOR}/${PAIMON_VERSION}/paimon-flink-${FLINK_MAJOR}-${PAIMON_VERSION}.jar \
 && curl -fsSL -o /opt/flink/lib/paimon-s3-${PAIMON_VERSION}.jar \
      https://repo1.maven.org/maven2/org/apache/paimon/paimon-s3/${PAIMON_VERSION}/paimon-s3-${PAIMON_VERSION}.jar

# ── Flink Kafka SQL connector (bronze source) ───────────────────────────────
RUN curl -fsSL -o /opt/flink/lib/flink-sql-connector-kafka-3.4.0-${FLINK_MAJOR}.jar \
      https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.4.0-${FLINK_MAJOR}/flink-sql-connector-kafka-3.4.0-${FLINK_MAJOR}.jar

# ── Hadoop client bundle (required by Paimon's catalog at runtime) ──────────
# Paimon references org.apache.hadoop.conf.Configuration when creating a catalog,
# even with the paimon-s3 filesystem — without it CREATE CATALOG throws
# ClassNotFoundException. Shaded bundle avoids chasing transitive deps. Needed
# both locally and on AWS.
RUN curl -fsSL -o /opt/flink/lib/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar \
      https://repo1.maven.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/2.8.3-10.0/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar

# ── S3 filesystem plugin for Flink checkpoints (s3://) ──────────────────────
# Must live in plugins/, not lib/, or Flink won't load it.
RUN mkdir -p /opt/flink/plugins/s3-fs-hadoop \
 && curl -fsSL -o /opt/flink/plugins/s3-fs-hadoop/flink-s3-fs-hadoop-1.20.5.jar \
      https://repo1.maven.org/maven2/org/apache/flink/flink-s3-fs-hadoop/1.20.5/flink-s3-fs-hadoop-1.20.5.jar

# ── AWS-only (uncomment for DEPLOY_ENV=aws builds) ──────────────────────────
# MSK IAM auth for the Kafka source:
# RUN curl -fsSL -o /opt/flink/lib/aws-msk-iam-auth-2.2.0-all.jar \
#      https://github.com/aws/aws-msk-iam-auth/releases/download/v2.2.0/aws-msk-iam-auth-2.2.0-all.jar
# Glue metastore client for Paimon Iceberg hive-catalog registration is also
# needed on AWS (aws-glue-datacatalog-hive metastore client jar in lib/).

USER flink
