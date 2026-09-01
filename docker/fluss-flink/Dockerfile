# Fluss's Flink cluster (JM/TM) + tiering, built against Fluss main (1.0-SNAPSHOT).
#
# Keeps the quickstart image's known-good Flink 1.20 / JDK base, but REPLACES its
# bundled 0.9.1 Fluss jars with our source-built 1.0-SNAPSHOT ones (FIP-27 clean
# lake schema, see docker/fluss/Dockerfile). Paimon is bumped to 1.4.2 to match
# what Fluss main builds against. Artifacts come from docker/fluss/build-fluss.sh.
FROM apache/fluss-quickstart-flink:1.20-0.9.1-incubating

# envsubst (gettext) + curl for the submitter's SQL rendering and JM readiness wait
RUN apt-get update \
 && apt-get install -y --no-install-recommends gettext-base curl \
 && rm -rf /var/lib/apt/lists/*

# ── Remove the image's bundled 0.9.1 Fluss jars ──────────────────────────────
RUN rm -f /opt/flink/lib/fluss-flink-1.20-0.9.1-incubating.jar \
          /opt/flink/lib/fluss-fs-s3-0.9.1-incubating.jar \
          /opt/flink/opt/fluss-flink-tiering-0.9.1-incubating.jar

# ── Our source-built 1.0-SNAPSHOT Fluss jars ─────────────────────────────────
# fluss-flink-1.20 is the shaded fat connector (→ lib); tiering is a thin launcher
# that uses it (→ opt, run via `flink run`); lake-paimon is the datalake plugin.
COPY jars/fluss-flink-1.20-1.0-SNAPSHOT.jar     /opt/flink/lib/
COPY jars/fluss-fs-s3-1.0-SNAPSHOT.jar          /opt/flink/lib/
COPY jars/fluss-lake-paimon-1.0-SNAPSHOT.jar    /opt/flink/lib/
COPY jars/fluss-flink-tiering-1.0-SNAPSHOT.jar  /opt/flink/opt/

# ── Kafka source (bronze) ────────────────────────────────────────────────────
RUN curl -fsSL -o /opt/flink/lib/flink-sql-connector-kafka-3.4.0-1.20.jar \
      https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.4.0-1.20/flink-sql-connector-kafka-3.4.0-1.20.jar

# ── Paimon runtime + S3 FileIO (1.4.2, matching Fluss main) + hadoop bundle ──
RUN curl -fsSL -o /opt/flink/lib/paimon-flink-1.20-1.4.2.jar \
      https://repo1.maven.org/maven2/org/apache/paimon/paimon-flink-1.20/1.4.2/paimon-flink-1.20-1.4.2.jar \
 && curl -fsSL -o /opt/flink/lib/paimon-s3-1.4.2.jar \
      https://repo1.maven.org/maven2/org/apache/paimon/paimon-s3/1.4.2/paimon-s3-1.4.2.jar \
 && curl -fsSL -o /opt/flink/lib/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar \
      https://repo1.maven.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/2.8.3-10.0/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar

# ── S3 filesystem plugin for Flink's OWN filesystem (s3://) ─────────────────
# paimon-s3 above is Paimon's FileIO, it does not register a Flink FileSystem, so
# anything Flink itself opens on s3:// (HA blob store, savepoints, checkpoints) fails
# with:
#   UnsupportedFileSystemSchemeException: Could not find a file system implementation
#   for scheme 's3'   ...at KubernetesHaServicesFactory.createHAServices
# which crash-looped the JobManager the moment Kubernetes HA was enabled. flink-paimon
# already carries this (see docker/flink-paimon/Dockerfile); the fluss image never did,
# because nothing here had opened s3:// through Flink before.
# Must live in plugins/, not lib/, or Flink will not load it.
RUN mkdir -p /opt/flink/plugins/s3-fs-hadoop \
 && curl -fsSL -o /opt/flink/plugins/s3-fs-hadoop/flink-s3-fs-hadoop-1.20.5.jar \
      https://repo1.maven.org/maven2/org/apache/flink/flink-s3-fs-hadoop/1.20.5/flink-s3-fs-hadoop-1.20.5.jar
