# Fluss coordinator/tablet server, built from source (main / 1.0-SNAPSHOT).
#
# We run main (not the 0.9.1 release) for FIP-27: newly created Paimon lake tables
# get a "clean" schema WITHOUT the __bucket/__offset/__timestamp system columns.
# The precision-3 __timestamp (TIMESTAMP_LTZ(3)) is what broke Paimon's Iceberg
# metadata generation ("only support timestamp type with precision from 4 to 9"),
# making the tiered tables invisible to Athena/Spark. FIP-27 (PR #3982) removes it.
#
# Artifacts come from docker/fluss/build-fluss.sh (JDK11 maven build of the clone).
# Base image only needs a JRE 11 + bash + envsubst (gettext) for the entrypoint.
FROM eclipse-temurin:11-jre

RUN apt-get update \
 && apt-get install -y --no-install-recommends bash gettext-base curl \
 && rm -rf /var/lib/apt/lists/*

ENV FLUSS_HOME=/opt/fluss
# Full server distribution (bin/ conf/ lib/ plugins/) built from main.
# plugins/paimon already carries fluss-lake-paimon + paimon-bundle-1.4.2.
COPY dist/fluss-1.0-SNAPSHOT/ ${FLUSS_HOME}/

# Paimon S3 FileIO for the datalake tier (paimon-bundle is present; paimon-s3 is not).
RUN curl -fsSL -o ${FLUSS_HOME}/plugins/paimon/paimon-s3-1.4.2.jar \
      https://repo1.maven.org/maven2/org/apache/paimon/paimon-s3/1.4.2/paimon-s3-1.4.2.jar

COPY docker-entrypoint.sh /docker-entrypoint.sh
RUN chmod +x /docker-entrypoint.sh ${FLUSS_HOME}/bin/*.sh

WORKDIR ${FLUSS_HOME}
ENTRYPOINT ["/docker-entrypoint.sh"]
CMD ["help"]
