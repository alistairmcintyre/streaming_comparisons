#!/usr/bin/env bash
# Build Fluss from source (main / 1.0-SNAPSHOT) and stage the artifacts our two
# Fluss images consume. We build from source because FIP-27 ("clean" Paimon lake
# schema without the __bucket/__offset/__timestamp system columns) is on main but
# Not in any release (latest tag 0.9.1-incubating, 2026-05-04). The precision-3
# __timestamp system column is what breaks Paimon's Iceberg-compat view (Athena),
# and FIP-27 removes it. See README.md.
#
# Fluss requires JDK 11 to build; we do it in a container so no local JDK needed.
#
# Usage:  FLUSS_SRC=~/git/apache/fluss docker/fluss/build-fluss.sh
set -euo pipefail

FLUSS_SRC="${FLUSS_SRC:-$HOME/git/apache/fluss}"
REPO="$(cd "$(dirname "$0")/../.." && pwd)"
M2="${M2_CACHE:-$REPO/.fluss-m2-cache}"          # reused across builds
VER="1.0-SNAPSHOT"

[ -f "$FLUSS_SRC/pom.xml" ] || { echo "Fluss source not found at $FLUSS_SRC (set FLUSS_SRC)"; exit 1; }
mkdir -p "$M2"

echo "==> Building Fluss $VER from $FLUSS_SRC (JDK11 container)"
docker run --rm -v "$FLUSS_SRC":/src -v "$M2":/root/.m2 -w /src \
  maven:3.9-eclipse-temurin-11 \
  mvn -U -T4 clean install \
    -pl fluss-dist,fluss-flink/fluss-flink-1.20,fluss-flink/fluss-flink-tiering,fluss-lake/fluss-lake-paimon -am \
    -DskipTests -Drat.skip=true -Dspotless.check.skip=true \
    -Dcheckstyle.skip=true -Denforcer.skip=true -Dmaven.javadoc.skip=true -Dgpg.skip=true

FT="$REPO/docker/fluss-flink/jars"; DS="$REPO/docker/fluss/dist"
mkdir -p "$FT"; rm -rf "$DS"; mkdir -p "$DS"

echo "==> Staging flink-side jars -> $FT"
cp "$FLUSS_SRC/fluss-flink/fluss-flink-1.20/target/fluss-flink-1.20-$VER.jar"       "$FT/"
cp "$FLUSS_SRC/fluss-flink/fluss-flink-tiering/target/fluss-flink-tiering-$VER.jar" "$FT/"
cp "$FLUSS_SRC/fluss-lake/fluss-lake-paimon/target/fluss-lake-paimon-$VER.jar"      "$FT/"
cp "$FLUSS_SRC/fluss-filesystems/fluss-fs-s3/target/fluss-fs-s3-$VER.jar"           "$FT/"

echo "==> Staging server distribution -> $DS"
cp -r "$FLUSS_SRC/fluss-dist/target/fluss-$VER-bin/fluss-$VER" "$DS/"
# keep only the plugins we use (drop big cloud-fs/lance/metrics bundles)
for p in oss gs azure cos obs hdfs hudi lance influxdb prometheus datadog graphite; do
  rm -rf "$DS/fluss-$VER/plugins/$p"
done
echo "==> Done. dist=$(du -sh "$DS/fluss-$VER" | cut -f1), jars staged in $FT"
