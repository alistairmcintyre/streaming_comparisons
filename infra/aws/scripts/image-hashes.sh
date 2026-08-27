#!/usr/bin/env bash
# Content-addressed image identity: hash each image's BUILD INPUTS, exactly as Docker
# keys a layer on the instruction plus the content it touches.
#
# WHY NOT TIMESTAMPS: the first version of this compared the ECR push time against the
# Dockerfile's last commit time. That is a heuristic and it breaks on clock skew, on a
# rebuild that changes nothing, on a rebase that rewrites commit times, and on a
# Dockerfile touched without semantic change. A content hash is an equality test:
# same inputs -> same tag -> the image in ECR is provably the image these sources build.
#
# The failure it exists to prevent, seen live 2026-08-27: spark-iceberg was pushed at
# 06:28:18 and the Dockerfile that added kafka-python was committed at 06:28:47. The
# build raced the commit and lost by 29 seconds. Every run afterwards passed
# skip_build=true, inherited images with no kafka module, and the Spark latency emit
# failed silently on all three engines for the rest of the day.
#
#   image-hashes.sh            -> "<image> <hash>" per line
#   image-hashes.sh <image>    -> just that image's hash
set -uo pipefail

# image:dockerfile:context — must mirror the bp() calls in eks-run.yml
SPECS="
fluss-server:docker/fluss/Dockerfile:docker/fluss
fluss-flink:docker/fluss-flink/Dockerfile:docker/fluss-flink
generator:docker/generator/Dockerfile:docker/generator
latency-exporter:docker/latency-exporter/Dockerfile:docker/latency-exporter
flink-paimon:docker/flink-paimon/Dockerfile:docker/flink-paimon
spark-iceberg:docker/spark/Dockerfile:docker/spark
spark-delta:docker/spark-delta/Dockerfile:docker/spark-delta
spark-hudi:docker/spark-hudi/Dockerfile:docker/spark-hudi
"

hash_one() {
  local df="$1" ctx="$2"
  # Every file in the build context plus the Dockerfile, content-hashed and ORDER
  # STABLE (sort), so the digest depends on content only — not on filesystem order,
  # mtimes, or where the checkout lives.
  { echo "$df"; find "$ctx" -type f 2>/dev/null | sort; } \
    | while read -r f; do [ -f "$f" ] && sha256sum "$f"; done \
    | sha256sum | cut -c1-16
}

want="${1:-}"
echo "$SPECS" | while IFS=: read -r img df ctx; do
  [ -z "$img" ] && continue
  [ -n "$want" ] && [ "$want" != "$img" ] && continue
  printf '%s %s\n' "$img" "$(hash_one "$df" "$ctx")"
done
