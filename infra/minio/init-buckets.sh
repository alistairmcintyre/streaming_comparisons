#!/bin/sh
set -e

MC_ALIAS=local
MINIO_URL=http://minio:9000
MINIO_USER=${MINIO_ROOT_USER:-minioadmin}
MINIO_PASS=${MINIO_ROOT_PASSWORD:-minioadmin}
BUCKET=${MINIO_BUCKET:-warehouse}

echo "Waiting for MinIO to be ready..."
until mc alias set ${MC_ALIAS} ${MINIO_URL} ${MINIO_USER} ${MINIO_PASS} 2>/dev/null; do
  echo "  MinIO not ready, retrying in 2s..."
  sleep 2
done

echo "Creating bucket: ${BUCKET}"
mc mb --ignore-existing ${MC_ALIAS}/${BUCKET}

echo "Setting bucket policy to public read (for local dev ease)..."
mc anonymous set download ${MC_ALIAS}/${BUCKET}

echo "MinIO bucket init complete."
mc ls ${MC_ALIAS}/${BUCKET}
