FROM python:3.11-slim

WORKDIR /app

# System deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Python deps
# - pyiceberg[s3fs,pyarrow]: reads Iceberg tables via REST catalog + S3
# - streamlit: reporting UI
# - duckdb: optional fast in-process SQL over Arrow
# - psycopg2-binary: used by data generators
# - boto3: underlying S3 client for pyiceberg s3fs
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

ENV PYTHONUNBUFFERED=1
