"""Streamlit UI configuration — loaded from environment variables."""

import os

ICEBERG_REST_URI  = os.environ.get("ICEBERG_REST_URI",  "http://iceberg-rest:8181")
ICEBERG_WAREHOUSE = os.environ.get("ICEBERG_WAREHOUSE",  "s3://warehouse/")
MINIO_ENDPOINT    = os.environ.get("MINIO_ENDPOINT",     "http://minio:9000")
AWS_ACCESS_KEY    = os.environ.get("AWS_ACCESS_KEY_ID",  "minioadmin")
AWS_SECRET_KEY    = os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin")

# The 5 item_ids shown side-by-side in the reporting table
SELECTED_ITEM_IDS = [1, 42, 100, 500, 999]

# How often to refresh (seconds)
REFRESH_INTERVAL_SECS = 30

# Tolerance for reconciliation mismatch flagging (qty difference threshold)
RECON_QTY_TOLERANCE = 0
