"""
Shared utilities for data generators.
"""

import os
import time
import logging
import psycopg2
from psycopg2.extras import execute_values

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def get_conn():
    """Create a Postgres connection from environment variables."""
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "postgres-app"),
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        user=os.environ.get("POSTGRES_USER", "app"),
        password=os.environ.get("POSTGRES_PASSWORD", "app"),
        dbname=os.environ.get("POSTGRES_DB", "appdb"),
    )


def wait_for_postgres(retries=30, delay=3):
    for attempt in range(retries):
        try:
            conn = get_conn()
            conn.close()
            log.info("Postgres is reachable.")
            return
        except Exception as e:
            log.warning(f"Postgres not ready (attempt {attempt+1}/{retries}): {e}")
            time.sleep(delay)
    raise RuntimeError("Postgres did not become ready in time.")


def get_rate() -> float:
    """Events per second from env var EVENTS_PER_SEC."""
    return float(os.environ.get("EVENTS_PER_SEC", "5"))
