"""
Creates all Iceberg namespaces and tables via PyIceberg REST catalog.
Run once after infrastructure is up, before starting streaming jobs.

Usage (from docker-compose ddl-init service, or manually):
    python ddl/create_namespaces.py
"""

import os
import sys
import time
import logging

from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, TableAlreadyExistsError, NoSuchNamespaceError

sys.path.insert(0, os.path.dirname(__file__))
from tables import ALL_TABLES

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def wait_for_catalog(catalog, retries=30, delay=3):
    for attempt in range(retries):
        try:
            catalog.list_namespaces()
            log.info("Iceberg REST catalog is reachable.")
            return
        except Exception as e:
            log.warning(f"Catalog not ready (attempt {attempt+1}/{retries}): {e}")
            time.sleep(delay)
    raise RuntimeError("Iceberg REST catalog did not become ready in time.")


def main():
    rest_uri  = os.environ.get("ICEBERG_REST_URI", "http://iceberg-rest:8181")
    warehouse = os.environ.get("ICEBERG_WAREHOUSE", "s3://warehouse/")
    endpoint  = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
    ak        = os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin")
    sk        = os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin")

    log.info(f"Connecting to Iceberg REST catalog at {rest_uri}")
    catalog = load_catalog(
        "rest",
        **{
            "type": "rest",
            "uri": rest_uri,
            "warehouse": warehouse,
            "s3.endpoint": endpoint,
            "s3.path-style-access": "true",
            "s3.access-key-id": ak,
            "s3.secret-access-key": sk,
        }
    )

    wait_for_catalog(catalog)

    # Create namespaces
    for ns in ("bronze", "silver", "gold"):
        try:
            catalog.create_namespace(ns)
            log.info(f"Created namespace: {ns}")
        except NamespaceAlreadyExistsError:
            log.info(f"Namespace already exists: {ns}")

    # Create tables
    for ns, table_name, schema, partition_spec, properties in ALL_TABLES:
        full_name = f"{ns}.{table_name}"
        try:
            catalog.create_table(
                identifier=full_name,
                schema=schema,
                partition_spec=partition_spec,
                properties=properties,
            )
            log.info(f"Created table: {full_name}")
        except TableAlreadyExistsError:
            log.info(f"Table already exists (skipping): {full_name}")
        except Exception as e:
            log.error(f"Failed to create table {full_name}: {e}")
            raise

    log.info("All namespaces and tables created successfully.")

    # Verify
    log.info("Current tables:")
    for ns in ("bronze", "silver"):
        for tbl in catalog.list_tables(ns):
            log.info(f"  {'.'.join(tbl)}")


if __name__ == "__main__":
    main()
