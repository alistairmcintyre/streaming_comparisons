"""
Creates all Iceberg namespaces and tables via Spark SQL.
Replaces the PyIceberg DDL script to avoid partition transform
compatibility issues between PyIceberg 0.6.1 and iceberg-spark-runtime 1.5.2.
"""

import os
import time
import logging
from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

SQL_FILE = os.path.join(os.path.dirname(__file__), "create_tables.sql")


def main():
    spark = (
        SparkSession.builder
        .appName("ddl-init")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    log.info(f"Reading DDL from {SQL_FILE}")
    with open(SQL_FILE, encoding="utf-8") as f:
        sql = f.read()

    # Strip comment lines before splitting so section headers don't corrupt adjacent statements
    stripped = "\n".join(
        line for line in sql.splitlines() if not line.strip().startswith("--")
    )
    statements = [s.strip() for s in stripped.split(";") if s.strip()]

    log.info(f"Found {len(statements)} statements to execute")
    for i, stmt in enumerate(statements):
        log.info(f"[{i+1}/{len(statements)}] Executing: {stmt[:80]}...")
        try:
            spark.sql(stmt)
        except Exception as e:
            log.error(f"Statement failed: {e}\nFull statement:\n{stmt}")
            raise

    log.info("All tables created successfully.")

    log.info("Tables in bronze:")
    spark.sql("SHOW TABLES IN rest.bronze").show(truncate=False)
    log.info("Tables in silver:")
    spark.sql("SHOW TABLES IN rest.silver").show(truncate=False)
    log.info("Tables in gold:")
    spark.sql("SHOW TABLES IN rest.gold").show(truncate=False)


if __name__ == "__main__":
    main()
