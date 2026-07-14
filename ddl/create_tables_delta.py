"""
Delta Lake table DDL — run once before starting streaming jobs.

Delta doesn't have a catalog server; tables are path-addressed under s3a://.
This script creates the table directories and schemas using DeltaTable.createIfNotExists().
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, IntegerType,
    TimestampType, DoubleType, DateType
)
from delta.tables import DeltaTable

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")


def main():
    spark = (
        SparkSession.builder
        .appName("delta-ddl")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # ── Bronze: append-only, full CDC envelope ────────────────────────────────
    bronze_schema = StructType([
        StructField("op",               StringType(),    True),
        StructField("item_id",          LongType(),      True),
        StructField("name",             StringType(),    True),
        StructField("price",            DoubleType(),    True),
        StructField("category",         StringType(),    True),
        StructField("source_updated_at", TimestampType(), True),
        StructField("event_ts",         TimestampType(), True),
        StructField("ingest_ts",        TimestampType(), True),
        StructField("kafka_offset",     LongType(),      True),
        StructField("kafka_partition",  IntegerType(),   True),
    ])

    (
        DeltaTable.createIfNotExists(spark)
        .tableName("delta_bronze_item_attributes")
        .addColumns(bronze_schema)
        .location("s3a://warehouse/delta/bronze/item_attributes")
        .property("delta.enableDeletionVectors", "false")  # append-only, no DVs needed
        .execute()
    )
    print("Created: delta/bronze/item_attributes")

    # ── Silver: upsert (MoR equivalent via Deletion Vectors) ──────────────────
    silver_schema = StructType([
        StructField("item_id",           LongType(),      False),
        StructField("op",                StringType(),    True),
        StructField("name",              StringType(),    True),
        StructField("price",             DoubleType(),    True),
        StructField("category",          StringType(),    True),
        StructField("source_updated_at", TimestampType(), True),
        StructField("event_ts",          TimestampType(), True),
        StructField("event_date",        DateType(),      True),
        StructField("ingest_ts",         TimestampType(), True),
        StructField("commit_ts",         TimestampType(), True),
    ])

    (
        DeltaTable.createIfNotExists(spark)
        .tableName("delta_silver_item_attributes")
        .addColumns(silver_schema)
        .location("s3a://warehouse/delta/silver/item_attributes")
        # Deletion Vectors = MoR equivalent for Delta: marks deletes/updates
        # without rewriting files until OPTIMIZE runs
        .property("delta.enableDeletionVectors", "true")
        .execute()
    )
    print("Created: delta/silver/item_attributes")

    # ── Gold: aggregation, upsert by category ─────────────────────────────────
    gold_schema = StructType([
        StructField("category",   StringType(),    False),
        StructField("item_count", LongType(),      True),
        StructField("commit_ts",  TimestampType(), True),
    ])

    (
        DeltaTable.createIfNotExists(spark)
        .tableName("delta_gold_item_category_count")
        .addColumns(gold_schema)
        .location("s3a://warehouse/delta/gold/item_category_count")
        .property("delta.enableDeletionVectors", "true")
        .execute()
    )
    print("Created: delta/gold/item_category_count")

    print("Delta DDL complete.")


if __name__ == "__main__":
    main()
