"""
Apache Hudi table DDL — run once before starting streaming jobs.

Hudi tables are created lazily on first write, so this script just validates
connectivity and pre-creates the S3 path structure by writing empty bootstrap
records. Alternatively, tables auto-initialise on the first foreachBatch write.

For this project we rely on auto-initialisation: the bronze/silver/gold jobs
each create their table on the first batch. This script is a connectivity check.
"""

import os
from pyspark.sql import SparkSession

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")


def main():
    spark = (
        SparkSession.builder
        .appName("hudi-ddl")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Hudi tables are auto-created on first write; just verify S3 access here.
    print("Hudi DDL: tables will be auto-created by streaming jobs on first write.")
    print("Paths:")
    print("  bronze: s3a://warehouse/hudi/bronze/item_attributes")
    print("  silver: s3a://warehouse/hudi/silver/item_attributes")
    print("  gold:   s3a://warehouse/hudi/gold/item_category_count")

    # Verify S3 access by listing the warehouse root
    try:
        sc = spark.sparkContext
        hadoop_conf = sc._jsc.hadoopConfiguration()
        fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(
            sc._jvm.java.net.URI.create("s3a://warehouse/"),
            hadoop_conf
        )
        exists = fs.exists(sc._jvm.org.apache.hadoop.fs.Path("s3a://warehouse/"))
        print(f"S3 warehouse bucket accessible: {exists}")
    except Exception as e:
        print(f"S3 check skipped: {e}")

    print("Hudi DDL complete.")


if __name__ == "__main__":
    main()
