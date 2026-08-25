"""
Show the Iceberg .snapshots metadata for the flink bronze/silver tables.

The `operation` column is the arbiter of append vs overwrite. append =
data-files only (stream-readable incrementally); overwrite = includes delete
files (equality/positional), which Flink's streaming source can't read as a
changelog. Run inside the spark (iceberg) image.
"""
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("show-snapshots").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

for t in ["rest.bronze.customers_flink",
          "rest.silver.customers_flink",
          "rest.silver.customers_flink_direct"]:
    print(f"=== {t}.snapshots ===")
    try:
        spark.sql(f"""
            SELECT snapshot_id,
                   operation,
                   summary['added-data-files']            AS data_files,
                   summary['added-equality-delete-files'] AS eq_deletes,
                   summary['added-position-delete-files'] AS pos_deletes
            FROM {t}.snapshots
            ORDER BY committed_at
        """).show(50, False)
    except Exception as e:
        print("err:", str(e)[:180])
