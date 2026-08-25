"""
Spark Structured Streaming: Iceberg bronze.trades_spark → silver.trades_spark.
Cleaned + deduped fills (append — immutable events). Dedup on trade_id within a
watermark (bounded state); drop the Kafka envelope. Reads bronze as an APPEND
stream (append snapshots are stream-readable in Iceberg — no merged-table issue).
"""
import os
from pyspark.sql import SparkSession
from iceberg_tables import ensure_all  # in-pipeline DDL

CHECKPOINT_BASE = os.environ.get("CHECKPOINT_BASE", "s3a://warehouse/_chk")
CHECKPOINT_PATH = f"{CHECKPOINT_BASE}/silver_trades_spark"
BRONZE = "rest.bronze.trades_spark"
SILVER = "rest.silver.trades_spark"


def main():
    spark = SparkSession.builder.appName("silver-trades-spark").getOrCreate()
    ensure_all(spark)  # idempotent create-if-not-exists; no separate ddl-init
    spark.sparkContext.setLogLevel("WARN")

    cleaned = (
        spark.readStream.format("iceberg")
        .option("streaming-skip-overwrite-snapshots", "true")
        .option("streaming-skip-delete-snapshots", "true")
        .option("streaming-max-files-per-micro-batch", "500")
        .load(BRONZE)
        .withWatermark("event_ts", "1 hour")
        .dropDuplicatesWithinWatermark(["trade_id"])
        .select("trade_id", "account_id", "symbol", "side", "quantity",
                "price", "executed_at", "event_ts", "ingest_ts")
    )

    (cleaned.writeStream.format("iceberg").outputMode("append")
        .option("path", SILVER)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .option("fanout-enabled", "true")
        .trigger(processingTime="10 seconds")
        .start().awaitTermination())


if __name__ == "__main__":
    main()
