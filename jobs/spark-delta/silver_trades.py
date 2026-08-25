"""
Spark Structured Streaming: Delta bronze.trades → Delta silver.trades.

Cleaned + deduped fills (still append — fills are immutable events, NOT a
current-view). Dedups exact re-deliveries on trade_id within a watermark (bounded
state), drops the Kafka envelope/offsets. This is where derived/enriched fields
would be added — none required yet, so it's a clean passthrough.
"""
import os
from pyspark.sql import SparkSession
from delta_tables import ensure_all  # in-pipeline DDL
from pyspark.sql.functions import col

CHECKPOINT_BASE = os.environ.get("CHECKPOINT_BASE", "s3a://warehouse/_chk")
CHECKPOINT_PATH = f"{CHECKPOINT_BASE}/silver_trades_delta"
_BASE = os.environ.get("DELTA_WAREHOUSE", "s3a://warehouse/delta")
BRONZE_TRADES   = f"{_BASE}/bronze/trades"
SILVER_TRADES   = f"{_BASE}/silver/trades"


def main():
    spark = SparkSession.builder.appName("silver-trades-delta").getOrCreate()
    ensure_all(spark)  # idempotent create-if-not-exists; no separate ddl-init
    spark.sparkContext.setLogLevel("WARN")

    cleaned = (
        spark.readStream.format("delta")
        .option("startingVersion", "0")
        .option("maxFilesPerTrigger", "200")
        .load(BRONZE_TRADES)
        .withWatermark("event_ts", "1 hour")
        .dropDuplicatesWithinWatermark(["trade_id"])   # bounded exact-dedupe of re-deliveries
        .select("trade_id", "account_id", "symbol", "side", "quantity",
                "price", "executed_at", "event_ts", "ingest_ts")
    )

    (cleaned.writeStream.format("delta").outputMode("append")
        .option("path", SILVER_TRADES)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime="10 seconds")
        .start().awaitTermination())


if __name__ == "__main__":
    main()
