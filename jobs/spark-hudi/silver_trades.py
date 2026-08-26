"""
Spark Structured Streaming: Hudi bronze.trades → Hudi silver.trades

Deduplicates on trade_id (upsert). Reads the bronze table incrementally rather than
rescanning it — see STREAMING_DESIGN_PRINCIPLES.md.
"""
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
from hudi_tables import BRONZE_TRADES, SILVER_TRADES, silver_trades_opts

CHECKPOINT_BASE = os.environ.get("CHECKPOINT_BASE", "s3a://warehouse/_chk")
CHECKPOINT_PATH = f"{CHECKPOINT_BASE}/silver_trades_hudi"


def main():
    spark = SparkSession.builder.appName("silver-trades-hudi").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # Hudi streaming source reads the commit timeline incrementally.
    src = (spark.readStream.format("hudi")
           .load(BRONZE_TRADES)
           .select("trade_id", "account_id", "symbol", "side", "quantity", "price",
                   "executed_at", "event_ts", "executed_date")
           .withColumn("ingest_ts", current_timestamp())
           .filter(col("trade_id").isNotNull()))

    (src.writeStream.format("hudi")
        .options(**silver_trades_opts())
        .option("path", SILVER_TRADES)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .outputMode("append")
        .trigger(processingTime="15 seconds")
        .start().awaitTermination())


if __name__ == "__main__":
    main()
