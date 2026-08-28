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
    # NO per-batch admission control. Delta caps its stream with maxFilesPerTrigger
    # and Iceberg with streaming-max-files-per-micro-batch; Hudi 1.2.0 exposes no
    # equivalent — there is no ReadLimit machinery and no hoodie.* option for it
    # (checked against hudi-spark4.0-bundle_2.13-1.2.0.jar). So after a stall this
    # source pulls the whole backlog in one micro-batch.
    # This is a real capability difference, not an oversight, and it makes Hudi's
    # batch sizes incomparable to the other engines' under recovery — recorded in
    # results.json rather than papered over. The KEEP_LATEST_BY_HOURS cleaner
    # retention in hudi_tables.py is what stops the worst case (losing the start
    # instant and silently falling back to a full-table scan).
    src = (spark.readStream.format("hudi")
           .load(BRONZE_TRADES)
           .select("trade_id", "account_id", "symbol", "side", "quantity", "price",
                   "executed_at", "event_ts", "executed_date",
                     # ordering key for last-wins (precombine); see hudi_tables.py
                     "source_lsn")
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
