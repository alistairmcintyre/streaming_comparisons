"""
Spark Structured Streaming: Hudi bronze.trades → Hudi silver.trades

Deduplicates on trade_id (upsert). Reads the bronze table incrementally rather than
rescanning it — see STREAMING_DESIGN_PRINCIPLES.md.
"""
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
from hudi_tables import BRONZE_TRADES, SILVER_TRADES as SILVER_TRADES_PATH, silver_trades_opts
from schemas import SILVER_TRADES, conform

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
           .withColumn("ingest_ts", current_timestamp())
           .filter(col("trade_id").isNotNull()))
    # conform(), not a bare select. Hudi has NO DDL — the table's schema is whatever frame
    # is written — so both the field ORDER and the TYPES have to be pinned here or they
    # drift silently, as gold's net_notional did (decimal(33,4) vs the declared (38,4)).
    # This projection previously emitted source_lsn and ingest_ts the other way round.
    # executed_date is Hudi's partition FIELD: Hudi partitions by a column, so it must be
    # present, and it is excluded from the parity contract (PARTITION_ARTEFACTS).
    src = conform(src, SILVER_TRADES, extra=["executed_date"])

    (src.writeStream.format("hudi")
        .options(**silver_trades_opts())
        .option("path", SILVER_TRADES_PATH)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .outputMode("append")
        .trigger(processingTime="15 seconds")
        .start().awaitTermination())


if __name__ == "__main__":
    main()
