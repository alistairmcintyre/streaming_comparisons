"""
Spark Structured Streaming: Delta bronze.trades → Delta silver.trades.

Cleaned + deduped fills (still append — fills are immutable events, NOT a
current-view). Dedups exact re-deliveries on trade_id within a watermark (bounded
state), drops the Kafka envelope/offsets. This is where derived/enriched fields
would be added — none required yet, so it's a clean passthrough.
"""
import os
from pyspark.sql import SparkSession
from schemas import SILVER_TRADES as SILVER_TRADES_FIELDS, conform
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
        # DEDUPE WINDOW — 2 hours, and the number is load-bearing.
        # This is the ONLY place a re-delivered trade_id can be removed. Paimon and Fluss
        # hold silver.trades as a PK table with the first-row merge engine, and Hudi as an
        # upsert on trade_id, so on those three a duplicate CANNOT become a second row —
        # the dedupe is structural and unbounded. Delta and Iceberg keep silver.trades
        # APPEND-ONLY (gold streams it, and a streaming read of an updating table either
        # fails or re-emits rewritten files, which would double-count far worse), so here
        # the dedupe lives in operator STATE instead, and state has to be bounded.
        # A re-delivery arriving after the window is appended as a genuine second row, and
        # NOTHING downstream can undo it: the gold fold is `+=` over (account_id, symbol)
        # and has no memory of which trade_ids it has already folded, so the position is
        # then permanently wrong on two of the five engines. 2h is chosen to cover a whole
        # run: run_minutes defaults to 120 and the EventBridge kill switch fires at 150,
        # so for the runs this benchmark actually performs the window spans the entire
        # event-time range and all five engines dedupe identically. It is a MATCH to the
        # run length, not a margin over it — a run configured longer than 2h reopens the
        # gap, and the window must be raised with it.
        # COST: dropDuplicatesWithinWatermark state is one entry per distinct trade_id in
        # the window, heap-resident under the default HDFS-backed state store. At 1k/s
        # that is ~7.2M entries at 2h (~2x the 1h figure).
        .withWatermark("event_ts", "2 hours")
        .dropDuplicatesWithinWatermark(["trade_id"])
        # conform(), not a bare select: this is a POSITIONAL append, and Iceberg matches
        # the frame to the table by position, not name. The bronze job got this wrong and
        # crash-looped on `source_lsn is out of order`. One canon decides the order here
        # and in the DDL. (source_lsn is the CDC total order and Hudi's precombine key —
        # bronze carried it all along and silver used to drop it.)
    )
    cleaned = conform(cleaned, SILVER_TRADES_FIELDS)

    (cleaned.writeStream.format("delta").outputMode("append")
        .option("path", SILVER_TRADES)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime="10 seconds")
        .start().awaitTermination())


if __name__ == "__main__":
    main()
