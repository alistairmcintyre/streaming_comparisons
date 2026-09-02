"""
Spark Structured Streaming: Delta bronze.trades → Delta silver.trades.

Cleaned + deduped fills (still append, fills are immutable events, not a
current-view). Dedups exact re-deliveries on trade_id within a watermark (bounded
state), drops the Kafka envelope/offsets. This is where derived/enriched fields
would be added, none required yet, so it's a clean passthrough.
"""
import os
from pyspark.sql import SparkSession
from schemas import SILVER_TRADES as SILVER_TRADES_FIELDS, conform
from latency import observe_event_time, attach_latency_listener
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
        # Dedupe window of 2 hours. The number matters:
        # This is the only place a re-delivered trade_id can be removed. Paimon and Fluss
        # hold silver.trades as a PK table with the first-row merge engine, and Hudi as an
        # upsert on trade_id, so on those three a duplicate CANNOT become a second row, 
        # the dedupe is structural and unbounded. Delta and Iceberg keep silver.trades
        # APPEND-ONLY (gold streams it, and a streaming read of an updating table either
        # fails or re-emits rewritten files, which would double-count far worse), so here
        # the dedupe lives in operator STATE instead, and state has to be bounded.
        # A re-delivery arriving after the window is appended as a genuine second row, and
        # NOTHING downstream can undo it: the gold fold is `+=` over (account_id, symbol)
        # and has no memory of which trade_ids it has already folded, so the position is
        # then permanently wrong on two of the five engines.
        # 1 HOUR, DELIBERATELY SHORTER THAN THE RUN. This was 2h, chosen to span a whole
        # 120-minute run so the window covered the entire event-time range and all five
        # engines deduped identically. 1h halves the state for the same throughput, which
        # is the point: at 1k/s the window holds one entry per distinct trade_id, so ~3.6M
        # instead of ~7.2M, on executors that were being OOMKilled at their pod limit.
        # THE EXPOSURE, stated rather than buried: a re-delivery arriving more than an
        # hour after the original is no longer deduped, and on Delta and Iceberg it lands
        # as a genuine second row that nothing downstream can remove. That is a real hole
        # and it is accepted on the grounds that Debezium re-delivers on redelivery, which
        # is seconds, not hours. Raise it back to match run length if a run ever shows
        # duplicate trade_ids in the correctness snapshot.
        # State lives on disk, not heap: the manifests set RocksDBStateStoreProvider. The
        # cost is still real, since RocksDB is native memory charged to the container.
        .withWatermark("event_ts", "1 hour")
        .dropDuplicatesWithinWatermark(["trade_id"])
        # conform(), not a bare select: this is a POSITIONAL append, and Iceberg matches
        # the frame to the table by position, not name. The bronze job got this wrong and
        # crash-looped on `source_lsn is out of order`. One canon decides the order here
        # and in the DDL. (source_lsn is the CDC total order and Hudi's precombine key, 
        # bronze carried it all along and silver used to drop it.)
    )
    cleaned = conform(cleaned, SILVER_TRADES_FIELDS)

    # LATENCY EMIT for the bronze->silver hop. Until now only bronze and gold emitted, so
    # the dashboard could show end-to-end and gold but not where time goes in the middle, 
    # on four of five engines. Silver is where the dedupe and the SCD2 work happen, so an
    # unmeasured silver hop is the least useful one to be missing.
    attach_latency_listener(spark, "delta-silver")
    cleaned = observe_event_time(cleaned)
    (cleaned.writeStream.format("delta").outputMode("append")
        .option("path", SILVER_TRADES)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime="10 seconds")
        .start().awaitTermination())


if __name__ == "__main__":
    main()
