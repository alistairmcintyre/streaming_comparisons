"""
Spark Structured Streaming: Kafka (Debezium) → Hudi bronze.trades

Append-only fills, mirroring jobs/spark-delta/bronze_trades.py so the engines differ
only in table format. MOR + inline compaction, see jobs/_shared/hudi_tables.py.
"""
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp, to_timestamp, to_date
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, IntegerType, DecimalType,
)
from schemas import BRONZE_TRADES, conform
from hudi_tables import BRONZE_TRADES as BRONZE_TRADES_PATH, bronze_trades_opts
from latency import observe_event_time, attach_latency_listener

KAFKA_BROKERS   = os.environ.get("KAFKA_BROKERS", "kafka:9092")
TOPIC           = "app.public.trades"
CHECKPOINT_BASE = os.environ.get("CHECKPOINT_BASE", "s3a://warehouse/_chk")
CHECKPOINT_PATH = f"{CHECKPOINT_BASE}/bronze_trades_hudi"

# price arrives as an exact decimal STRING (decimal.handling.mode=string) and is cast
# to DECIMAL, it must never pass through a float.
PAYLOAD = StructType([
    StructField("trade_id",    LongType(),    True),
    StructField("account_id",  LongType(),    True),
    StructField("symbol",      StringType(),  True),
    StructField("side",        StringType(),  True),
    StructField("quantity",    IntegerType(), True),
    StructField("price",       StringType(),  True),
    StructField("executed_at", StringType(),  True),
])
# lsn is Postgres's Log Sequence Number from the Debezium envelope, a STRICT TOTAL
# ORDER across the whole replication stream. kafka_offset only orders within a
# partition, so it is a valid tiebreaker solely because Debezium keys by the table's
# primary key and every version of a trade therefore lands in the same partition; if
# that keying ever changed, offset comparison would silently become meaningless. lsn
# has no such precondition, which is what makes a backfill ranking
#   ROW_NUMBER() OVER (PARTITION BY trade_id ORDER BY event_ts DESC, ingest_ts DESC,
#                      source_lsn DESC)
# robust rather than incidentally correct.
SOURCE = StructType([StructField("ts_ms", LongType(), True),
                     StructField("lsn",   LongType(), True)])
ENVELOPE = StructType([
    StructField("op",     StringType(), True),
    StructField("after",  PAYLOAD,      True),
    StructField("source", SOURCE,       True),
])


def main():
    spark = SparkSession.builder.appName("bronze-trades-hudi").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", KAFKA_BROKERS)
           .option("subscribe", TOPIC)
           .option("startingOffsets", "earliest")
           .option("failOnDataLoss", "false")
           .option("kafka.group.id", "hudi-bronze-trades")
           .option("maxOffsetsPerTrigger", "200000")
           .load()
           .filter(col("value").isNotNull()))

    parsed = (raw.select(
                from_json(col("value").cast("string"), ENVELOPE).alias("env"),
                col("offset").alias("kafka_offset"),
                col("partition").alias("kafka_partition"))
              .filter(col("env.after.trade_id").isNotNull())   # fills are inserts (op c/r)
              .select(
                col("env.op").alias("op"),
                col("env.after.trade_id").alias("trade_id"),
                col("env.after.account_id").alias("account_id"),
                col("env.after.symbol").alias("symbol"),
                col("env.after.side").alias("side"),
                col("env.after.quantity").alias("quantity"),
                col("env.after.price").cast(DecimalType(12, 4)).alias("price"),
                to_timestamp(col("env.after.executed_at")).alias("executed_at"),
                (col("env.source.ts_ms") / 1000).cast("timestamp").alias("event_ts"),
                col("env.source.lsn").alias("source_lsn"),
                current_timestamp().alias("ingest_ts"),
                col("kafka_offset"), col("kafka_partition"))
              .withColumn("executed_date", to_date(col("executed_at"))))

    # conform(): Hudi has NO DDL, so this projection's order and inferred types BECOME the
    # table's schema. It emitted source_lsn before ingest_ts and both before the kafka
    # coordinates, where delta/iceberg/paimon all DECLARE ..., ingest_ts, kafka_offset,
    # kafka_partition, source_lsn. executed_date is Hudi's partition field (schemas.py:
    # PARTITION_ARTEFACTS) and has to stay in the written frame.
    parsed = conform(parsed, BRONZE_TRADES, extra=["executed_date"])

    # observe() costs no extra action; the listener emits after each batch COMMITS
    # (for Hudi MOR that includes the inline compaction the write triggers).
    attach_latency_listener(spark, "hudi-bronze")
    observed = observe_event_time(parsed)

    (observed.writeStream.format("hudi")
        .options(**bronze_trades_opts())
        .option("path", BRONZE_TRADES_PATH)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .start().awaitTermination())


if __name__ == "__main__":
    main()
