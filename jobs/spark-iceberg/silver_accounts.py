"""
Spark Structured Streaming: Kafka accounts (Debezium) -> silver.accounts as SCD2.

Every version is retained. Accounts are a MUTABLE dimension — gen_accounts.py issues
real UPDATEs to country/tier — and in a regulated trading platform you must be able to
answer "what was this client's classification AT THE TIME OF THE TRADE" (MiFID II
categorisation, suitability, best execution). An SCD1 overwrite destroys exactly that.

Validity is DERIVED rather than materialised:

    SELECT *, LEAD(effective_from) OVER (PARTITION BY account_id
                                         ORDER BY effective_from) AS effective_to
    FROM silver.accounts
    -- current version: effective_to IS NULL

Materialised close-out (UPDATE the prior row's effective_to, INSERT the new one) is the
classic form and is natural in a Spark MERGE, but it is NOT expressible in Flink SQL for
a PK table: closing the prior row means targeting (account_id, old_effective_from), which
a stateless job does not know. Deriving keeps all five engines on an identical model at
the cost of a window function at read.

Deletes are kept as a version with op='d' rather than removed — a closed account must
still be joinable for trades that happened while it was open.

Dedupe of at-least-once CDC re-deliveries is on (account_id, source_lsn): source_lsn is
the CDC total order, so an identical re-delivery collapses while a real change does not.
"""
import os
from iceberg_tables import ensure_all  # in-pipeline DDL
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, from_json, current_timestamp, to_timestamp, coalesce, row_number,
)
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, LongType

KAFKA_BROKERS   = os.environ.get("KAFKA_BROKERS", "kafka:9092")
TOPIC           = "app.public.accounts"
CHECKPOINT_BASE = os.environ.get("CHECKPOINT_BASE", "s3a://warehouse/_chk")
CHECKPOINT_PATH = f"{CHECKPOINT_BASE}/silver_accounts_spark"
TABLE           = "rest.silver.accounts_spark"

PAYLOAD = StructType([
    StructField("account_id", LongType(),   True),
    StructField("name",       StringType(), True),
    StructField("country",    StringType(), True),
    StructField("tier",       StringType(), True),
    StructField("updated_at", StringType(), True),
])
SOURCE = StructType([StructField("ts_ms", LongType(), True)])
ENVELOPE = StructType([
    StructField("op",     StringType(), True),
    StructField("before", PAYLOAD,      True),
    StructField("after",  PAYLOAD,      True),
    StructField("source", SOURCE,       True),
])



def main():
    spark = SparkSession.builder.appName("silver-accounts-spark").getOrCreate()
    ensure_all(spark)  # idempotent create-if-not-exists; no separate ddl-init
    spark.sparkContext.setLogLevel("WARN")

    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", KAFKA_BROKERS)
           .option("subscribe", TOPIC)
           .option("startingOffsets", "earliest")
           # Bound the catch-up batch, as bronze_trades already does (at 200k). Without
           # it, a restart with a backlog makes the FIRST micro-batch attempt the whole
           # backlog at once. accounts is a low-volume SCD trickle so this never bites in
           # steady state — but the accounts topic is COMPACTED, so a replay from earliest
           # delivers one record per account in a single burst, which is exactly the
           # unbounded first batch this caps.
           .option("maxOffsetsPerTrigger", os.environ.get("MAX_OFFSETS_PER_TRIGGER", "100000"))
           .option("failOnDataLoss", "false")
           .option("kafka.group.id", "spark-silver-accounts")
           .load()
           .filter(col("value").isNotNull()))

    parsed = (raw.select(
                from_json(col("value").cast("string"), ENVELOPE).alias("env"),
                col("offset").alias("kafka_offset"))
              .filter(col("env.op").isNotNull())
              .select(
                col("env.op").alias("op"),
                coalesce(col("env.after.account_id"), col("env.before.account_id")).alias("account_id"),
                col("env.after.name").alias("name"),
                col("env.after.country").alias("country"),
                col("env.after.tier").alias("tier"),
                to_timestamp(col("env.after.updated_at")).alias("source_updated_at"),
                (col("env.source.ts_ms") / 1000).cast("timestamp").alias("event_ts"),
                # effective_from IS the source commit time: when this version became the truth
                # in the source system. Not ingest time, which would make as-of joins depend
                # on pipeline lag.
                (col("env.source.ts_ms") / 1000).cast("timestamp").alias("effective_from"),
                col("env.source.lsn").alias("source_lsn"),
                current_timestamp().alias("commit_ts"),
                col("kafka_offset")))

    # Append EVERY version. dropDuplicates on the natural key (account_id, source_lsn)
    # collapses at-least-once CDC re-deliveries — byte-identical rows — while leaving
    # genuine versions alone. source_lsn is monotonic, so a re-delivery lands close
    # behind its original and the watermark bounds the state.
    versions = (parsed
                .withWatermark("event_ts", "1 hour")
                .dropDuplicatesWithinWatermark(["account_id", "source_lsn"])
                .select("account_id", "name", "country", "tier", "source_updated_at",
                        "event_ts", "effective_from", "source_lsn", "op", "commit_ts"))

    (versions.writeStream.format("iceberg").outputMode("append")
        .toTable(TABLE)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime="15 seconds")
        .start().awaitTermination())


if __name__ == "__main__":
    main()
