"""
Spark Structured Streaming: Kafka accounts (Debezium) -> silver.accounts as SCD2.

Every version is retained. Accounts are a MUTABLE dimension, gen_accounts.py issues
real UPDATEs to country/tier, and in a regulated trading platform you must be able to
answer "what was this client's classification AT THE TIME OF THE TRADE" (MiFID II
categorisation, suitability, best execution). An SCD1 overwrite destroys exactly that.

Validity is MATERIALISED by an atomic close-out: when version N+1 arrives, ONE MERGE
writes the new row and re-writes version N with effective_to set, so a reader never sees
two current rows or none.

    SELECT * FROM silver.accounts WHERE is_current      -- current view
    SELECT * FROM silver.accounts                       -- full history

This was derived at read (LEAD(effective_from) OVER ...) on the argument that close-out
is not expressible in Flink SQL for a PK table. That was wrong: the key is
(account_id, source_lsn), so re-emitting version N MERGES onto the existing row and the
job never needs to know its old effective_from. All five engines materialise it, see
jobs/_shared/scd2.py, and tests/scd2-behaviour.sh for the Flink half.

Deletes are kept as a version with op='d' rather than removed, a closed account must
still be joinable for trades that happened while it was open.

Dedupe of at-least-once CDC re-deliveries is on (account_id, source_lsn): source_lsn is
the CDC total order, so an identical re-delivery collapses while a real change does not.
"""
import os
from iceberg_tables import ensure_all  # in-pipeline DDL
from scd2 import current_for_batch, stage_scd2
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
SOURCE = StructType([StructField("ts_ms", LongType(), True),
                     # source.lsn is projected below as source_lsn (the SCD2 version
                     # and half the merge key). from_json drops anything not declared
                     # here, so omitting it fails the job at analysis, not at runtime.
                     StructField("lsn",   LongType(), True)])
ENVELOPE = StructType([
    StructField("op",     StringType(), True),
    StructField("before", PAYLOAD,      True),
    StructField("after",  PAYLOAD,      True),
    StructField("source", SOURCE,       True),
])

# The dimension's attribute columns. Both the incoming batch and the current-row read
# must project these: a close row is rebuilt from the CURRENT row, so if the read
# omits an attribute the close writes a NULL over it, invisible on a MERGE, which
# only updates two columns; fatal on Hudi, whose upsert replaces the whole record.
ATTRS = ["name", "country", "tier", "source_updated_at", "event_ts", "op", "commit_ts"]



def upsert_scd2(batch, batch_id):
    """One MERGE that both CLOSES the predecessor AND INSERTS the new version.

    Atomic: a reader never sees a moment with two current rows, or none. The staging is
    jobs/_shared/scd2.py, shared with the other Spark engines so the five pipelines cannot
    drift on what SCD2 means.
    """
    spark = batch.sparkSession
    if batch.rdd.isEmpty():
        return
    current = current_for_batch(lambda: spark.table(TABLE), batch, ATTRS)

    stage_scd2(batch, current, attrs=ATTRS).createOrReplaceTempView("_scd2")
    spark.sql(f"""
        MERGE INTO {TABLE} AS t
        USING _scd2 AS s
        ON t.account_id = s.account_id AND t.source_lsn = s.source_lsn
        WHEN MATCHED AND s.action = 'close' THEN UPDATE SET
            t.effective_to = s.effective_to, t.is_current = false
        WHEN NOT MATCHED AND s.action = 'new' THEN INSERT
            (account_id, name, country, tier, source_updated_at, event_ts, effective_from, effective_to, is_current, source_lsn, op, commit_ts)
            VALUES (s.account_id, s.name, s.country, s.tier, s.source_updated_at, s.event_ts, s.effective_from, s.effective_to, s.is_current, s.source_lsn, s.op, s.commit_ts)
    """)


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
           # steady state, but the accounts topic is COMPACTED, so a replay from earliest
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
              # op != d, matching the other three engines. On a delete `after` is
              # null, so without this a version lands with NULL name/country/tier.
              .filter(col("env.op").isNotNull() & (col("env.op") != "d"))
              .select(
                col("env.op").alias("op"),
                coalesce(col("env.after.account_id"), col("env.before.account_id")).alias("account_id"),
                col("env.after.name").alias("name"),
                col("env.after.country").alias("country"),
                col("env.after.tier").alias("tier"),
                to_timestamp(col("env.after.updated_at")).alias("source_updated_at"),
                (col("env.source.ts_ms") / 1000).cast("timestamp").alias("event_ts"),
                # effective_from is the source commit time: when this version became the truth
                # in the source system. Not ingest time, which would make as-of joins depend
                # on pipeline lag.
                (col("env.source.ts_ms") / 1000).cast("timestamp").alias("effective_from"),
                col("env.source.lsn").alias("source_lsn"),
                current_timestamp().alias("commit_ts"),
                col("kafka_offset")))

    (parsed.writeStream.foreachBatch(upsert_scd2)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime="15 seconds")
        .start().awaitTermination())


if __name__ == "__main__":
    main()
