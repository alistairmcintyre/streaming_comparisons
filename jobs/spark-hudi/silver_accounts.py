"""
Spark Structured Streaming: Kafka (Debezium) → Hudi silver.accounts

SCD2 dimension: EVERY version is retained. The composite record key (account_id,
source_lsn) means a new version inserts while an at-least-once re-delivery collapses,
and the staged close row rewrites the superseded version in the same commit.
"""
import os
from scd2 import stage_scd2
from schemas import SILVER_ACCOUNTS, conform
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp, to_timestamp, coalesce
from pyspark.sql.types import StructType, StructField, StringType, LongType
from hudi_tables import SILVER_ACCOUNTS as SILVER_ACCOUNTS_PATH, silver_accounts_opts

KAFKA_BROKERS   = os.environ.get("KAFKA_BROKERS", "kafka:9092")
TOPIC           = "app.public.accounts"
CHECKPOINT_BASE = os.environ.get("CHECKPOINT_BASE", "s3a://warehouse/_chk")
CHECKPOINT_PATH = f"{CHECKPOINT_BASE}/silver_accounts_hudi"

PAYLOAD = StructType([
    StructField("account_id", LongType(),   True),
    StructField("name",       StringType(), True),
    StructField("country",    StringType(), True),
    StructField("tier",       StringType(), True),
    StructField("updated_at", StringType(), True),
])
SOURCE = StructType([StructField("ts_ms", LongType(), True),
                     StructField("lsn",   LongType(), True)])
ENVELOPE = StructType([
    StructField("op",     StringType(), True),
    StructField("before", PAYLOAD,      True),
    StructField("after",  PAYLOAD,      True),
    StructField("source", SOURCE,       True),
])

# The dimension's attribute columns. Both the incoming batch AND the current-row read
# must project these: a close row is rebuilt from the CURRENT row, so if the read
# omits an attribute the close writes a NULL over it — invisible on a MERGE, which
# only updates two columns; fatal on Hudi, whose upsert replaces the whole record.
ATTRS = ["name", "country", "tier", "source_updated_at", "event_ts", "op"]


def upsert_scd2(batch, batch_id):
    """Same staging as Delta/Iceberg (jobs/_shared/scd2.py); the WRITE differs.

    Hudi has no MERGE with per-clause conditions here, but it does not need one: the record
    key is (account_id, source_lsn), so upserting the staged rows CLOSES the predecessor
    (its key already exists, so the row is replaced with effective_to set) and INSERTS the
    new version (its key is new) in a single commit. Same atomicity, different primitive —
    which is exactly the kind of difference this benchmark exists to show.
    """
    spark = batch.sparkSession
    if batch.rdd.isEmpty():
        return
    ids = [r[0] for r in batch.select("account_id").distinct().collect()]
    try:
        current = (spark.read.format("hudi").load(SILVER_ACCOUNTS_PATH)
                   .filter(col("is_current") & col("account_id").isin(ids))
                   .select("account_id", "source_lsn", "effective_from", *ATTRS))
    except Exception:                      # first batch: the table does not exist yet
        current = spark.createDataFrame(
            [], batch.select("account_id", "source_lsn", "effective_from", *ATTRS).schema)

    staged = stage_scd2(batch, current,
                        attrs=ATTRS)
    # conform(), not a bare write. stage_scd2 emits its own column order (key, version,
    # validity, then attributes) and Hudi has NO DDL, so that order — and the inferred
    # types — would become the table's schema, differing from the four engines that
    # DECLARE this table. Pinned to jobs/_shared/schemas.py:SILVER_ACCOUNTS.
    out = conform(staged.drop("action").withColumn("commit_ts", current_timestamp()),
                  SILVER_ACCOUNTS)
    (out.write.format("hudi").options(**silver_accounts_opts())
        .mode("append").save(SILVER_ACCOUNTS_PATH))


def main():
    spark = SparkSession.builder.appName("silver-accounts-hudi").getOrCreate()
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
           .option("kafka.group.id", "hudi-silver-accounts")
           .load()
           .filter(col("value").isNotNull()))

    # `after` is null on delete (op=d); fall back to `before` to keep the key.
    parsed = (raw.select(from_json(col("value").cast("string"), ENVELOPE).alias("env"))
              .select(
                col("env.op").alias("op"),
                coalesce(col("env.after.account_id"), col("env.before.account_id")).alias("account_id"),
                col("env.after.name").alias("name"),
                col("env.after.country").alias("country"),
                col("env.after.tier").alias("tier"),
                to_timestamp(col("env.after.updated_at")).alias("source_updated_at"),
                (col("env.source.ts_ms") / 1000).cast("timestamp").alias("event_ts"),
                # effective_from = source commit time; source_lsn = CDC total order and the
                # version half of the composite record key.
                (col("env.source.ts_ms") / 1000).cast("timestamp").alias("effective_from"),
                col("env.source.lsn").alias("source_lsn"),
                current_timestamp().alias("commit_ts"))
              .filter(col("account_id").isNotNull() & (col("op") != "d")))

    (parsed.writeStream.foreachBatch(upsert_scd2)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime="15 seconds")
        .start().awaitTermination())


if __name__ == "__main__":
    main()
