"""
Delta table DDL as importable, idempotent functions — run IN the pipeline.

Each streaming job calls ensure_all(spark) at startup (after building the session,
before streaming). createIfNotExists is create-once + idempotent, so every stage can
self-provision the tables it reads/writes with no separate ddl-init and no ordering
dependency. Compaction is in-pipeline (delta.autoOptimize.optimizeWrite/.autoCompact);
VACUUM is the only separate job. Warehouse base is env-driven (local MinIO / AWS S3).

NOTE: create-once semantics — changing a schema/props in code only lands on a fresh
table (fine for ephemeral benchmark runs; a long-lived table needs explicit migration).
"""
import os
from delta.tables import DeltaTable
from pyspark.sql.types import (
    BooleanType,
    StructType, StructField, StringType, LongType, IntegerType, DecimalType, TimestampType,
)

_BASE = os.environ.get("DELTA_WAREHOUSE", "s3a://warehouse/delta")



def _is_delta(spark, rel):
    """True if a Delta table already exists at this location."""
    try:
        return DeltaTable.isDeltaTable(spark, f"{_BASE}/{rel}")
    except Exception:
        return False


def create_bronze_trades(spark):
    if _is_delta(spark, "bronze/trades"):
        return  # exists: properties are converged by _converge_properties()
    schema = StructType([
        StructField("op", StringType()), StructField("trade_id", LongType()),
        StructField("account_id", LongType()), StructField("symbol", StringType()),
        StructField("side", StringType()), StructField("quantity", IntegerType()),
        StructField("price", DecimalType(12, 4)), StructField("executed_at", TimestampType()),
        StructField("event_ts", TimestampType()), StructField("ingest_ts", TimestampType()),
        StructField("kafka_offset", LongType()), StructField("kafka_partition", IntegerType()),
        # Postgres LSN: a strict TOTAL order across the replication stream, unlike
        # kafka_offset which only orders within a partition. The definitive tiebreaker
        # for a backfill ranking on (event_ts, ingest_ts, source_lsn).
        StructField("source_lsn", LongType()),
    ])
    (DeltaTable.createIfNotExists(spark)
        .tableName("delta_bronze_trades").addColumns(schema)
        .location(f"{_BASE}/bronze/trades").clusterBy("executed_at")
        .property("delta.enableDeletionVectors", "false")
        .property("delta.autoOptimize.optimizeWrite", "true")
        .property("delta.autoOptimize.autoCompact", "true")
        .execute())


def create_silver_trades(spark):
    if _is_delta(spark, "silver/trades"):
        return  # exists: properties are converged by _converge_properties()
    schema = StructType([
        StructField("trade_id", LongType(), False), StructField("account_id", LongType()),
        StructField("symbol", StringType()), StructField("side", StringType()),
        StructField("quantity", IntegerType()), StructField("price", DecimalType(12, 4)),
        StructField("executed_at", TimestampType()), StructField("event_ts", TimestampType()),
        StructField("ingest_ts", TimestampType()),
        # The CDC total order. bronze carried it and silver dropped it, so the ordering
        # key this repo documents everywhere existed on three engines of five.
        StructField("source_lsn", LongType()),
    ])
    (DeltaTable.createIfNotExists(spark)
        .tableName("delta_silver_trades").addColumns(schema)
        .location(f"{_BASE}/silver/trades").clusterBy("executed_at")
        .property("delta.enableDeletionVectors", "false")
        .property("delta.autoOptimize.optimizeWrite", "true")
        .property("delta.autoOptimize.autoCompact", "true")
        .execute())


def create_silver_accounts(spark):
    if _is_delta(spark, "silver/accounts"):
        return  # exists: properties are converged by _converge_properties()
    schema = StructType([
        # SCD2 — ALL VERSIONS RETAINED, validity MATERIALISED.
        # Accounts are a MUTABLE dimension (gen_accounts.py issues real UPDATEs to
        # country/tier), and in a regulated trading platform you must be able to answer
        # "what was this client's classification AT THE TIME OF THE TRADE" — MiFID II
        # categorisation, suitability, best execution. SCD1 overwrites destroy that.
        #
        # effective_to and is_current are WRITTEN, by an atomic close-out: when version
        # N+1 arrives the job emits BOTH the new row AND version N again with effective_to
        # set, in ONE MERGE, so a reader never sees two current rows or none.
        # This was DERIVED at read (LEAD(effective_from) OVER ...) on the argument that
        # close-out is not expressible in Flink SQL for a PK table. That turned out to be
        # wrong: the PK is (account_id, source_lsn), so re-emitting version N MERGES onto
        # the existing row — no need to know its old effective_from. All five engines
        # materialise it. See jobs/_shared/scd2.py and tests/scd2-behaviour.sh.
        #
        # The natural key is (account_id, source_lsn): source_lsn is the CDC total order,
        # so an at-least-once re-delivery collapses onto the same row while a genuine
        # change gets a new one.
        StructField("account_id", LongType(), False), StructField("name", StringType()),
        StructField("country", StringType()), StructField("tier", StringType()),
        StructField("source_updated_at", TimestampType()), StructField("event_ts", TimestampType()),
        # effective_to / is_current ARE columns, closed by the MERGE described above.
        StructField("effective_from", TimestampType(), False),
        # MATERIALISED by the atomic close-out in silver_accounts.py — when version
        # N+1 arrives, one MERGE both closes N and inserts N+1.
        StructField("effective_to", TimestampType()),
        StructField("is_current", BooleanType()),
        StructField("source_lsn", LongType(), False),
        StructField("op", StringType()),          # 'd' marks the version that closed the account
        StructField("commit_ts", TimestampType()),
    ])
    (DeltaTable.createIfNotExists(spark)
        .tableName("delta_silver_accounts").addColumns(schema)
        .location(f"{_BASE}/silver/accounts").clusterBy("account_id")
        .property("delta.enableDeletionVectors", "true")
        .property("delta.autoOptimize.optimizeWrite", "true")
        .property("delta.autoOptimize.autoCompact", "true")
        .execute())


def create_gold_open_positions(spark):
    if _is_delta(spark, "gold/open_positions"):
        return  # exists: properties are converged by _converge_properties()
    schema = StructType([
        StructField("account_id", LongType(), False), StructField("symbol", StringType(), False),
        StructField("net_quantity", LongType()), StructField("net_notional", DecimalType(38, 4)),
        StructField("trade_count", LongType()), StructField("status", StringType()),
        # country/tier are NOT denormalised here. They are account attributes, not
        # position attributes, and a current-state table has no defensible temporal
        # semantic for them: the value would be "whatever the last batch that happened
        # to touch this row saw" — neither the account's country now, nor its country
        # at the fill. gen_accounts.py trickles real SCD updates, so that is live, not
        # theoretical. Enrich at query time instead:
        #   SELECT p.*, a.country, a.tier
        #   FROM gold.open_positions p LEFT JOIN silver.accounts a USING (account_id)
        # LEFT, always: the trades and accounts CDC streams are independent, so a fill
        # can land before its account row. An inner join would silently drop those
        # positions from the book.
        # EVENT-time lineage: opened_at = MIN(executed_at) for the position,
        # last_updated_at = MAX(executed_at). commit_ts stays PROCESSING time, so
        # (commit_ts - last_updated_at) is a per-row processing delay, uniform across
        # engines and computable from the table itself with no emit chain involved.
        # opened_at is NOT reset when a flat position reopens: that is easy here in
        # the MERGE and impossible as a pure Flink fold, so it would make the Spark
        # and Flink golds disagree on identical input.
        StructField("opened_at", TimestampType()),
        StructField("last_updated_at", TimestampType()),
        StructField("commit_ts", TimestampType()),
    ])
    (DeltaTable.createIfNotExists(spark)
        .tableName("delta_gold_open_positions").addColumns(schema)
        .location(f"{_BASE}/gold/open_positions").clusterBy("symbol", "account_id")
        .property("delta.enableDeletionVectors", "true")
        .property("delta.autoOptimize.optimizeWrite", "true")
        .property("delta.autoOptimize.autoCompact", "true")
        .execute())


# Table properties we want to hold on EVERY layer. createIfNotExists() FAILS with
# DELTA_CREATE_TABLE_WITH_DIFFERENT_PROPERTY when a table already exists with a
# different property set, so changing this list would break every restart against
# tables created by an earlier build. Converge with ALTER TABLE instead: create is
# for new tables, ALTER makes existing ones match.
_TABLE_PROPERTIES = {
    "delta.autoOptimize.optimizeWrite": "true",
    "delta.autoOptimize.autoCompact": "true",
}

_TABLE_PATHS = ["bronze/trades", "silver/trades", "silver/accounts", "gold/open_positions"]


def _converge_properties(spark):
    """Make existing tables match _TABLE_PROPERTIES, ALTERing ONLY when they differ.

    READ BEFORE WRITE, and the reason is not tidiness. ALTER TABLE ... SET TBLPROPERTIES
    writes a Delta METADATA COMMIT even when every value is already correct, and a metadata
    change on a table another job is STREAMING FROM kills that stream outright:

        DELTA_METADATA_CHANGED: The metadata of the Delta table has been changed by a
        concurrent update. Please try the operation again.

    All four Delta jobs call ensure_all() at startup and this loop covers ALL FOUR tables,
    so every job start — and every restart — could kill the other jobs' source streams.
    Seen live: delta-silver-trades failed repeatedly and ended the run 37 source versions
    behind bronze, 2,240,000 rows against bronze's 4,086,000. Its invariant still read `ok`,
    because that check compares gold to SILVER rather than to bronze, so a silver that has
    fallen behind passes cleanly.

    The previous version called itself "idempotent" and was — in final STATE. It was not a
    no-op, which is the property that actually mattered here.
    """
    for rel in _TABLE_PATHS:
        tbl = f"delta.`{_BASE}/{rel}`"
        try:
            cur = {r["key"]: r["value"]
                   for r in spark.sql(f"SHOW TBLPROPERTIES {tbl}").collect()}
        except Exception as e:  # table may not exist yet on a first run
            print(f"skip property converge for {rel}: {str(e)[:160]}", flush=True)
            continue
        drift = {k: v for k, v in _TABLE_PROPERTIES.items() if cur.get(k) != v}
        if not drift:
            continue
        props = ", ".join(f"'{k}' = '{v}'" for k, v in drift.items())
        try:
            spark.sql(f"ALTER TABLE {tbl} SET TBLPROPERTIES ({props})")
            print(f"converged {rel}: {sorted(drift)}", flush=True)
        except Exception as e:
            print(f"skip property converge for {rel}: {str(e)[:160]}", flush=True)


def ensure_all(spark):
    create_bronze_trades(spark)
    create_silver_trades(spark)
    create_silver_accounts(spark)
    create_gold_open_positions(spark)
    _converge_properties(spark)
