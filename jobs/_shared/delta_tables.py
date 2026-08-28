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
        # SCD2 — ALL VERSIONS RETAINED, validity derived at read.
        # Accounts are a MUTABLE dimension (gen_accounts.py issues real UPDATEs to
        # country/tier), and in a regulated trading platform you must be able to answer
        # "what was this client's classification AT THE TIME OF THE TRADE" — MiFID II
        # categorisation, suitability, best execution. SCD1 overwrites destroy that.
        #
        # Validity is DERIVED, not materialised:
        #     effective_to = LEAD(effective_from) OVER (PARTITION BY account_id
        #                                               ORDER BY effective_from)
        #     is_current   = that LEAD being NULL
        # Materialised close-out (UPDATE the old row's effective_to, INSERT the new) is
        # the classic form and is natural in a Spark MERGE — but it is not expressible in
        # Flink SQL for a PK table, because closing the prior row means targeting
        # (account_id, old_effective_from), which a stateless job does not know. Deriving
        # keeps ALL FIVE engines on an identical model; the cost is a window function at
        # read instead of an extra write.
        #
        # The natural key is (account_id, source_lsn): source_lsn is the CDC total order,
        # so an at-least-once re-delivery collapses onto the same row while a genuine
        # change gets a new one.
        StructField("account_id", LongType(), False), StructField("name", StringType()),
        StructField("country", StringType()), StructField("tier", StringType()),
        StructField("source_updated_at", TimestampType()), StructField("event_ts", TimestampType()),
        # effective_to and is_current are NOT columns here — they are derived, and
        # register-glue-tables.sh publishes silver.accounts_*_scd2 views that do it:
        #     LEAD(effective_from) OVER (PARTITION BY account_id ORDER BY effective_from)
        # Why not materialise them? Close-out (UPDATE the prior row when a new version
        # arrives) is not uniformly achievable: Paimon could via partial-update, Fluss has
        # only first_row/versioned/aggregation. Materialising in three engines and
        # deriving in two would put a PIPELINE difference into the DATA MODEL. The base
        # table is the audit record; the view is the lens, and it costs nothing.
        StructField("effective_from", TimestampType(), False),
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
    """Make existing tables match _TABLE_PROPERTIES (idempotent, safe on new tables)."""
    props = ", ".join(f"'{k}' = '{v}'" for k, v in _TABLE_PROPERTIES.items())
    for rel in _TABLE_PATHS:
        try:
            spark.sql(f"ALTER TABLE delta.`{_BASE}/{rel}` SET TBLPROPERTIES ({props})")
        except Exception as e:  # table may not exist yet on a first run
            print(f"skip property converge for {rel}: {str(e)[:160]}", flush=True)


def ensure_all(spark):
    create_bronze_trades(spark)
    create_silver_trades(spark)
    create_silver_accounts(spark)
    create_gold_open_positions(spark)
    _converge_properties(spark)
