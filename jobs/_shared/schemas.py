"""The ONE definition of every lake table's fields, shared by all five engines.

The five pipelines must return the same fields or the benchmark is comparing different
tables. Four of them declare the schema in DDL (delta_tables.py, iceberg_tables.py, and
the two Flink create_tables.sql) and agree. HUDI HAS NO DDL — a Hudi table's schema is
inferred from whatever DataFrame is written to it — so its gold silently drifted:

    net_notional  decimal(33,4)   <- everyone else declares decimal(38,4)

which is what SUM(int * decimal(12,4)) happens to widen to. Nothing failed; the column was
simply a different type on one engine of five, and only a schema comparison could see it.

conform() makes the Hudi write project this list explicitly, so the inferred schema is
pinned to the declared one. tests/gold_schema_test.py checks all five against it.

NOT part of the contract, deliberately:
  * Hudi's _hoodie_* metadata columns. Every Hudi table carries them; removing them means
    hoodie.populate.meta.fields=false, which disables incremental queries — a real
    capability, given up to make a cosmetic difference go away. The test asserts the
    extras are ONLY those metadata columns.
  * Nullability. Delta and Iceberg declare account_id/symbol NOT NULL; Hudi infers
    nullability from the computed DataFrame and cannot be told otherwise without a
    round-trip through the RDD. Dropping the constraint on two engines to match an
    artifact on a third would make the modelling worse, not more consistent.
"""

# (column, SQL type) in ORDER. Timestamps are microsecond precision everywhere:
# Spark's TIMESTAMP and Flink's TIMESTAMP(6) are the same thing.
GOLD_OPEN_POSITIONS = [
    ("account_id",      "bigint"),
    ("symbol",          "string"),
    ("net_quantity",    "bigint"),
    ("net_notional",    "decimal(38,4)"),
    ("trade_count",     "bigint"),
    ("status",          "string"),
    ("opened_at",       "timestamp"),
    ("last_updated_at", "timestamp"),
    ("commit_ts",       "timestamp"),
]

HUDI_META_PREFIX = "_hoodie_"


def conform(df, schema=None, extra=()):
    """Project df onto a canonical schema, casting each column to its declared type.

    `extra` appends engine-artefact columns that must exist in the written frame but are
    not part of the contract — Hudi's partition field, which Hudi requires as a real column.
    """
    from pyspark.sql.functions import col
    schema = GOLD_OPEN_POSITIONS if schema is None else schema
    return df.select(*[col(c).cast(t).alias(c) for c, t in schema], *extra)


# ── bronze ───────────────────────────────────────────────────────────────────
# The raw CDC landing table: the Debezium envelope flattened, plus Kafka coordinates.
# FLUSS HAS NO BRONZE and is excluded from this contract, not missing from it — its PK
# table IS the cleaned landing table, so it has one hop fewer BY DESIGN (`hops` in
# results.json records it). Inventing a Fluss bronze to make a table count match would
# misrepresent the engine.
BRONZE_TRADES = [
    ("op",              "string"),
    ("trade_id",        "bigint"),
    ("account_id",      "bigint"),
    ("symbol",          "string"),
    ("side",            "string"),
    ("quantity",        "int"),
    ("price",           "decimal(12,4)"),
    ("executed_at",     "timestamp"),
    ("event_ts",        "timestamp"),
    ("ingest_ts",       "timestamp"),
    ("kafka_offset",    "bigint"),
    ("kafka_partition", "int"),
    ("source_lsn",      "bigint"),
]


# ── silver ───────────────────────────────────────────────────────────────────
# silver.trades: the deduplicated fill. source_lsn is the CDC total order and is what Hudi
# precombines on — Delta and Iceberg carried it in BRONZE but dropped it at the silver
# hop, so the documented ordering key existed on three engines of five.
SILVER_TRADES = [
    ("trade_id",    "bigint"),
    ("account_id",  "bigint"),
    ("symbol",      "string"),
    ("side",        "string"),
    ("quantity",    "int"),
    ("price",       "decimal(12,4)"),
    ("executed_at", "timestamp"),
    ("event_ts",    "timestamp"),
    ("ingest_ts",   "timestamp"),
    ("source_lsn",  "bigint"),
]

# silver.accounts: the SCD2 dimension. One row per (account_id, source_lsn) version.
# `op` is load-bearing, not decoration: it marks the version that CLOSED an account, which
# the SCD2 semantics depend on. Paimon and Fluss did not carry it.
SILVER_ACCOUNTS = [
    ("account_id",        "bigint"),
    ("name",              "string"),
    ("country",           "string"),
    ("tier",              "string"),
    ("source_updated_at", "timestamp"),
    ("event_ts",          "timestamp"),
    ("effective_from",    "timestamp"),
    ("effective_to",      "timestamp"),
    ("is_current",        "boolean"),
    ("source_lsn",        "bigint"),
    ("op",                "string"),
    ("commit_ts",         "timestamp"),
]

# Excluded from every parity contract, because they are ENGINE ARTEFACTS, not modelling:
#   _hoodie_*      Hudi metadata, present on every Hudi table.
#   executed_date  Hudi's partition FIELD. Hudi partitions by a column, so the column must
#                  exist; Iceberg expresses the same partitioning as days(executed_at) and
#                  Delta as a clusterBy, neither of which adds a field. Excluding it is the
#                  honest comparison — one physical layout, declared three ways.
PARTITION_ARTEFACTS = {"executed_date"}
