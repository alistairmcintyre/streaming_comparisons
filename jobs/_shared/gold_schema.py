"""The ONE definition of gold.open_positions' fields, shared by every engine.

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


def conform(df):
    """Project df onto the canonical gold schema, casting each column to its declared type."""
    from pyspark.sql.functions import col
    return df.select(*[col(c).cast(t).alias(c) for c, t in GOLD_OPEN_POSITIONS])
