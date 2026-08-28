"""SCD2 close-out staging — shared by the three Spark engines.

Given a micro-batch of new dimension versions and the CURRENT rows for the affected keys,
produce the rows to write:

  action='new'    the incoming version, with effective_to / is_current computed
  action='close'  its predecessor, to have effective_to set and is_current cleared

Each engine then writes that staging set its own way — Delta and Iceberg with a single
MERGE on (key, version), Hudi with an upsert on the same composite record key — but the
LOGIC lives here once, so the five pipelines cannot drift on what SCD2 means.

Two rules, both learned from tests/scd2-behaviour.sh catching them in the Flink version:

  * An OUT-OF-ORDER arrival (version <= the current one) is NOT current. It is a late
    historical row, valid until the row that already superseded it. Marking it current
    gives the key TWO current rows, which silently fans out every downstream join and
    passes every compile check.
  * A re-delivery (same key AND same version) must collapse, not duplicate. That falls
    out of the version being part of the key.
"""
from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import col, lit, row_number, when, coalesce


def stage_scd2(batch: DataFrame, current: DataFrame, attrs,
               key: str = "account_id", version: str = "source_lsn",
               eff_from: str = "effective_from") -> DataFrame:
    """batch: incoming versions. current: the is_current row per key (may be empty).
    attrs: attribute column names carried on the dimension."""
    # One row per (key, version): an at-least-once re-delivery is byte-identical, so any
    # of them will do.
    w_dedupe = Window.partitionBy(key, version).orderBy(col(version))
    b = (batch.withColumn("_rn", row_number().over(w_dedupe))
              .filter(col("_rn") == 1).drop("_rn"))

    # The predecessor of each incoming version: the newest EARLIER version, taken from the
    # batch itself where present, else the table's current row. Doing it batch-first
    # matters — two versions of one account can land in the same micro-batch, and only
    # looking at the table would miss the first closing the second.
    w_prev = Window.partitionBy(key).orderBy(col(version))
    from pyspark.sql.functions import lag
    b = (b.withColumn("_prev_ver_in_batch", lag(col(version)).over(w_prev))
          .withColumn("_prev_eff_in_batch", lag(col(eff_from)).over(w_prev)))

    cur = current.select(col(key).alias("_c_key"),
                         col(version).alias("_c_ver"),
                         col(eff_from).alias("_c_eff"))
    b = b.join(cur, b[key] == cur["_c_key"], "left")

    b = (b.withColumn("_prev_ver", coalesce(col("_prev_ver_in_batch"), col("_c_ver")))
          .withColumn("_prev_eff", coalesce(col("_prev_eff_in_batch"), col("_c_eff"))))

    # Is this version the newest we have seen for the key?
    newest = col("_prev_ver").isNull() | (col(version) > col("_prev_ver"))

    new_rows = (b.withColumn("effective_to",
                             when(newest, lit(None).cast("timestamp")).otherwise(col("_prev_eff")))
                 .withColumn("is_current", newest)
                 .withColumn("action", lit("new"))
                 .select(key, version, eff_from, "effective_to", "is_current", "action", *attrs))

    # Close the predecessor — only when this version genuinely supersedes it.
    close_rows = (b.filter(col("_prev_ver").isNotNull() & (col(version) > col("_prev_ver")))
                   .withColumn("action", lit("close"))
                   .select(col(key), col("_prev_ver").alias(version),
                           col("_prev_eff").alias(eff_from),
                           col(eff_from).alias("effective_to"),
                           lit(False).alias("is_current"), col("action"),
                           # NULL of the SAME TYPE as the batch column. Casting these to
                           # string worked only because the first test used a string
                           # attribute; unionByName fails on a type mismatch otherwise.
                           *[lit(None).cast(batch.schema[a].dataType).alias(a) for a in attrs]))

    return new_rows.unionByName(close_rows)
