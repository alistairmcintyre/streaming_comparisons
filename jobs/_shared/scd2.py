"""SCD2 close-out staging — shared by the three Spark engines.

Given a micro-batch of new dimension versions and the CURRENT row for each affected key,
produce the rows to write:

  action='new'    an incoming version, with effective_to / is_current computed
  action='close'  the table's existing current row, superseded by this batch

Each engine then writes that staging set its own way — Delta and Iceberg with a single
MERGE on (key, version), Hudi with an upsert on the same composite record key — but the
LOGIC lives here once, so the five pipelines cannot drift on what SCD2 means.

TWO INVARIANTS THIS FILE OWES ITS CALLERS, both learned the hard way:

  * NO KEY APPEARS TWICE in the returned set. Delta and Iceberg tolerate a duplicate
    (whichever clause fails to match is a no-op); Hudi does not — its upsert combines
    same-key rows by precombine, and on a tie the winner is arbitrary. So within-batch
    supersession is expressed on the row ITSELF via LEAD, and a 'close' is only ever
    emitted for the row already IN the table.
  * A 'close' ROW IS A COMPLETE RECORD, carrying the closed version's real attributes.
    A MERGE close is an UPDATE of two columns so nulls there are invisible, but Hudi's
    upsert replaces the whole record — nulling the attributes ERASES the history the
    close exists to preserve. Verified by tests/scd2_hudi_upsert_test.py, which caught
    exactly this after the Delta and Iceberg tests both passed.

And two rules on ordering, from tests/scd2-behaviour.sh catching them in the Flink version:

  * An OUT-OF-ORDER arrival (version < the current one) is NOT current. It is a late
    historical row, valid until the row that already superseded it. Marking it current
    gives the key TWO current rows, which silently fans out every downstream join and
    passes every compile check.
  * A re-delivery (same key AND same version as the current row) is dropped outright.
    Restating it as a non-current row is not harmless: on Hudi it overwrites the live
    row with is_current=false and the key ends up with NO current version at all.
"""
from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import col, lead, lit, row_number, when


def stage_scd2(batch: DataFrame, current: DataFrame, attrs,
               key: str = "account_id", version: str = "source_lsn",
               eff_from: str = "effective_from") -> DataFrame:
    """batch: incoming versions. current: the is_current row per affected key, which MUST
    carry the attribute columns as well (they are what the close row is rebuilt from) and
    may be empty. attrs: attribute column names carried on the dimension."""
    # One row per (key, version): an at-least-once re-delivery is byte-identical, so any
    # of them will do.
    w_key = Window.partitionBy(key).orderBy(col(version))
    b = (batch.withColumn("_rn", row_number().over(Window.partitionBy(key, version)
                                                   .orderBy(col(version))))
              .filter(col("_rn") == 1).drop("_rn"))

    # The version that supersedes each incoming row WITHIN this batch, if any. Two versions
    # of one account can land in the same micro-batch, and the earlier one is then already
    # historical on arrival — it must never be written as current and then fixed up later.
    b = (b.withColumn("_next_ver", lead(col(version)).over(w_key))
          .withColumn("_next_eff", lead(col(eff_from)).over(w_key)))

    cur = current.select(col(key).alias("_c_key"),
                         col(version).alias("_c_ver"),
                         col(eff_from).alias("_c_eff"),
                         *[col(a).alias(f"_c_{a}") for a in attrs])
    b = b.join(cur, b[key] == cur["_c_key"], "left")

    # Drop exact re-deliveries of the version already sitting current in the table.
    fresh = b.filter(col("_c_ver").isNull() | (col(version) != col("_c_ver")))

    supersedes_table = col("_c_ver").isNull() | (col(version) > col("_c_ver"))
    new_rows = (fresh
        .withColumn("effective_to",
                    # closed by a later version in this batch …
                    when(col("_next_ver").isNotNull(), col("_next_eff"))
                    # … else, if it is a late arrival, by the row already in the table …
                    .when(~supersedes_table, col("_c_eff"))
                    # … else it is the newest version known and stays open.
                    .otherwise(lit(None).cast("timestamp")))
        .withColumn("is_current", col("_next_ver").isNull() & supersedes_table)
        .withColumn("action", lit("new"))
        .select(key, version, eff_from, "effective_to", "is_current", "action", *attrs))

    # Close the row ALREADY IN THE TABLE, once, at the effective_from of the earliest batch
    # version that supersedes it. Rebuilt from `current` so its attributes survive.
    close_rows = (b.filter(col("_c_ver").isNotNull() & (col(version) > col("_c_ver")))
        .withColumn("_rn", row_number().over(w_key))
        .filter(col("_rn") == 1)
        .withColumn("action", lit("close"))
        .select(col(key),
                col("_c_ver").alias(version),
                col("_c_eff").alias(eff_from),
                col(eff_from).alias("effective_to"),
                lit(False).alias("is_current"), col("action"),
                *[col(f"_c_{a}").cast(batch.schema[a].dataType).alias(a) for a in attrs]))

    return new_rows.unionByName(close_rows)
