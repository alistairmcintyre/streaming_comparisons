"""
Shared Hudi write options for the trades pipelines.

TABLE TYPE: MERGE_ON_READ everywhere, to match how the other engines are configured
— Iceberg gold uses write.merge.mode=merge-on-read, Delta uses deletion vectors, and
Paimon is LSM (merge-on-read by nature). Hudi's own default is COPY_ON_WRITE, which
would make it look slower on writes and faster on reads for configuration reasons
rather than engine ones. MOR is also the standard recommendation for streaming CDC
ingest specifically, which is this workload.

COMPACTION: inline, so the cost lands INSIDE the write path. Delta compacts inline
(autoCompact) and Paimon self-compacts in the writer; if Hudi deferred compaction to
an async job its write latency would look artificially good. Iceberg is the odd one
out — it has no in-writer compaction and needs a separate rewrite job — which is
recorded in SIZING.md as a known asymmetry.

READ SEMANTICS: a MOR table exposes _ro (read-optimised, base files only — STALE) and
_rt (real-time, merges the log files — CURRENT). Correctness checks must use _rt or
the snapshot query type; _ro looks fast and returns old data.
"""
import os

_BASE = os.environ.get("HUDI_WAREHOUSE", "s3a://warehouse/hudi")

BRONZE_TRADES   = f"{_BASE}/bronze/trades"
SILVER_TRADES   = f"{_BASE}/silver/trades"
SILVER_ACCOUNTS = f"{_BASE}/silver/accounts"
GOLD_POSITIONS  = f"{_BASE}/gold/open_positions"

# Applied to every table: MOR + inline compaction + parquet/zstd to match the others.
_COMMON = {
    "hoodie.datasource.write.table.type": "MERGE_ON_READ",
    "hoodie.compact.inline": "true",
    "hoodie.compact.inline.max.delta.commits": "5",
    "hoodie.parquet.compression.codec": "zstd",
    # Keep the timeline bounded on a long run; without this the .hoodie dir grows
    # without limit and commit listing slowly dominates write latency.
    "hoodie.clean.automatic": "true",
    # TIME-based retention, not commit-count. commits.retained=10 at a 15s trigger is
    # ~2.5 MINUTES of history — and both Hudi streaming consumers (silver reading
    # bronze, gold reading silver) read INCREMENTALLY from a start instant. Fall further
    # behind than the cleaner's window (a restart, a slow batch, a node replacement) and
    # that instant is gone: the read either fails or takes the
    # hoodie.datasource.read.incr.fallback.fulltablescan path — which re-reads the whole
    # table and DOUBLE-COUNTS it into the `+=` gold fold, silently. 24h of history means
    # a consumer can be down a full day and still resume incrementally.
    "hoodie.cleaner.policy": "KEEP_LATEST_BY_HOURS",
    "hoodie.cleaner.hours.retained": "24",
    "hoodie.datasource.write.hive_style_partitioning": "true",
    # ATTEMPTED Athena compatibility — CURRENTLY INEFFECTIVE, kept as intent.
    # Hudi 1.2.0 writes table version 9 with timeline layout 2 (the LSM timeline under
    # .hoodie/timeline/), which Athena's Hudi reader cannot parse: every query fails
    # with a bare HIVE_UNKNOWN_ERROR, _ro and _rt alike. This setting asks for the
    # legacy version 6 / flat timeline, and it IS reaching the driver (verified in the
    # mounted file) but Hudi ignores it and still writes v9. Left in place because it
    # documents the intent and may be honoured by a later Hudi; do NOT assume Athena
    # can read these tables — query Hudi via Spark. See DEPLOY_LOG #55.
    "hoodie.write.table.version": "6",
}


# Hudi registers ITSELF in Glue on every commit via AwsGlueCatalogSyncTool
# (hudi-aws-bundle). That is better than pointing Glue at a metadata file after the
# fact: there is no pointer to go stale, which is exactly what broke Paimon's Athena
# visibility. For a MERGE_ON_READ table the sync creates TWO Glue tables:
#   <table>_ro  read-optimised — base files only, STALE
#   <table>_rt  real-time      — merges log files, CURRENT   <- use this one
_SYNC_TOOL = "org.apache.hudi.aws.sync.AwsGlueCatalogSyncTool"


def _sync(database, table, partition_fields=""):
    o = {
        "hoodie.datasource.meta.sync.enable": "true",
        "hoodie.meta.sync.client.tool.class": _SYNC_TOOL,
        "hoodie.datasource.hive_sync.enable": "true",
        "hoodie.datasource.hive_sync.database": database,
        "hoodie.datasource.hive_sync.table": table,
        "hoodie.datasource.hive_sync.use_jdbc": "false",
        "hoodie.datasource.hive_sync.support_timestamp": "true",
        "hoodie.datasource.hive_sync.partition_fields": partition_fields,
    }
    o["hoodie.datasource.hive_sync.partition_extractor_class"] = (
        "org.apache.hudi.hive.MultiPartKeysValueExtractor" if partition_fields
        else "org.apache.hudi.hive.NonPartitionedExtractor")
    return o


def _opts(table_name, recordkey, precombine, partitionpath="", operation="upsert",
          extra=None):
    o = dict(_COMMON)
    o.update({
        "hoodie.table.name": table_name,
        "hoodie.datasource.write.recordkey.field": recordkey,
        "hoodie.datasource.write.precombine.field": precombine,
        "hoodie.datasource.write.partitionpath.field": partitionpath,
        "hoodie.datasource.write.operation": operation,
        # Streaming writes need a stable identifier so restarts resume cleanly.
        "hoodie.datasource.write.streaming.checkpoint.identifier": f"{table_name}_writer",
    })
    if not partitionpath:
        o["hoodie.datasource.write.keygenerator.class"] = \
            "org.apache.hudi.keygen.NonpartitionedKeyGenerator"
    if extra:
        o.update(extra)
    return o


# bronze: append-only fills. `insert` skips the dedup/index lookup an upsert would do
# — the correct choice for immutable executions and much cheaper at 1k+/s.
def bronze_trades_opts():
    return _opts("hudi_bronze_trades", "trade_id", "executed_at",
                 partitionpath="executed_date", operation="insert",
                 extra=_sync("bronze", "trades_hudi", "executed_date"))


# silver trades: same grain, deduplicated on trade_id — LAST-WINS.
# precombine is the ordering key: on a duplicate trade_id Hudi keeps the row with the
# GREATEST precombine value. It moves from executed_at to source_lsn because executed_at
# ties at millisecond granularity at 1k/s, and a tie means an arbitrary winner. LSN is a
# strict total order across the CDC stream and never ties.
# This engine was ALREADY last-wins while paimon/fluss were first-wins and delta/iceberg
# were first-wins within a bounded watermark window (now 2h) — three different answers to "which row survives".
def silver_trades_opts():
    return _opts("hudi_silver_trades", "trade_id", "source_lsn",
                 partitionpath="executed_date", operation="upsert",
                 extra=_sync("silver", "trades_hudi", "executed_date"))


# silver accounts: SCD2 — EVERY version retained, not just the latest.
# The record key is composite (account_id, source_lsn), so a genuine change writes a NEW
# row while an at-least-once CDC re-delivery — identical account_id AND source_lsn —
# collapses onto the existing one. That is the whole trick: upsert still deduplicates
# re-deliveries, but it no longer destroys history, because the version is part of the key.
# Contrast silver.trades, where the key is trade_id alone: a trade is an immutable event
# and has no versions.
# effective_to / is_current are MATERIALISED, closed by a staged row that carries the
# superseded version's full attributes — Hudi's upsert replaces the whole record, so a
# close row with nulled attributes would erase the history it exists to preserve
# (tests/scd2_hudi_upsert_test.py).
def silver_accounts_opts():
    return _opts("hudi_silver_accounts", "account_id,source_lsn", "source_lsn",
                 operation="upsert",
                 extra={**_sync("silver", "accounts_hudi"),
                        "hoodie.datasource.write.keygenerator.class":
                        "org.apache.hudi.keygen.ComplexKeyGenerator"})


# gold: net book per (account_id, symbol). Compound key, partitioned by symbol so the
# per-batch read of affected keys prunes instead of scanning the whole table.
def gold_positions_opts():
    return _opts("hudi_gold_open_positions", "account_id,symbol", "commit_ts",
                 partitionpath="symbol", operation="upsert",
                 extra={**_sync("gold", "open_positions_hudi", "symbol"),
                        "hoodie.datasource.write.keygenerator.class":
                        "org.apache.hudi.keygen.ComplexKeyGenerator"})
