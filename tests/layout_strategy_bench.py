"""Can silver.accounts avoid a full scan by SORTING rather than partitioning? No, and the
reason makes the whole question moot: the table is 539 KB.

RESULT, MinIO, 1000 accounts and 36,000 versions built by 120 appends:

                             files   avg_span     lookup    MERGE
    none (shipped)             132   419.3/1000    307ms    567ms
    hash (no partition col)    132   419.3/1000    159ms    486ms
    range + WRITE ORDERED BY   132   419.3/1000    145ms    470ms
    none + sorted compaction     1   999.0/1000    105ms    271ms

avg_span is the average per-file account_id bound spread out of a 1000-key space, read from
readable_metrics in the manifests. It is what pruning actually consults, so it matters more
than the timings.

THREE FINDINGS, in order of how much they change what you would do.

1. NEITHER WRITE-SIDE DISTRIBUTION MODE DOES ANYTHING. hash and range produce byte-identical
   layouts to none: same 132 files, same 419.3 span. hash was expected to be inert because it
   distributes by PARTITION columns and this table has none. range was expected to work,
   because it distributes by the SORT ORDER and therefore needs no partition column, and it
   did not, for a reason worth keeping: distribution only redistributes rows WITHIN a single
   write. Each micro-batch here is 300 rows and lands in about one file, so there is nothing
   to redistribute, and a file's min/max is set by which rows the batch held rather than by
   how they were ordered inside it. Clustering ACROSS commits is inherently a compaction
   concern, not a write-path one, on any engine.

2. THE TABLE IS TOO SMALL FOR DATA SKIPPING TO MEAN ANYTHING. Sorted compaction rewrote 128
   files into ONE, 539 KB in total, which is the entire SCD2 history of 1000 accounts. File
   statistics prune FILES; with one file there is nothing to skip, which is why avg_span rose
   to 999 rather than falling. Data skipping is a large-table technique and this is not a
   large table.

3. SO THE ONLY LEVER THAT MOVED WAS FILE COUNT, AND IT MOVED A LOT. 132 files to 1 took the
   MERGE from 567ms to 271ms. The gain is fewer objects to open, not pruning, and the
   existing 15-minute rewrite_data_files (binpack, no sort) already delivers it. Adding
   strategy => 'sort' would buy nothing here and cost a global sort.

CONCLUSION: the shipped configuration is correct, and now measured rather than assumed.
Leave silver.accounts unpartitioned, leave the write path alone, keep compacting. This also
closes the layout theory for the 47-51 second iceberg-silver-accounts batches for good: the
whole table is 539 KB and merges in well under a second at every layout tried.

WHAT THIS SAYS ABOUT DELTA, which is the only engine that always had a layout here: its
clusterBy("account_id") is equally moot at this size, and its real advantage is
optimizeWrite + autoCompact running INLINE, which holds the file count down continuously
instead of every 15 minutes. File count was the thing that mattered, so Delta was right by
accident about the mechanism and right on purpose about the outcome.

LIMITATIONS, stated because two of them flatter the baseline. MinIO rather than S3, so
absolute times are not comparable to tests/bucket_layout_bench.py. And account_id = id % 1000
over a contiguous 300-row append gives each file a contiguous key block, so the baseline
arrives semi-clustered at 419 where real CDC would deliver nearer 1000; that understates any
gain a layout change could show, which is the conservative direction for the hypothesis being
tested but means the spans are not production-faithful.

    # MinIO on a docker network, bucket `warehouse`, then:
    docker run --rm --network lbench -u 0 -v "$PWD:/w" -w /w \
      -e BENCH_WAREHOUSE=s3a://warehouse/layoutab -e S3A_ENDPOINT=http://minio:9000 \
      -e S3A_PATH_STYLE=true -e S3A_SSL=false \
      -e AWS_ACCESS_KEY_ID=minioadmin -e AWS_SECRET_ACCESS_KEY=minioadmin \
      --entrypoint /opt/spark/bin/spark-submit "$ECR/spark-iceberg:latest" \
      --master 'local[1]' --driver-memory 3g /w/tests/layout_strategy_bench.py

About 3 minutes. Not wired into run-checks.sh: it needs MinIO and it answers a question that
is now answered.
"""
import os, time
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast, col

WH       = os.environ["BENCH_WAREHOUSE"]
ACCOUNTS = int(os.environ.get("BENCH_ACCOUNTS", "1000"))
ROWS     = int(os.environ.get("BENCH_ROWS", "36000"))
APPENDS  = int(os.environ.get("BENCH_APPENDS", "120"))
PER_BATCH= int(os.environ.get("BENCH_PER_BATCH", "75"))
REPS     = int(os.environ.get("BENCH_REPS", "7"))

spark = (SparkSession.builder.appName("layout-bench").master("local[1]")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local.type", "hadoop")
    .config("spark.sql.catalog.local.warehouse", WH)
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.driver.memory", "3g")
    .config("spark.hadoop.fs.s3a.endpoint", os.environ.get("S3A_ENDPOINT", ""))
    .config("spark.hadoop.fs.s3a.path.style.access", os.environ.get("S3A_PATH_STYLE", "true"))
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", os.environ.get("S3A_SSL", "false"))
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            os.environ.get("S3A_CREDS_PROVIDER", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"))
    .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

COLS = ("account_id BIGINT, source_lsn BIGINT, name STRING, country STRING, tier STRING, "
        "source_updated_at TIMESTAMP, event_ts TIMESTAMP, effective_from TIMESTAMP, "
        "effective_to TIMESTAMP, is_current BOOLEAN, op STRING, commit_ts TIMESTAMP")
MOR = ("'format-version'='2','write.delete.mode'='merge-on-read',"
       "'write.update.mode'='merge-on-read','write.merge.mode'='merge-on-read'")

def build(tbl, dist, ordered):
    spark.sql(f"DROP TABLE IF EXISTS local.db.{tbl}")
    spark.sql(f"CREATE TABLE local.db.{tbl} ({COLS}) USING iceberg "
              f"TBLPROPERTIES ({MOR}, 'write.distribution-mode'='{dist}')")
    if ordered:
        # Sets the table sort order. Iceberg then orders each write by it, which is what
        # makes range distribution meaningful on an unpartitioned table.
        spark.sql(f"ALTER TABLE local.db.{tbl} WRITE ORDERED BY account_id")
    per = ROWS // APPENDS
    t0 = time.time()
    for a in range(APPENDS):
        lo = a * per
        (spark.range(lo, lo + per).selectExpr(
            f"cast(id % {ACCOUNTS} + 1 as bigint) as account_id",
            f"cast(id / {ACCOUNTS} as bigint) as source_lsn",
            "concat('acct-', cast(id % 1000 as string)) as name",
            "'GB' as country", "'gold' as tier",
            "timestamp'2026-01-01' as source_updated_at", "timestamp'2026-01-01' as event_ts",
            "timestamp'2026-01-01' as effective_from", "cast(null as timestamp) as effective_to",
            f"(id >= {ROWS - ACCOUNTS}) as is_current",
            "'u' as op", "current_timestamp() as commit_ts")
         .writeTo(f"local.db.{tbl}").append())
    return time.time() - t0

def stats(tbl):
    """Files, and the average per-file account_id bound span. readable_metrics decodes the
    manifest lower/upper bounds, which is the thing pruning actually reads."""
    r = spark.sql(f"""
        SELECT count(*) AS files,
               avg(readable_metrics.account_id.upper_bound
                 - readable_metrics.account_id.lower_bound) AS avg_span
        FROM local.db.{tbl}.files""").collect()[0]
    return r["files"], (r["avg_span"] or 0)

def bench(tbl, label, build_s):
    def keys(seed):
        return spark.range(0, PER_BATCH).selectExpr(
            f"cast(pmod(hash(id + {seed}), {ACCOUNTS}) + 1 as bigint) as account_id").distinct()
    def lookup(seed):
        (spark.table(f"local.db.{tbl}").filter(col("is_current"))
             .join(broadcast(keys(seed)), "account_id", "left_semi").count())
    def merge(seed):
        keys(seed).createOrReplaceTempView("_k")
        spark.sql(f"""MERGE INTO local.db.{tbl} t USING (
            SELECT account_id, cast({9000+seed} as bigint) source_lsn FROM _k) s
          ON t.account_id = s.account_id AND t.is_current
          WHEN MATCHED THEN UPDATE SET t.is_current = false, t.source_lsn = s.source_lsn""")
    out = []
    for name, fn in (("lookup", lookup), ("MERGE", merge)):
        fn(0)
        ts = []
        for i in range(1, REPS + 1):
            t0 = time.time(); fn(i); ts.append((time.time() - t0) * 1000)
        ts.sort(); out.append(f"{name}={ts[len(ts)//2]:>6.0f}ms")
    f, span = stats(tbl)
    print(f"  {label:<26} files={f:<5} avg_span={span:>7.1f}/{ACCOUNTS}  "
          f"build={build_s:>5.0f}s  " + "  ".join(out), flush=True)

print(f"### iceberg silver.accounts layout, {WH}", flush=True)
print(f"### {ACCOUNTS} accounts, {ROWS} rows, {APPENDS} appends, {PER_BATCH}-key batches, "
      f"median of {REPS}. avg_span is the per-file account_id bound spread: lower prunes.",
      flush=True)

b = build("acct_none", "none", False);  bench("acct_none",  "none (shipped)", b)
b = build("acct_hash", "hash", False);  bench("acct_hash",  "hash (no partition col)", b)
b = build("acct_rng",  "range", True);  bench("acct_rng",   "range + WRITE ORDERED BY", b)

# Compaction-side fix on the shipped write path: sort instead of binpack.
t0 = time.time()
spark.sql("""CALL local.system.rewrite_data_files(
    table => 'db.acct_none', strategy => 'sort',
    sort_order => 'account_id ASC NULLS LAST',
    options => map('min-input-files','2','target-file-size-bytes','134217728'))""").show(truncate=False)
bench("acct_none", "none + sorted compaction", time.time() - t0)
