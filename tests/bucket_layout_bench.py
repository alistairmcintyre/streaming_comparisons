"""Does bucket(16, account_id) on silver.accounts actually help? Measured answer: no.

RESULT, real S3, 1000 accounts and 36,000 versions built by 120 micro-batches:

                           files   lookup     MERGE
    no layout               120    1384ms    5692ms
    bucket(16, account_id) 1920   15159ms   28825ms
    no layout (warm)        120    1567ms    4737ms

Bucketing is 11x worse on the lookup and 5x worse on the MERGE, and the mechanism is the
file count. A bucketed append writes one file per bucket it touches, so 120 micro-batches
leave 1920 files instead of 120, and the pruning that is meant to pay for that never
arrives: a 75-key batch hashes across all 16 buckets, so every bucket is read anyway.

WHY THIS FILE EXISTS RATHER THAN JUST THE ANSWER. The first version of this bench said
bucketing was 2x slower, and that number was an artefact of the bench. It wrote all 36,000
rows in ONE append, so the unbucketed table was a single large file and the bucketed one
was sixteen. The real table is written by a micro-batch every 15 seconds for two hours, so
the unbucketed table is hundreds of small files, which is the exact situation bucketing is
supposed to fix. The old bench handed the unbucketed table a compaction that production
never performs, and then reported the result of that gift. Both versions reached "bucketing
loses" and only one of them was measuring the thing.

So this builds both tables the way the stream does, one append per micro-batch, and prints
the file count so the mechanism is visible rather than assumed.

Scale: the accounts generator runs at 5 evt/s (docker-compose ACCOUNTS_EVENTS_PER_SEC), so
a 120 minute run is ~36,000 versions over 1000 accounts arriving 75 at a time on a 15
second trigger. APPENDS is how many of those 480 micro-batches to actually perform. Fewer
than 480 understates the unbucketed table's file count, which biases the result AGAINST
bucketing, so the margin above is a floor.

Order bias is controlled for: plain is benched twice, before and after the bucketed table,
and the two plain lines agree.

    . scripts/lake-aws-env.sh
    docker run --rm -u 0 -v "$PWD:/w" -w /w \
      -e BENCH_WAREHOUSE="$ICEBERG_WAREHOUSE/bucket_layout_bench" \
      -e S3A_ENDPOINT -e S3A_PATH_STYLE -e S3A_SSL -e S3A_CREDS_PROVIDER \
      -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e AWS_SESSION_TOKEN -e AWS_REGION \
      --entrypoint /opt/spark/bin/spark-submit "$ECR/spark-iceberg:latest" \
      --master 'local[1]' --driver-memory 3g /w/tests/bucket_layout_bench.py

Takes about 20 minutes against real S3, most of it building the two tables.
"""
import os, sys, time
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast, col

WH       = os.environ["BENCH_WAREHOUSE"]
ACCOUNTS = int(os.environ.get("BENCH_ACCOUNTS", "1000"))
ROWS     = int(os.environ.get("BENCH_ROWS", "36000"))     # 120 min at 5 evt/s
APPENDS  = int(os.environ.get("BENCH_APPENDS", "120"))    # micro-batches to simulate
PER_BATCH= int(os.environ.get("BENCH_PER_BATCH", "75"))   # 5 evt/s x 15s trigger
BUCKETS  = int(os.environ.get("BENCH_BUCKETS", "16"))
REPS     = int(os.environ.get("BENCH_REPS", "7"))

spark = (SparkSession.builder.appName("bucket-ab3").master("local[1]")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local.type", "hadoop")
    .config("spark.sql.catalog.local.warehouse", WH)
    .config("spark.sql.shuffle.partitions", "8")          # matches what the jobs now set
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
PROPS = ("'format-version'='2','write.delete.mode'='merge-on-read',"
         "'write.update.mode'='merge-on-read','write.merge.mode'='merge-on-read'")

def build(tbl, partition_clause):
    """One append per simulated micro-batch, which is the whole point of this rewrite."""
    spark.sql(f"DROP TABLE IF EXISTS local.db.{tbl}")
    spark.sql(f"CREATE TABLE local.db.{tbl} ({COLS}) USING iceberg {partition_clause} "
              f"TBLPROPERTIES ({PROPS})")
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
    files = spark.sql(f"SELECT count(*) c FROM local.db.{tbl}.files").collect()[0]["c"]
    print(f"  built {tbl:<14} {ROWS} rows in {APPENDS} appends, "
          f"{files} data files, {time.time()-t0:.0f}s", flush=True)
    return files

def batch(seed):
    return spark.range(0, PER_BATCH).selectExpr(
        f"cast(pmod(hash(id + {seed}), {ACCOUNTS}) + 1 as bigint) as account_id").distinct()

def bench(tbl, label, files):
    def lookup(seed):
        keys = batch(seed)
        (spark.table(f"local.db.{tbl}").filter(col("is_current"))
             .join(broadcast(keys), "account_id", "left_semi").count())
    def merge(seed):
        # Full SCD2 shape: close the current row out AND insert the new version. The old
        # bench did the close-out only, which leaves out the half that grows the table.
        batch(seed).createOrReplaceTempView("_k")
        spark.sql(f"""MERGE INTO local.db.{tbl} t USING (
            SELECT account_id, cast({9000+seed} as bigint) source_lsn FROM _k) s
          ON t.account_id = s.account_id AND t.is_current
          WHEN MATCHED THEN UPDATE SET t.is_current = false, t.source_lsn = s.source_lsn""")
    out = []
    for name, fn in (("lookup", lookup), ("MERGE", merge)):
        fn(0)                                   # warm, not measured
        ts = []
        for i in range(1, REPS + 1):
            t0 = time.time(); fn(i); ts.append((time.time() - t0) * 1000)
        ts.sort()
        out.append(f"{name}={ts[len(ts)//2]:.0f}ms")
    print(f"  {label:<26} files={files:<5} " + "  ".join(out), flush=True)

print(f"### iceberg silver.accounts on {WH}", flush=True)
print(f"### {ACCOUNTS} accounts, {ROWS} rows, {APPENDS} appends, "
      f"{PER_BATCH}-key batches, median of {REPS}", flush=True)
fp = build("acct_plain", "")
fb = build("acct_bucketed", f"PARTITIONED BY (bucket({BUCKETS}, account_id))")
bench("acct_plain",    "no layout (1st)", fp)
bench("acct_bucketed", f"bucket({BUCKETS}, account_id)", fb)
bench("acct_plain",    "no layout (2nd, warm)", fp)
