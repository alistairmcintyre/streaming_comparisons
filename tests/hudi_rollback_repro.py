"""Reproduce Hudi's rollback failure on a table with an incomplete instant.

Needs real S3 and a table carrying an incomplete instant; point GOLD at a COPY, never at
a live table. A preserved one is kept at
  s3://<warehouse>/benchmarks/iter/repro-hudi-gold-credentials/gold_open_positions

WHAT THIS SHOWS. Rolling back an incomplete instant fails with

  RollbackHelperV1.getPathInfoUnderPartition -> HoodieHadoopStorage.listDirectEntries
    -> FileSystem.get(conf) with no URI -> the DEFAULT filesystem -> RawLocalFileSystem
    -> IllegalArgumentException: Wrong FS: s3a://..., expected: file:///

and it only runs during a ROLLBACK, which is why a healthy table never sees it.

WHAT DOES NOT FIX IT, all measured with this script:
  fs.s3a.impl.disable.cache=false          still fails, so the cache is not the cause
  spark.hadoop.fs.defaultFS=s3a://bucket   still fails, driver honours it, Hudi does not
  hoodie.rollback.using.markers=false      still fails
  hoodie.rollback.instant.was.failed=false still fails
  core-site.xml via HADOOP_CONF_DIR        still fails

Hudi appears to build a fresh Configuration on that path, so nothing set through Spark
reaches it. We pin hoodie.write.table.version=6 for Athena, which selects the V1 rollback
helper, so moving off it is not free.

RECOVERY, which does work and is what unwedged the live table: delete the .requested and
.inflight markers of the genuinely incomplete instants, then restart the job. Note Hudi
keeps those files beside COMPLETED instants too, so count only instants that have no
completed file, or you will overstate the damage badly.

PREVENTION: a table only wedges when a commit dies mid-flight, so the real guard is
whatever stops commits dying. See the s3a cache note in infra/aws/k8s/95-spark-hudi.yaml.

  DISABLE_CACHE=true|false   fs.s3a.impl.disable.cache
  DEFAULT_FS=<uri>           fs.defaultFS, empty to leave alone
  HUDI_OPTS=k=v,k=v          extra write options
"""
import os, sys
sys.path.insert(0, "jobs/_shared")
WH = os.environ["WAREHOUSE"]; GOLD = os.environ["GOLD"]
os.environ["HUDI_WAREHOUSE"] = WH
DC = os.environ.get("DISABLE_CACHE", "true")
DFS = os.environ.get("DEFAULT_FS", "")

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
import hudi_tables

b = (SparkSession.builder.appName("wrongfs").master("local[2]")
     .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
     .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
     .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
     .config("spark.hadoop.fs.s3a.aws.credentials.provider",
             "software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider")
     .config("spark.hadoop.fs.s3a.impl.disable.cache", DC)
     .config("spark.sql.shuffle.partitions", "4").config("spark.driver.memory", "3g"))
if DFS:
    b = b.config("spark.hadoop.fs.defaultFS", DFS)
spark = b.getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
print(f"### disable.cache={DC}  defaultFS={DFS or '<unset>'}", flush=True)

opts = dict(hudi_tables.gold_positions_opts())
opts["hoodie.datasource.meta.sync.enable"] = "false"
opts["hoodie.datasource.hive_sync.enable"] = "false"
for kv in os.environ.get("HUDI_OPTS", "").split(","):
    if "=" in kv:
        k, v = kv.split("=", 1); opts[k.strip()] = v.strip()
print("   extra opts:", os.environ.get("HUDI_OPTS", "<none>"), flush=True)

try:
    t = spark.read.format("hudi").load(GOLD)
    keep = [c for c in t.columns if not c.startswith("_hoodie_")]
    out = t.select(*keep).limit(50).withColumn("commit_ts", current_timestamp())
    print("   writing columns:", keep, flush=True)
    out.write.format("hudi").options(**opts).mode("append").save(GOLD)
    print("### RESULT: WROTE OK (no Wrong FS)", flush=True)
    sys.exit(0)
except Exception as e:
    m = str(e)
    tag = "WRONG_FS" if "Wrong FS" in m else type(e).__name__
    print(f"### RESULT: FAILED [{tag}]", flush=True)
    print("---- raw ----", flush=True)
    print(m[:2500], flush=True)
    sys.exit(1)
