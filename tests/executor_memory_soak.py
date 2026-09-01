"""Does a pipeline executor fit in the memory its pod actually gets?

WHY THIS EXISTS: on the AWS run, delta-bronze died with ExecutorDeadException and
iceberg-silver with MetadataFetchFailedException. Both are the same event seen from
different sides -- the ONE executor (instances: 1 on every Spark app) went away, and
with it the shuffle output the next stage wanted. Neither says WHY it went away, and
nothing in the diagnostics bundle recorded whether the container was OOMKilled or the
node was taken from under it.

This tests the OOMKill half, and it does NOT need AWS, because the thing to reproduce is
the CGROUP LIMIT, not the data volume: run the same work in a container capped at the
same bytes the pod is capped at. Run it under `docker run --memory=<pod limit>` and let
the kernel answer. Exit 137 is the finding.

Two modes, one per suspect:

  state   iceberg-silver-trades. .withWatermark("event_ts", "2 hours")
          .dropDuplicatesWithinWatermark(["trade_id"]) on the RocksDB state store.
          RocksDB is a NATIVE library: its block cache, memtables and SSTs are off-heap,
          so they are charged to the container and are invisible to every JVM heap
          setting. The manifests set RocksDBStateStoreProvider and size the executor at
          2g with NO memoryOverhead anywhere in infra/aws/k8s -- so whatever RocksDB
          takes comes out of the overhead Spark guessed for it.
          Event time is SYNTHETIC and advances at 1/RATE of a second per row, so the
          2-hour window fills in row count without waiting two hours: 7.2M distinct
          trade_ids at the manifest's 1000/s is ~36s of wall clock here.

  batch   delta-bronze-trades. maxOffsetsPerTrigger=200000 on a 10s trigger, through
          PySpark's from_json in the same 2g. PySpark workers are separate PROCESSES,
          so their memory is also off-heap and also uncharged.

  MEM_MODE     "state" (default) or "batch"
  MEM_ROWS     distinct keys to drive through   (default 8_000_000, just over a 2h window)
  MEM_RATE     synthetic events per event-second (default 1000, the manifest's rate)
  MEM_HEAP     JVM heap, matching the manifests  (default 2g)
  MEM_BATCH    rows per micro-batch in batch mode (default 200000, the manifest's value)

The container limit is NOT set here -- it is set by whoever runs the container, because
that is the variable under test. See tests/run-checks.sh for the pod-faithful figure.
"""
import os, sys, time

MODE  = os.environ.get("MEM_MODE", "state")
ROWS  = int(os.environ.get("MEM_ROWS", "8000000"))
RATE  = int(os.environ.get("MEM_RATE", "1000"))
HEAP  = os.environ.get("MEM_HEAP", "2g")
BATCH = int(os.environ.get("MEM_BATCH", "200000"))

from pyspark.sql import SparkSession

b = (SparkSession.builder.appName(f"executor-memory-soak-{MODE}").master("local[2]")
     .config("spark.driver.memory", HEAP)
     .config("spark.sql.shuffle.partitions", "4")
     # The manifests' state store, and the reason this test exists.
     .config("spark.sql.streaming.stateStore.providerClass",
             "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider"))
spark = b.getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

rt = spark.sparkContext._jvm.java.lang.Runtime.getRuntime()
print(f"mode={MODE} rows={ROWS} heap_max={rt.maxMemory() // (1 << 20)}MiB "
      f"(asked {HEAP})", flush=True)

CHK = f"/tmp/mem_soak_{MODE}/_chk"
os.system(f"rm -rf /tmp/mem_soak_{MODE}")

# One row per trade_id, event time advancing at 1/RATE s per row -- so `rows` rows span
# rows/RATE seconds of EVENT time regardless of how fast they are actually produced.
src = (spark.readStream.format("rate")
       .option("rowsPerSecond", str(max(BATCH, 50000)))
       .option("rampUpTime", "0s").load()
       .selectExpr(
           "value AS trade_id",
           f"timestampadd(MICROSECOND, cast(value * {1_000_000 // RATE} as bigint),"
           f" timestamp '2026-01-01 00:00:00') AS event_ts",
           "value % 500 AS account_id",
           "cast(value % 97 as string) AS symbol"))

if MODE == "state":
    q = (src.withWatermark("event_ts", "2 hours")
            .dropDuplicatesWithinWatermark(["trade_id"])
            .writeStream.format("noop").outputMode("append")
            .option("checkpointLocation", CHK)
            .trigger(processingTime="1 second").start())
else:
    q = (src.writeStream.format("noop").outputMode("append")
            .option("checkpointLocation", CHK)
            .trigger(processingTime="1 second").start())

def _read(*paths):
    for p_ in paths:
        try:
            v = open(p_).read().strip()
            return None if v in ("max", "-1") else int(v)
        except Exception:
            continue
    return None


def _container_mib():
    """What the CGROUP thinks we are using -- the number the OOM killer reads. The JVM
    heap, RocksDB's native allocations and every PySpark worker process are all in here;
    a JVM-side reading would miss the two that matter."""
    v = _read("/sys/fs/cgroup/memory.current",
              "/sys/fs/cgroup/memory/memory.usage_in_bytes")
    return v // (1 << 20) if v else -1


def _container_limit_mib():
    v = _read("/sys/fs/cgroup/memory.max",
              "/sys/fs/cgroup/memory/memory.limit_in_bytes")
    return v // (1 << 20) if v else "unlimited"


seen = 0
t0 = time.time()
while q.isActive and seen < ROWS:
    q.awaitTermination(3)
    ps = q.recentProgress
    if ps:
        seen = sum(p.numInputRows for p in ps) or seen
        last = ps[-1]
        rss = _container_mib()
        st = (last.stateOperators[0] if last.stateOperators else None)
        rows_in_state = st.numRowsTotal if st else 0
        mem_state = (st.memoryUsedBytes // (1 << 20)) if st else 0
        print(f"  t={time.time()-t0:6.0f}s  input={seen:>10,}  "
              f"state_rows={rows_in_state:>10,}  state_mem={mem_state:>6,}MiB  "
              f"container={rss:>5,}/{_container_limit_mib()}MiB", flush=True)
q.stop()
print(f"OK: survived {seen:,} rows in mode={MODE}, no OOMKill at this container limit",
      flush=True)
