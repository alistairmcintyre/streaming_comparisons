"""Every Spark app must set spark.sql.shuffle.partitions, and set it below Spark's default.

WHY THIS IS A CHECK RATHER THAN A ONE-OFF FIX. The default is 200, it is silent, and it is
wrong for this deployment: these executors run 1 to 2 cores, so 200 shuffle partitions
means up to 200 serialised tasks per shuffle stage over trivial data, and Spark DISABLES
adaptive execution for streaming queries so coalescePartitions never rescues it. Nothing
fails, nothing logs, the job is just slower than it looks like it should be. A new
SparkApplication added later inherits exactly that, and the only way anyone would notice
is by measuring again.

SIZE OF THE PRIZE, REVISED UPWARDS ONCE IT WAS MEASURED PROPERLY. On latency it is worth
about 3%: against real S3, 200 versus 4 was 12933 vs 12525 ms. That is what this was
originally shipped for and it undersold the change badly.

The real cost is MEMORY, because shuffle partitions are not only a shuffle setting here.
Spark creates one RocksDB state store instance PER SHUFFLE PARTITION per stateful
operator, and with boundedMemoryUsage on its default of false each instance allocates its
own memtables and block cache off-heap. Measured at the pod's real 2867MiB cgroup limit
with tests/executor_memory_soak.py, 5M rows through a 2h dedupe window:

    shuffle=8     40 SST files    peak 1248MiB
    shuffle=200  833 SST files    peak 1617MiB

20x the files and 30% more native memory from one number, on executors that were being
OOMKilled at that exact limit. Neither arm actually died, so this is a contributor rather
than a proven cause, but it is not tidiness.
"""
import glob
import sys

import yaml

SPARK_KINDS = ("SparkApplication", "ScheduledSparkApplication")
KEY = "spark.sql.shuffle.partitions"
SPARK_DEFAULT = 200

def main() -> int:
    problems, seen = [], 0
    for path in sorted(glob.glob("infra/aws/k8s/*.yaml")):
        for doc in yaml.safe_load_all(open(path)):
            if not isinstance(doc, dict) or doc.get("kind") not in SPARK_KINDS:
                continue
            name = doc.get("metadata", {}).get("name", "<unnamed>")
            spec = doc.get("spec", {})
            # A ScheduledSparkApplication nests the real spec under `template`.
            if doc["kind"] == "ScheduledSparkApplication":
                spec = spec.get("template", spec)
            seen += 1
            value = (spec.get("sparkConf") or {}).get(KEY)
            if value is None:
                problems.append(f"{name} ({path}): {KEY} is unset, so it inherits {SPARK_DEFAULT}")
            elif int(value) >= SPARK_DEFAULT:
                problems.append(f"{name} ({path}): {KEY}={value}, not below the {SPARK_DEFAULT} default")
    if not seen:
        print("no Spark applications found; the check would pass vacuously")
        return 1
    for p in problems:
        print(f"  {p}")
    print(f"{seen} spark apps checked, {len(problems)} problem(s)")
    return 1 if problems else 0

if __name__ == "__main__":
    sys.exit(main())
