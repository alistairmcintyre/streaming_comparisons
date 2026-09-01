#!/usr/bin/env bash
# Capture everything you would need to debug a failed run, BEFORE the cluster is
# destroyed.
#
# Why: destroy_mode is now `always`, because leaving a failed run's cluster alive to
# debug cost two idle clusters in a single day. That trade is only safe if the evidence
# outlives the cluster. Every bug found on the first live run came from something that
# vanishes at teardown: the Fluss submitter log (the invalid latency_sink SQL), the
# operator log ("REST service in session cluster timed out"), the JM pod's declared
# ports (no 9249), and Prometheus's active-target list (no flink targets at all).
#
# Never fails the run: diagnostics are best-effort by definition, and a missing log
# must not mask the failure that prompted the collection.
set -uo pipefail
# --request-timeout so a call against something that is not answering fails in seconds
# instead of sitting on the `timeout` backstop. Without it this script took NINE MINUTES
# after a failed apply: the six Prometheus proxy calls below each hung for the full 90s
# (6 x 90 = 540s) because nothing had been applied for Prometheus to scrape or answer
# with. Diagnostics run on every failure, so their cost is paid exactly when a run has
# already gone wrong and the cluster is still billing.
KB="${KUBECTL:-kubectl} --request-timeout=25s"
OUT="${1:-diagnostics}"
mkdir -p "$OUT"
# timeout is now a backstop for something --request-timeout cannot bound, not the normal
# exit path.
cap() { local f="$OUT/$1.txt"; shift; { echo "### $* ###"; timeout 45 "$@" 2>&1; echo; } >> "$f" || true; }

# ── cluster-wide shape ───────────────────────────────────────────────────────
cap cluster $KB get nodes -o wide
cap cluster $KB get pods -A -o wide
cap cluster $KB top nodes
cap cluster $KB get events -A --sort-by=.lastTimestamp

# Anything not Running/Completed, described in full. ReplicaSet-level errors (a missing
# serviceaccount, an unschedulable pod) appear only here, never on the Deployment.
$KB get pods -A --no-headers 2>/dev/null | awk '$4!="Running" && $4!="Completed" {print $1" "$2}' \
| while read -r ns pod; do cap unhealthy $KB -n "$ns" describe pod "$pod"; done

# ── per-engine: the logs that actually explained the failures ────────────────
for spec in "flink:job/fluss-submitter" "flink:job/paimon-submitter" \
            "flink:deploy/fluss-flink" "flink:deploy/flink-paimon" \
            "flink:deploy/flink-kubernetes-operator" \
            "streaming:deploy/latency-exporter"; do
  ns="${spec%%:*}"; obj="${spec##*:}"
  cap "logs-${ns}-$(echo "$obj" | tr '/' '-')" $KB -n "$ns" logs "$obj" --tail=2000
done
# Debezium is a StrimziPodSet, not a Deployment: Strimzi creates the pod
# `debezium-connect-0` and no deploy/debezium-connect exists, so the entry above captured
# nothing for the whole CDC layer and said so only inside the file
#   error from server (NotFound): deployments.apps "debezium-connect" not found
# A label selector survives the rename and the pod count.
cap logs-kafka-debezium-connect $KB -n kafka logs -l strimzi.io/cluster=debezium --tail=2000 --max-log-requests=6

for d in $($KB -n spark get pods --no-headers 2>/dev/null | awk '/driver/{print $1}'); do
  cap "logs-spark-$d" $KB -n spark logs "$d" --tail=2000
done

# ── the state that is invisible once the cluster is gone ────────────────────
cap flink $KB -n flink get flinkdeployment -o yaml
cap flink $KB -n flink get pods -l type=flink-native-kubernetes \
      -o jsonpath='{range .items[*]}{.metadata.name}{" ports="}{.spec.containers[*].ports[*].containerPort}{" labels="}{.metadata.labels}{"\n"}{end}'
cap spark $KB -n spark get sparkapplications -o wide

# ── WHY AN EXECUTOR WENT AWAY ────────────────────────────────────────────────
# delta-bronze died with ExecutorDeadException and iceberg-silver with
# MetadataFetchFailedException on the same run. Both mean "the executor is gone"; neither
# says why, and this bundle could not tell you, because the two causes look identical
# from the driver and NOTHING here recorded either of them:
#
#   OOMKilled          the container exceeded its cgroup limit -> exit 137. Visible only
#                      on the pod, and only while the pod still exists. (Measured against
#                      the real pod limit by tests/executor_memory_soak.py, which did not
#                      reproduce it: 8.4M keys of RocksDB state peaked at 1451/2867MiB.
#                      So expect this to be empty, and if it is not, that measurement is
#                      wrong and this file is the evidence.)
#   node disruption    Karpenter consolidated or spot reclaimed the node underneath it.
#                      Visible only in the Karpenter log and the NodeClaim lifecycle.
#
# Every Spark app runs `instances: 1`, so either one kills the query outright.
cap executors $KB -n spark get pods -l spark-role=executor -o wide
# The corpse, not the summary: Last State / Reason / Exit Code live here. Requires
# spark.kubernetes.executor.deleteOnTermination=false in the manifests, or Spark deletes
# the evidence the moment the executor dies.
for p in $($KB -n spark get pods -l spark-role=executor --no-headers 2>/dev/null | awk '{print $1}'); do
  cap executors $KB -n spark describe pod "$p"
done
cap executors $KB -n spark get events --field-selector reason=Evicted,reason=Killing,reason=OOMKilling

# Karpenter's own account of every node it took away, and the NodeClaims that outlive
# the nodes. `disrupting via consolidation` and `interrupted by spot` both appear here
# and nowhere else.
cap nodes $KB -n kube-system logs -l app.kubernetes.io/name=karpenter --tail=3000 --max-log-requests=6
cap nodes $KB get nodeclaims -o wide
cap nodes $KB get nodes -o custom-columns='NAME:.metadata.name,CREATED:.metadata.creationTimestamp,CAPACITY:.metadata.labels.karpenter\.sh/capacity-type,TYPE:.metadata.labels.node\.kubernetes\.io/instance-type,UNSCHEDULABLE:.spec.unschedulable'
cap nodes $KB get events -A --field-selector reason=NodeNotReady,reason=DisruptionBlocked,reason=Unconsolidatable,reason=SpotInterrupted
cap kafka $KB -n kafka get kafka,kafkatopic,kafkaconnect,kafkaconnector -o wide

# ── Hudi timeline state ──────────────────────────────────────────────────────
# A Hudi table wedges by accumulating inflight/requested instants: every failed commit
# leaves one, the next attempt tries to roll it back, that fails, and it leaves another.
# gold/open_positions reached 18 of them on a live run while the driver reported only a
# credentials error, so nothing in this bundle said the table itself was stuck. The
# counts below make that obvious at a glance rather than forty minutes in.
{
  echo "### hudi timeline: pending instants per table ###"
  for t in bronze/trades silver/trades silver/accounts gold/open_positions; do
    n=$(aws s3 ls "s3://${WAREHOUSE_BUCKET}/hudi/$t/.hoodie/" 2>/dev/null \
        | grep -cE "inflight|requested" || echo "?")
    last=$(aws s3 ls "s3://${WAREHOUSE_BUCKET}/hudi/$t/.hoodie/" 2>/dev/null \
        | grep -E "\\.(delta)?commit$" | tail -1 | awk '{print $1" "$2" "$4}')
    printf "  %-22s pending=%-4s last_commit=%s\n" "$t" "$n" "${last:-none}"
  done
} > "$OUT/hudi-timeline.txt" 2>&1

# Prometheus: which targets exist, and are the alert rules actually loaded? Both were
# silently wrong on the first run. Via the apiserver proxy, the prometheus container
# is distroless and has no wget/curl, so `kubectl exec` cannot work here.
PX="/api/v1/namespaces/monitoring/services/kube-prometheus-stack-prometheus:9090/proxy/api/v1"
# PROBE ONCE. If Prometheus is not answering, which it will not be when the run died
# before the manifests applied, the six calls below have nothing to say and used to cost
# 90 seconds each. One cheap request decides whether to make them at all.
if $KB get --raw "$PX/status/buildinfo" >/dev/null 2>&1; then
  cap prometheus $KB get --raw "$PX/targets?state=active"
  cap prometheus $KB get --raw "$PX/rules"
  for m in kafka_consumergroup_lag flink_jobmanager_job_uptime pipeline_latency_events_total up; do
    cap prometheus $KB get --raw "$PX/query?query=$m"
  done
else
  echo "prometheus not reachable, targets/rules/metrics not captured" | tee "$OUT/prometheus.txt"
fi

# ── the error MESSAGES, pulled out of the logs ───────────────────────────────
# A tail is not a diagnosis. A Spark analyzer failure emits a stack trace hundreds of
# frames deep, so a --tail window fills with `at org.apache.spark...` and the ONE LINE
# that says what went wrong scrolls out of the capture. That happened: hudi-silver-accounts
# died after exhausting its 10 retries and the bundle recorded only ColumnResolutionHelper
# frames, enough to know it failed, not enough to know why.
# So: tails are larger now, and every captured log is grepped for the message lines and
# collected here, which is the file to open first.
{
  echo "### error lines across every captured log ###"
  for f in "$OUT"/logs-*.txt; do
    [ -f "$f" ] || continue
    hits=$(grep -hoiE "(Caused by|[A-Za-z.]*(Exception|Error)):[^$]{0,200}|\[[A-Z_]{4,40}\][^$]{0,160}" "$f" 2>/dev/null \
           | grep -viE "^at |OriginalTryStackTrace" | sort -u | head -8)
    [ -n "$hits" ] && { echo; echo "-- $(basename "$f")"; echo "$hits" | sed 's/^/   /'; }
  done
} > "$OUT/errors.txt" 2>/dev/null

echo "diagnostics written to $OUT/ ($(ls "$OUT" | wc -l) files), start with errors.txt"
