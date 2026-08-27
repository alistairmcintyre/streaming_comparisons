#!/usr/bin/env bash
# Capture everything you would need to debug a failed run, BEFORE the cluster is
# destroyed.
#
# WHY: destroy_mode is now `always`, because leaving a failed run's cluster alive to
# debug cost two idle clusters in a single day. That trade is only safe if the evidence
# outlives the cluster. Every bug found on the first live run came from something that
# vanishes at teardown: the Fluss submitter log (the invalid latency_sink SQL), the
# operator log ("REST service in session cluster timed out"), the JM pod's declared
# ports (no 9249), and Prometheus's active-target list (no flink targets at all).
#
# Never fails the run: diagnostics are best-effort by definition, and a missing log
# must not mask the failure that prompted the collection.
set -uo pipefail
KB="${KUBECTL:-kubectl}"
OUT="${1:-diagnostics}"
mkdir -p "$OUT"
cap() { local f="$OUT/$1.txt"; shift; { echo "### $* ###"; timeout 90 "$@" 2>&1; echo; } >> "$f" || true; }

# ── cluster-wide shape ───────────────────────────────────────────────────────
cap cluster $KB get nodes -o wide
cap cluster $KB get pods -A -o wide
cap cluster $KB top nodes
cap cluster $KB get events -A --sort-by=.lastTimestamp

# Anything not Running/Completed, described in full — ReplicaSet-level errors (a missing
# serviceaccount, an unschedulable pod) appear ONLY here, never on the Deployment.
$KB get pods -A --no-headers 2>/dev/null | awk '$4!="Running" && $4!="Completed" {print $1" "$2}' \
| while read -r ns pod; do cap unhealthy $KB -n "$ns" describe pod "$pod"; done

# ── per-engine: the logs that actually explained the failures ────────────────
for spec in "flink:job/fluss-submitter" "flink:job/paimon-submitter" \
            "flink:deploy/fluss-flink" "flink:deploy/flink-paimon" \
            "flink:deploy/flink-kubernetes-operator" \
            "kafka:deploy/debezium-connect" "streaming:deploy/latency-exporter"; do
  ns="${spec%%:*}"; obj="${spec##*:}"
  cap "logs-${ns}-$(echo "$obj" | tr '/' '-')" $KB -n "$ns" logs "$obj" --tail=500
done
for d in $($KB -n spark get pods --no-headers 2>/dev/null | awk '/driver/{print $1}'); do
  cap "logs-spark-$d" $KB -n spark logs "$d" --tail=300
done

# ── the state that is invisible once the cluster is gone ────────────────────
cap flink $KB -n flink get flinkdeployment -o yaml
cap flink $KB -n flink get pods -l type=flink-native-kubernetes \
      -o jsonpath='{range .items[*]}{.metadata.name}{" ports="}{.spec.containers[*].ports[*].containerPort}{" labels="}{.metadata.labels}{"\n"}{end}'
cap spark $KB -n spark get sparkapplications -o wide
cap kafka $KB -n kafka get kafka,kafkatopic,kafkaconnect,kafkaconnector -o wide

# Prometheus: which targets exist, and are the alert rules actually loaded? Both were
# silently wrong on the first run. Via the apiserver proxy — the prometheus container
# is distroless and has no wget/curl, so `kubectl exec` cannot work here.
PX="/api/v1/namespaces/monitoring/services/kube-prometheus-stack-prometheus:9090/proxy/api/v1"
cap prometheus $KB get --raw "$PX/targets?state=active"
cap prometheus $KB get --raw "$PX/rules"
for m in kafka_consumergroup_lag flink_jobmanager_job_uptime pipeline_latency_events_total up; do
  cap prometheus $KB get --raw "$PX/query?query=$m"
done

echo "diagnostics written to $OUT/ ($(ls "$OUT" | wc -l) files)"
