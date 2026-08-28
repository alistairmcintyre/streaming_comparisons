#!/usr/bin/env bash
# Check EVERY pipeline stage in one pass and report ALL failures together.
#
# WHY: the debugging loop has been deploy -> find ONE bug -> fix -> tear down ->
# redeploy, at 30-60 min and real money per cycle, when most of the bugs were
# independent and could have been found in a single run. This never stops at the
# first failure: it reports everything that is broken so one fix pass can address
# the lot.
#
# Run it ~5 minutes after the manifests are applied, then again 10 minutes later
# (the second run is what proves things are GROWING rather than merely present).
#
#   ./validate-run.sh              # full check
#   ./validate-run.sh --quick      # skip the growth samples
set -uo pipefail

KB="${KUBECTL:-kubectl}"
: "${AWS_PROFILE:=streaming-comparisons}"; export AWS_PROFILE
WAREHOUSE="${WAREHOUSE_BUCKET:-streaming-comparison-amc-warehouse}"
PAIMON="${PAIMON_BUCKET:-streaming-comparison-amc-paimon}"
QUICK=""; [ "${1:-}" = "--quick" ] && QUICK=1

PASS=0; FAIL=0; WARN=0
declare -a PROBLEMS
ok()   { printf '  \033[32mPASS\033[0m  %s\n' "$1"; PASS=$((PASS+1)); }
bad()  { printf '  \033[31mFAIL\033[0m  %s\n' "$1"; FAIL=$((FAIL+1)); PROBLEMS+=("$1"); }
warn() { printf '  \033[33mWARN\033[0m  %s\n' "$1"; WARN=$((WARN+1)); }

# kubectl exec has broken mid-run before (apiserver->kubelet proxy authorization);
# degrade to a clear WARN rather than reporting every downstream stage as failed.
EXEC_OK=1
$KB -n kafka get pods >/dev/null 2>&1 || { echo "cannot reach cluster"; exit 2; }
$KB -n streaming exec postgres-0 -- true >/dev/null 2>&1 || EXEC_OK=0
[ "$EXEC_OK" = 0 ] && warn "kubectl exec unavailable (apiserver->kubelet); in-pod checks skipped"

echo "== 1. nodes and pods =="
NOT_READY=$($KB get nodes --no-headers 2>/dev/null | grep -vc ' Ready ')
[ "$NOT_READY" = 0 ] && ok "all nodes Ready" || bad "$NOT_READY node(s) not Ready"
# only pods stuck >3min matter; fresh ContainerCreating is normal during scale-up
STUCK=$($KB get pods -A --no-headers 2>/dev/null | awk '$4!="Running"&&$4!="Completed"{print $6}' | grep -cE '^([0-9]+m|[0-9]+h)' || true)
[ "${STUCK:-0}" = 0 ] && ok "no pods stuck >3m" || bad "$STUCK pod(s) not Running for >3m"
PENDING=$($KB get pods -A --no-headers 2>/dev/null | grep -c Pending || true)
[ "${PENDING:-0}" = 0 ] && ok "nothing Pending" || warn "$PENDING Pending (capacity? check NodePool limits.cpu vs EC2 quota)"

echo "== 2. generator -> postgres =="
if [ "$EXEC_OK" = 1 ]; then
  A=$($KB -n streaming exec postgres-0 -- psql -U app -d appdb -tAc 'select count(*) from trades;' 2>/dev/null | tr -d ' ')
  ACC=$($KB -n streaming exec postgres-0 -- psql -U app -d appdb -tAc 'select count(*) from accounts;' 2>/dev/null | tr -d ' ')
  [ "${ACC:-0}" -gt 0 ] && ok "accounts seeded ($ACC)" || bad "accounts table EMPTY -> silver-accounts pipelines have no input"
  if [ -z "$QUICK" ]; then
    sleep 10; B=$($KB -n streaming exec postgres-0 -- psql -U app -d appdb -tAc 'select count(*) from trades;' 2>/dev/null | tr -d ' ')
    [ "${B:-0}" -gt "${A:-0}" ] && ok "trades growing ($A -> $B)" || bad "trades NOT growing (generator stalled?)"
  else
    [ "${A:-0}" -gt 0 ] && ok "trades present ($A)" || bad "trades table empty"
  fi
fi

echo "== 3. debezium -> kafka =="
CST=$($KB -n kafka get kafkaconnector pg-app -o jsonpath='{.status.connectorStatus.connector.state}' 2>/dev/null)
[ "$CST" = "RUNNING" ] && ok "connector pg-app RUNNING" || bad "connector pg-app state=${CST:-missing}"
if [ "$EXEC_OK" = 1 ]; then
  # SUM across partitions: app.public.trades is 6 partitions now (31-kafka-topics.yaml),
  # so kafka-get-offsets prints 6 lines. `cut` alone yields a multi-line value and
  # `[ ... -gt 0 ]` dies with "integer expression expected" — reporting a topic holding
  # 800k messages as empty.
  OFF=$($KB -n kafka exec trades-dual-role-0 -- bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic app.public.trades 2>/dev/null | awk -F: '{s+=$3} END {print s+0}')
  [ "${OFF:-0}" -gt 0 ] && ok "app.public.trades offset=$OFF" || bad "app.public.trades has NO messages"
  DLQ=$($KB -n kafka exec trades-dual-role-0 -- bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic debezium-dlq 2>/dev/null | awk -F: '{s+=$3} END {print s+0}')
  [ -z "${DLQ:-}" ] || [ "${DLQ:-0}" = 0 ] && ok "DLQ empty" || bad "DLQ has $DLQ records — Debezium is REJECTING messages"
  # WIRE FORMAT: the bug that cost hours. Everything downstream silently produces
  # empty batches if `after` is not top-level or price is not an exact decimal.
  MSG=$($KB -n kafka exec trades-dual-role-0 -- bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
        --topic app.public.trades --max-messages 1 --timeout-ms 15000 2>/dev/null | tail -1)
  if [ -n "$MSG" ]; then
    # Check the TOP-LEVEL keys, not a grep for "schema": Debezium's unwrapped envelope
    # contains source.schema="public" (the Postgres schema name), which made a naive
    # grep report every healthy run as WRAPPED.
    echo "$MSG" | python3 -c "
import json,sys
try:
    d=json.load(sys.stdin); k=set(d)
    if 'payload' in k and 'after' not in k: print('BAD wire format WRAPPED ({schema,payload}) - set value.converter.schemas.enable=false; consumers parse NULL')
    elif 'after' in k or 'before' in k:     print('OK wire format unwrapped (after at top level)')
    else:                                    print('BAD unexpected envelope, top-level keys: '+','.join(sorted(k))[:60])
except Exception as e: print('BAD could not parse message: '+str(e)[:50])
" 2>/dev/null | while read -r line; do
      case "$line" in OK*) ok "${line#OK }";; *) bad "${line#BAD }";; esac
    done
    echo "$MSG" | python3 -c "
import json,sys
try:
    p=json.load(sys.stdin).get('after') or {}
    v=p.get('price')
    if v is None: print('BAD price missing')
    elif isinstance(v,(int,float)): print('WARN price is a float -> lossy for money; want decimal.handling.mode=string')
    else:
        float(v); print('OK price exact-decimal string:',v)
except Exception as e: print('BAD could not parse:',str(e)[:60])
" 2>/dev/null | while read -r line; do
      case "$line" in OK*) ok "${line#OK }";; WARN*) warn "${line#WARN }";; *) bad "${line#BAD }";; esac
    done
  fi
fi

echo "== 4. flink — fluss =="
FJ=$($KB -n flink exec deploy/fluss-flink -- curl -s localhost:8081/jobs/overview 2>/dev/null)
if [ -n "$FJ" ]; then
  R=$(echo "$FJ" | python3 -c "import json,sys;print(sum(1 for j in json.load(sys.stdin)['jobs'] if j['state']=='RUNNING'))" 2>/dev/null)
  # 4 jobs: silver_trades (Kafka→Fluss, one STATEMENT SET with its latency sink),
  # silver_accounts (the dimension gold enriches from), gold_open_positions, tiering.
  [ "${R:-0}" -ge 4 ] && ok "fluss jobs RUNNING ($R: silver trades+accounts, gold, tiering)" || bad "only ${R:-0}/4 fluss jobs RUNNING"
  # Is data actually IN Fluss? Tiering having created the Paimon tables proves only
  # that it connected — not that Fluss holds rows to tier.
  echo "$FJ" | python3 -c "
import json,sys
for j in json.load(sys.stdin)['jobs']:
    if 'trades' in j['name'] and 'insert' in j['name']: print(j['jid'])
" 2>/dev/null | head -1 | while read -r jid; do
    OUT=$($KB -n flink exec deploy/fluss-flink -- curl -s "localhost:8081/jobs/$jid" 2>/dev/null | python3 -c "
import json,sys
d=json.load(sys.stdin)
for v in d.get('vertices',[]):
    m=v.get('metrics',{})
    if m.get('write-records',0) or m.get('read-records',0):
        print(v['name'][:28], 'in=',m.get('read-records'), 'out=',m.get('write-records'))
" 2>/dev/null)
    [ -n "$OUT" ] && echo "        fluss silver vertices: $OUT"
  done
fi
FD=$(aws s3 ls "s3://$PAIMON/fluss/" --recursive 2>/dev/null | grep -cE '\.(parquet|orc|avro)$')
[ "${FD:-0}" -gt 0 ] && ok "fluss tiered $FD data files" \
  || bad "fluss lake has NO data files — check whether Fluss ITSELF has rows (sink), vs tiering not moving them"

echo "== 5. flink — paimon =="
PJ=$($KB -n flink exec deploy/flink-paimon -- curl -s localhost:8081/jobs/overview 2>/dev/null | python3 -c "import json,sys;print(sum(1 for j in json.load(sys.stdin)['jobs'] if j['state']=='RUNNING'))" 2>/dev/null)
# 4 jobs: bronze_trades, silver_trades, silver_accounts, gold_open_positions.
[ "${PJ:-0}" -ge 4 ] && ok "paimon jobs RUNNING ($PJ)" || bad "only ${PJ:-0}/4 paimon jobs RUNNING"

echo "== 6. spark =="
SR=$($KB -n spark get sparkapplications -o jsonpath='{.items[*].status.applicationState.state}' 2>/dev/null | tr ' ' '\n' | grep -c RUNNING)
[ "${SR:-0}" -ge 12 ] && ok "$SR spark apps RUNNING" || bad "only ${SR:-0}/12 spark apps RUNNING (iceberg+delta+hudi = 4 each)"
ERR=$($KB -n spark get pods --no-headers 2>/dev/null | grep -c Error)
[ "${ERR:-0}" = 0 ] && ok "no spark Error pods" || bad "$ERR spark pod(s) in Error"
BEHIND=$($KB -n spark logs -l spark-role=driver --tail=200 --max-log-requests=10 2>/dev/null | grep -c 'falling behind')
[ "${BEHIND:-0}" = 0 ] && ok "no microbatch overruns" || warn "$BEHIND 'falling behind' warnings — batches exceed the trigger (executor sizing?)"

echo "== 6b. latency telemetry (feeds the Grafana dashboard) =="
if [ "$EXEC_OK" = 1 ]; then
  LAT=$($KB -n kafka exec trades-dual-role-0 -- bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic pipeline_latency 2>/dev/null | awk -F: '{s+=$3} END {print s+0}')
  [ "${LAT:-0}" -gt 0 ] && ok "pipeline_latency has $LAT events" \
    || bad "pipeline_latency EMPTY — no pipeline is emitting; the Grafana dashboard will read No data"
fi
EXP=$($KB -n streaming get deploy latency-exporter -o jsonpath='{.status.readyReplicas}' 2>/dev/null)
[ "${EXP:-0}" -ge 1 ] && ok "latency-exporter ready" \
  || bad "latency-exporter not ready — check the ReplicaSet events (a missing serviceaccount shows there, NOT on the Deployment)"
DASH=$($KB -n monitoring get configmap grafana-dashboard-pipeline-comparison --no-headers 2>/dev/null | wc -l)
[ "${DASH:-0}" -ge 1 ] && ok "grafana dashboard ConfigMap present" || bad "grafana dashboard ConfigMap missing"

echo "== 7. data files in S3 (the actual point) =="
for spec in "delta:s3://$WAREHOUSE/delta/:parquet" "iceberg:s3://$WAREHOUSE/iceberg/:parquet" "hudi:s3://$WAREHOUSE/hudi/:parquet|log" "paimon:s3://$PAIMON/paimon/:parquet|orc|avro"; do
  n="${spec%%:*}"; rest="${spec#*:}"; path="${rest%:*}"; pat="${rest##*:}"
  C=$(aws s3 ls "$path" --recursive 2>/dev/null | grep -cE "\.($pat)$")
  [ "${C:-0}" -gt 0 ] && ok "$n has $C data files" || bad "$n has NO data files (running != writing)"
done

# ── 8. things this run changed for the first time ────────────────────────────
# Every check below covers a change that has NEVER run live. A rule written against
# a metric that does not exist, or a topic that was never created, fails silently and
# looks exactly like health — which is the whole reason this script exists.
echo "== 8. first-run surfaces =="

# Topics are now DECLARED (auto.create is off). An undeclared topic no longer
# springs into existence — a producer or consumer just fails.
# NOTE these are k8s RESOURCE names. The Kafka topic is pipeline_latency but the
# resource must be pipeline-latency — '_' is illegal in an RFC 1123 name.
for t in app.public.trades app.public.accounts debezium-dlq pipeline-latency; do
  R=$($KB -n kafka get kafkatopic "$t" -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null)
  [ "$R" = "True" ] && ok "topic $t Ready" || bad "topic $t NOT Ready (auto-create is OFF, so nothing will create it)"
done
AC=$($KB -n kafka get kafka trades -o jsonpath='{.spec.kafka.config.auto\.create\.topics\.enable}' 2>/dev/null)
[ "$AC" = "false" ] && ok "kafka auto-create disabled" || warn "kafka auto.create.topics.enable=$AC (expected false)"

# The alert rules. A PrometheusRule with a bad expr is silently DROPPED by the
# operator, so presence of the object is not proof the rules loaded.
PR=$($KB -n monitoring get prometheusrule streaming-pipelines --no-headers 2>/dev/null | wc -l)
[ "${PR:-0}" -ge 1 ] && ok "PrometheusRule streaming-pipelines applied" \
  || bad "PrometheusRule missing — 96-alerts.yaml did not apply"
PROXY="/api/v1/namespaces/monitoring/services/kube-prometheus-stack-prometheus:9090/proxy/api/v1"
# The prometheus container is distroless — it has no wget/curl, so `kubectl exec` can
# never work here and an exec-based check fails identically whether or not the rules
# loaded. Query through the apiserver proxy instead.
# The apiserver proxy is the cheap path but it TIMED OUT for every metric on a real run,
# so every metric check degraded to WARN and the section gave no signal at all. Fall back
# to a port-forward, which works: verified against the same cluster the proxy refused.
PROM_PF=""
_prom_pf() {
  [ -n "$PROM_PF" ] && return 0
  $KB -n monitoring port-forward svc/kube-prometheus-stack-prometheus 19090:9090 >/dev/null 2>&1 &
  PROM_PF=$!
  for _ in $(seq 1 10); do
    curl -sf --max-time 2 localhost:19090/-/ready >/dev/null 2>&1 && return 0
    sleep 2
  done
  return 1
}
trap '[ -n "$PROM_PF" ] && kill "$PROM_PF" 2>/dev/null' EXIT
promq() {
  out=$(timeout 15 $KB get --raw "${PROXY}$1" 2>/dev/null)
  [ -n "$out" ] && { echo "$out"; return 0; }
  _prom_pf && curl -sf --max-time 20 "localhost:19090/api/v1$1" 2>/dev/null
}

RG=$(promq "/rules" | python3 -c "
import json,sys
try: d=json.load(sys.stdin)
except Exception: print(-1); raise SystemExit
g=[x for x in d['data']['groups'] if x['name'].startswith('streaming.')]
print(sum(len(x['rules']) for x in g))" 2>/dev/null)
case "${RG:--1}" in
  -1|"") warn "could not reach prometheus to verify rules loaded (apiserver proxy timed out)" ;;
  *) [ "$RG" -ge 11 ] && ok "$RG alert rules LOADED in prometheus" \
       || bad "only $RG/11 streaming.* rules loaded — a bad expr is dropped silently" ;;
esac

for m in kafka_consumergroup_lag flink_jobmanager_job_uptime pipeline_latency_events_total; do
  N=$(promq "/query?query=$m" | python3 -c "
import json,sys
try: print(len(json.load(sys.stdin)['data']['result']))
except Exception: print(-1)" 2>/dev/null)
  case "${N:--1}" in
    -1|"") warn "could not query metric $m (prometheus unreachable)" ;;
    0)     bad "metric $m ABSENT — alerts built on it will never fire and will look healthy" ;;
    *)     ok "metric $m present ($N series)" ;;
  esac
done

# EVERY pipeline must be REPORTING, not just the metric existing. `pipeline_latency_events_total
# present (N series)` above passes with 10 of 14, which is exactly what a real run looked
# like while the whole bronze->silver hop was unmeasured on four of five engines.
# The expected set is derived FROM THE CODE, the same inventory tests/run-checks.sh checks
# the PipelinesMissing threshold against, so it cannot drift from what actually emits.
# NOTE ON TIMING: the PipelinesMissing ALERT waits `for: 10m` to avoid flapping. This check
# has no such dwell — it reports the truth at the moment it runs. A pipeline that has not
# emitted yet is indistinguishable from one that never will, so run this a few minutes in
# and again later; bronze emits within a trigger or two, but gold cannot emit until silver
# has produced rows for it to fold.
REPO="$(cd "$(dirname "$0")/../../.." && pwd)"
WANT=$( { grep -rho 'attach_latency_listener(spark, "[a-z-]*"' "$REPO"/jobs/spark-*/*.py 2>/dev/null | sed 's/.*"\(.*\)"/\1/';
          grep -rho '"pipeline":"[a-z-]*"' "$REPO"/jobs/flink-*/*.sql 2>/dev/null | sed 's/.*:"\(.*\)"/\1/'; } | sort -u )
HAVE=$(promq "/query?query=pipeline_latency_events_total" | python3 -c "
import json,sys
try: r=json.load(sys.stdin)['data']['result']
except Exception: raise SystemExit
print('\n'.join(sorted({s['metric'].get('pipeline','') for s in r if s['metric'].get('pipeline')})))" 2>/dev/null)
if [ -z "$WANT" ]; then
  warn "could not read the pipeline inventory from $REPO/jobs — skipping the per-pipeline check"
elif [ -z "$HAVE" ]; then
  warn "prometheus returned no pipeline_latency series — cannot verify the $(echo "$WANT" | wc -l) pipelines"
else
  MISSING=$(comm -23 <(echo "$WANT") <(echo "$HAVE"))
  if [ -z "$MISSING" ]; then
    ok "all $(echo "$WANT" | wc -l | tr -d ' ') pipelines are reporting latency"
  else
    bad "pipelines NOT reporting: $(echo "$MISSING" | tr '\n' ' ')(have $(echo "$HAVE" | wc -l | tr -d ' ') of $(echo "$WANT" | wc -l | tr -d ' ') — early in a run they may still be starting)"
  fi
fi

# Gold table presence. NOT a schema check — this only proves the Delta log exists.
# Field parity across the five engines is checked OFFLINE and for every layer by
# tests/schema_parity_test.py (declared schemas) and tests/hudi_schema_test.py +
# tests/gold_fold_test.py (Hudi, measured, since it has no DDL to declare). Doing it here
# would mean querying five catalogs mid-run to learn something a $0 local check already
# knows. The comment used to claim this line verified the 9 columns; it never did.
GC=$(aws s3 ls "s3://$WAREHOUSE/delta/gold/open_positions/_delta_log/" 2>/dev/null | wc -l)
[ "${GC:-0}" -gt 0 ] && ok "delta gold table exists" || warn "delta gold table not created yet"

echo
echo "════ SUMMARY: $PASS pass, $FAIL fail, $WARN warn ════"
if [ "$FAIL" -gt 0 ]; then
  echo "FIX THESE TOGETHER (one pass, not one per run):"
  for p in "${PROBLEMS[@]}"; do echo "  - $p"; done
  exit 1
fi
echo "all stages healthy"
