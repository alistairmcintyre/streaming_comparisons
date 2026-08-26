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
  OFF=$($KB -n kafka exec trades-dual-role-0 -- bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic app.public.trades 2>/dev/null | cut -d: -f3)
  [ "${OFF:-0}" -gt 0 ] && ok "app.public.trades offset=$OFF" || bad "app.public.trades has NO messages"
  DLQ=$($KB -n kafka exec trades-dual-role-0 -- bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic debezium-dlq 2>/dev/null | cut -d: -f3)
  [ -z "${DLQ:-}" ] || [ "${DLQ:-0}" = 0 ] && ok "DLQ empty" || bad "DLQ has $DLQ records — Debezium is REJECTING messages"
  # WIRE FORMAT: the bug that cost hours. Everything downstream silently produces
  # empty batches if `after` is not top-level or price is not an exact decimal.
  MSG=$($KB -n kafka exec trades-dual-role-0 -- bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
        --topic app.public.trades --max-messages 1 --timeout-ms 15000 2>/dev/null | tail -1)
  if [ -n "$MSG" ]; then
    echo "$MSG" | grep -q '"schema"' \
      && bad "wire format WRAPPED ({schema,payload}) — set value.converter.schemas.enable=false; consumers will parse NULL" \
      || ok "wire format unwrapped (after at top level)"
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
  [ "${R:-0}" -ge 3 ] && ok "fluss jobs RUNNING ($R: bronze+gold+tiering)" || bad "only ${R:-0}/3 fluss jobs RUNNING"
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
    [ -n "$OUT" ] && echo "        fluss bronze vertices: $OUT"
  done
fi
FD=$(aws s3 ls "s3://$PAIMON/fluss/" --recursive 2>/dev/null | grep -cE '\.(parquet|orc|avro)$')
[ "${FD:-0}" -gt 0 ] && ok "fluss tiered $FD data files" \
  || bad "fluss lake has NO data files — check whether Fluss ITSELF has rows (sink), vs tiering not moving them"

echo "== 5. flink — paimon =="
PJ=$($KB -n flink exec deploy/flink-paimon -- curl -s localhost:8081/jobs/overview 2>/dev/null | python3 -c "import json,sys;print(sum(1 for j in json.load(sys.stdin)['jobs'] if j['state']=='RUNNING'))" 2>/dev/null)
[ "${PJ:-0}" -ge 3 ] && ok "paimon jobs RUNNING ($PJ)" || bad "only ${PJ:-0} paimon jobs RUNNING"

echo "== 6. spark =="
SR=$($KB -n spark get sparkapplications -o jsonpath='{.items[*].status.applicationState.state}' 2>/dev/null | tr ' ' '\n' | grep -c RUNNING)
[ "${SR:-0}" -ge 12 ] && ok "$SR spark apps RUNNING" || bad "only ${SR:-0}/12 spark apps RUNNING (iceberg+delta+hudi = 4 each)"
ERR=$($KB -n spark get pods --no-headers 2>/dev/null | grep -c Error)
[ "${ERR:-0}" = 0 ] && ok "no spark Error pods" || bad "$ERR spark pod(s) in Error"
BEHIND=$($KB -n spark logs -l spark-role=driver --tail=200 --max-log-requests=10 2>/dev/null | grep -c 'falling behind')
[ "${BEHIND:-0}" = 0 ] && ok "no microbatch overruns" || warn "$BEHIND 'falling behind' warnings — batches exceed the trigger (executor sizing?)"

echo "== 7. data files in S3 (the actual point) =="
for spec in "delta:s3://$WAREHOUSE/delta/:parquet" "iceberg:s3://$WAREHOUSE/iceberg/:parquet" "hudi:s3://$WAREHOUSE/hudi/:parquet|log" "paimon:s3://$PAIMON/paimon/:parquet|orc|avro"; do
  n="${spec%%:*}"; rest="${spec#*:}"; path="${rest%:*}"; pat="${rest##*:}"
  C=$(aws s3 ls "$path" --recursive 2>/dev/null | grep -cE "\.($pat)$")
  [ "${C:-0}" -gt 0 ] && ok "$n has $C data files" || bad "$n has NO data files (running != writing)"
done

echo
echo "════ SUMMARY: $PASS pass, $FAIL fail, $WARN warn ════"
if [ "$FAIL" -gt 0 ]; then
  echo "FIX THESE TOGETHER (one pass, not one per run):"
  for p in "${PROBLEMS[@]}"; do echo "  - $p"; done
  exit 1
fi
echo "all stages healthy"
