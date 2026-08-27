#!/usr/bin/env bash
# Compile the Fluss SQL against a real Fluss cluster, locally, before it costs anything.
#
# Fluss SQL cannot be checked the way the Paimon SQL can: CREATE CATALOG needs a live
# coordinator, and the table options we set (table.merge-engine, table.log.local-ttl,
# table.datalake.auto-compaction) are validated SERVER-side by TableDescriptorValidation.
# So this brings up zookeeper + coordinator + tablet server on a throwaway docker
# network, points the datalake at a local filesystem, and submits the real SQL.
#
# It catches the class of bug that has hit this pipeline repeatedly: a table option that
# is accepted by the docs and rejected by the server, and SQL that plans to nothing.
# Asserts every job file produces a job — "some jobs submitted" is exactly what the bad
# runs looked like.
#
# IMAGE CAVEAT, stated because it decides what this proves: the deployment runs a
# SOURCE-BUILT Fluss (docker/fluss, main/FIP-27), not the released 0.9.1. Options that
# exist on main may not exist in a release. This uses the locally-built image when
# present and falls back to the release, and SAYS WHICH — a pass against the release is
# weaker evidence than a pass against the image you deploy.
set -uo pipefail
NET="fluss-sqlcheck-$$"
ZK="zk-$$" ; CO="co-$$" ; TS="ts-$$"
WORK=$(mktemp -d)
cleanup() {
  docker rm -f "$ZK" "$CO" "$TS" >/dev/null 2>&1
  docker network rm "$NET" >/dev/null 2>&1
  rm -rf "$WORK"
}
trap cleanup EXIT

SERVER_IMG=""
for cand in "${FLUSS_SERVER_IMAGE:-}" streaming-comparisons-fluss-coordinator:latest apache/fluss:0.9.1-incubating; do
  [ -z "$cand" ] && continue
  docker image inspect "$cand" >/dev/null 2>&1 && { SERVER_IMG="$cand"; break; }
done
[ -n "$SERVER_IMG" ] || { echo "  no Fluss server image available locally"; exit 1; }
FLINK_IMG="${FLUSS_FLINK_IMAGE:-167217327348.dkr.ecr.eu-west-1.amazonaws.com/streaming-comparison/fluss-flink:latest}"
docker image inspect "$FLINK_IMG" >/dev/null 2>&1 || FLINK_IMG=apache/fluss-quickstart-flink:1.20-0.9.1-incubating
echo "  server image: $SERVER_IMG"
echo "  flink  image: $FLINK_IMG"
case "$SERVER_IMG" in
  apache/fluss:*) echo "  NOTE: released image, not the source-built one the deployment uses — a pass here is weaker evidence" ;;
esac

docker network create "$NET" >/dev/null
docker run -d --name "$ZK" --network "$NET" --network-alias fluss-zookeeper zookeeper:3.9.2 >/dev/null

props() {   # $1 = role-specific extra lines
  cat <<EOF
zookeeper.address: fluss-zookeeper:2181
$1
remote.data.dir: /tmp/fluss-remote
datalake.format: paimon
datalake.paimon.metastore: filesystem
datalake.paimon.warehouse: /tmp/fluss-paimon
EOF
}
docker run -d --name "$CO" --network "$NET" --network-alias fluss-coordinator \
  -e FLUSS_PROPERTIES="$(props 'bind.listeners: INTERNAL://0.0.0.0:9124, CLIENT://0.0.0.0:9123
advertised.listeners: INTERNAL://fluss-coordinator:9124, CLIENT://fluss-coordinator:9123
internal.listener.name: INTERNAL')" "$SERVER_IMG" coordinatorServer >/dev/null
docker run -d --name "$TS" --network "$NET" --network-alias fluss-tablet-server \
  -e FLUSS_PROPERTIES="$(props 'bind.listeners: INTERNAL://0.0.0.0:9124, CLIENT://0.0.0.0:9123
advertised.listeners: INTERNAL://fluss-tablet-server:9124, CLIENT://fluss-tablet-server:9123
internal.listener.name: INTERNAL
tablet-server.id: 0
data.dir: /tmp/fluss/data
kv.snapshot.interval: 0s')" "$SERVER_IMG" tabletServer >/dev/null

echo "  waiting for the coordinator..."
for i in $(seq 1 45); do
  docker logs "$CO" 2>&1 | grep -qiE 'started|ready' && break
  docker ps -q --filter "name=$CO" | grep -q . || { echo "  coordinator died:"; docker logs "$CO" 2>&1 | tail -12 | sed 's/^/      /'; exit 1; }
  sleep 2
done
sleep 10   # tablet server registration

# Local stand-ins. Kafka is unreachable on purpose: creating a Kafka table does not
# connect, and the planner still validates its changelog mode.
export FLUSS_BOOTSTRAP="fluss-coordinator:9123"
export FLUSS_ICEBERG_OPTS=""
export KAFKA_BOOTSTRAP="localhost:9092"
export KAFKA_EXTRA_OPTS=""
SUBST='${FLUSS_BOOTSTRAP} ${FLUSS_ICEBERG_OPTS} ${KAFKA_BOOTSTRAP} ${KAFKA_EXTRA_OPTS}'
JOBS_DIR="${JOBS_DIR:-jobs/flink-fluss}"
mkdir -p "$WORK/sql"
for f in "$JOBS_DIR"/*.sql; do envsubst "$SUBST" < "$f" > "$WORK/sql/$(basename "$f")"; done

cat > "$WORK/run.sh" <<'INNER'
set -u
export FLINK_HOME=/opt/flink
"$FLINK_HOME/bin/start-cluster.sh" >/dev/null 2>&1
for i in $(seq 1 30); do curl -sf localhost:8081/overview >/dev/null 2>&1 && break; sleep 2; done
run_sql() { echo "=== $1 ==="; "$FLINK_HOME/bin/sql-client.sh" -f "/sql/$1" 2>&1; }
run_sql create_tables.sql
for f in silver_trades.sql silver_accounts.sql gold_open_positions.sql; do
  [ -f "/sql/$f" ] || continue
  echo "@@@BEGIN $f@@@"; run_sql "$f"; echo "@@@END $f@@@"
done
INNER

echo "== compiling $JOBS_DIR/*.sql =="
docker run --rm --network "$NET" -v "$WORK/sql:/sql:ro" -v "$WORK/run.sh:/run.sh:ro" \
  --entrypoint bash "$FLINK_IMG" /run.sh > "$WORK/out.txt" 2>&1

# create_tables must have worked: a rejected table option shows up here, and every
# INSERT afterwards would fail for the wrong reason.
if grep -vE '^\s*>|^\s*--' "$WORK/out.txt" | grep -qiE 'InvalidConfigException|Invalid value for config|Unsupported.*merge engine|InvalidTableException'; then
  echo "  TABLE OPTION REJECTED BY THE FLUSS SERVER:"
  grep -vE '^\s*>|^\s*--' "$WORK/out.txt" | grep -iE 'InvalidConfigException|Invalid value for config|Unsupported|InvalidTable' | head -5 | sed 's/^/      /'
  [ "${KEEP:-0}" = 1 ] && cp "$WORK/out.txt" ./fluss-sql-validate.log
  exit 1
fi

FAILED=""
for f in silver_trades.sql silver_accounts.sql gold_open_positions.sql; do
  grep -q "@@@BEGIN $f@@@" "$WORK/out.txt" || continue
  N=$(sed -n "/@@@BEGIN $f@@@/,/@@@END $f@@@/p" "$WORK/out.txt" | grep -c 'Job ID:')
  if [ "$N" -ge 1 ]; then printf '  ok       %-26s %s job(s)\n' "$f" "$N"
  else
    printf '  NO JOB   %-26s submitted nothing\n' "$f"
    sed -n "/@@@BEGIN $f@@@/,/@@@END $f@@@/p" "$WORK/out.txt" | grep -vE '^\s*>|^\s*--' \
      | grep -iE 'exception|error|not supported' | head -3 | sed 's/^/      /'
    FAILED="$FAILED $f"
  fi
done
if [ -n "$FAILED" ]; then
  echo; echo "these job files compile to NOTHING:$FAILED"
  [ "${KEEP:-0}" = 1 ] && cp "$WORK/out.txt" ./fluss-sql-validate.log
  exit 1
fi
echo "fluss SQL compiles — every job file produced a job"
