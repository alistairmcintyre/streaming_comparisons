#!/usr/bin/env bash
# Prove jobs/flink-fluss/count_live.sql returns the TRUE row count of a live Fluss table.
#
# This is the one number in the benchmark that Athena cannot produce, because Athena reads
# the tiered Paimon mirror rather than the hot table (see the header of count_live.sql).
# It therefore has no cross-check anywhere else, which is precisely why it needs one here.
#
# Writes a known number of rows into a real Fluss cluster and asserts the SQL counts them.
# Same harness as validate-fluss-sql.sh: zookeeper + coordinator + tablet server on a
# throwaway docker network.
set -uo pipefail
cd "$(dirname "$0")/.."
ROWS="${ROWS:-250}"
NET="fluss-count-$$"; ZK="zkc-$$"; CO="coc-$$"; TS="tsc-$$"
WORK=$(mktemp -d)
cleanup(){ docker rm -f "$ZK" "$CO" "$TS" >/dev/null 2>&1; docker network rm "$NET" >/dev/null 2>&1; rm -rf "$WORK"; }
trap cleanup EXIT

SERVER_IMG=""
for c in "${FLUSS_SERVER_IMAGE:-}" streaming-comparisons-fluss-coordinator:latest apache/fluss:0.9.1-incubating; do
  [ -z "$c" ] && continue
  docker image inspect "$c" >/dev/null 2>&1 && { SERVER_IMG="$c"; break; }
done
[ -n "$SERVER_IMG" ] || { echo "  no Fluss server image available locally"; exit 1; }
. scripts/ecr-env.sh
FLINK_IMG="${FLUSS_FLINK_IMAGE:-${ECR_REGISTRY:-none}/fluss-flink:latest}"
docker image inspect "$FLINK_IMG" >/dev/null 2>&1 || FLINK_IMG=apache/fluss-quickstart-flink:1.20-0.9.1-incubating

props(){ cat <<P
zookeeper.address: fluss-zookeeper:2181
$1
remote.data.dir: /tmp/fluss-remote
datalake.format: paimon
datalake.paimon.metastore: filesystem
datalake.paimon.warehouse: /tmp/fluss-paimon
P
}
docker network create "$NET" >/dev/null
docker run -d --name "$ZK" --network "$NET" --network-alias fluss-zookeeper zookeeper:3.9.2 >/dev/null
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

for i in $(seq 1 45); do
  docker logs "$CO" 2>&1 | grep -qiE 'started|ready' && break
  docker ps -q --filter "name=$CO" | grep -q . || { echo "  coordinator died"; exit 1; }
  sleep 2
done
sleep 12

export FLUSS_BOOTSTRAP="fluss-coordinator:9123"
mkdir -p "$WORK/sql"
# Seed: the tables count_live.sql reads, populated with a known number of rows. gold gets
# a different count from silver on purpose, so a SQL that counted the wrong table would
# still have to produce the wrong number to pass.
GOLD=$(( ROWS / 5 ))
cat > "$WORK/sql/seed.sql" <<SQL
CREATE CATALOG fluss_catalog WITH ('type'='fluss','bootstrap.servers'='${FLUSS_BOOTSTRAP}');
CREATE DATABASE IF NOT EXISTS fluss_catalog.silver;
CREATE DATABASE IF NOT EXISTS fluss_catalog.gold;
CREATE TABLE IF NOT EXISTS fluss_catalog.silver.trades (
  trade_id BIGINT, account_id BIGINT, PRIMARY KEY (trade_id) NOT ENFORCED
) WITH ('table.merge-engine' = 'first_row');
CREATE TABLE IF NOT EXISTS fluss_catalog.gold.open_positions (
  account_id BIGINT, net_quantity BIGINT, PRIMARY KEY (account_id) NOT ENFORCED
);
SET 'execution.runtime-mode' = 'batch';
SET 'table.dml-sync' = 'true';
CREATE TEMPORARY TABLE s_src (id BIGINT) WITH (
  'connector'='datagen','number-of-rows'='${ROWS}',
  'fields.id.kind'='sequence','fields.id.start'='1','fields.id.end'='${ROWS}');
CREATE TEMPORARY TABLE g_src (id BIGINT) WITH (
  'connector'='datagen','number-of-rows'='${GOLD}',
  'fields.id.kind'='sequence','fields.id.start'='1','fields.id.end'='${GOLD}');
INSERT INTO fluss_catalog.silver.trades SELECT id, MOD(id, 7) FROM s_src;
INSERT INTO fluss_catalog.gold.open_positions SELECT id, id * 10 FROM g_src;
SQL
envsubst '${FLUSS_BOOTSTRAP}' < jobs/flink-fluss/count_live.sql > "$WORK/sql/count_live.sql"

cat > "$WORK/run.sh" <<'INNER'
set -u
export FLINK_HOME=/opt/flink
"$FLINK_HOME/bin/start-cluster.sh" >/dev/null 2>&1
for i in $(seq 1 30); do curl -sf localhost:8081/overview >/dev/null 2>&1 && break; sleep 2; done
"$FLINK_HOME/bin/sql-client.sh" -f /sql/seed.sql 2>&1 | tail -3
echo "@@@COUNTS@@@"
"$FLINK_HOME/bin/sql-client.sh" -f /sql/count_live.sql 2>&1
INNER

echo "  seeding ${ROWS} silver rows and ${GOLD} gold rows, then counting"
docker run --rm --network "$NET" -v "$WORK/sql:/sql:ro" -v "$WORK/run.sh:/run.sh:ro" \
  --entrypoint bash "$FLINK_IMG" /run.sh > "$WORK/out.txt" 2>&1

# TABLEAU renders | tag | value |. Pull the value on the row carrying each tag.
got_silver=$(grep -aE '\| *silver\.trades *\|' "$WORK/out.txt" | tail -1 | awk -F'|' '{gsub(/ /,"",$3); print $3}')
got_gold=$(grep -aE '\| *gold\.open_positions *\|' "$WORK/out.txt" | tail -1 | awk -F'|' '{gsub(/ /,"",$3); print $3}')

fail=0
if [ "${got_silver:-}" = "$ROWS" ]; then printf '  \033[32mPASS\033[0m  silver.trades live count is %s\n' "$ROWS"
else printf '  \033[31mFAIL\033[0m  silver.trades: want %s, got "%s"\n' "$ROWS" "${got_silver:-}"; fail=1; fi
if [ "${got_gold:-}" = "$GOLD" ]; then printf '  \033[32mPASS\033[0m  gold.open_positions live count is %s\n' "$GOLD"
else printf '  \033[31mFAIL\033[0m  gold.open_positions: want %s, got "%s"\n' "$GOLD" "${got_gold:-}"; fail=1; fi
[ "$fail" -eq 0 ] || { echo "  --- output ---"; tail -30 "$WORK/out.txt" | sed 's/^/      /'; }
exit "$fail"
