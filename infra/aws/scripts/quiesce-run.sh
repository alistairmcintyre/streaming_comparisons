#!/usr/bin/env bash
# Drain the pipelines before measuring.
#
# WHY: with the generator still running, gold legitimately trails silver by whatever
# is in flight, and bronze trails the Kafka end offset by the consumer lag. Every
# correctness number taken mid-flight therefore shows a shortfall that is indistinguishable
# from real data loss. A drain step is what makes "gold == silver" a testable invariant
# rather than a race, and it is what any production reconciliation does too: compare at a
# point where the pipeline is caught up, not at an arbitrary instant.
#
# Stops the load, then waits for every consumer group to reach zero lag. Bounded: if the
# pipelines cannot catch up within QUIESCE_TIMEOUT the drain FAILS LOUDLY rather than
# letting the snapshot silently record mid-flight numbers as if they were final.
set -uo pipefail

KUBECTL="${KUBECTL:-kubectl}"
QUIESCE_TIMEOUT="${QUIESCE_TIMEOUT:-600}"     # seconds to reach zero lag
POLL="${POLL:-15}"
BROKER_POD="${BROKER_POD:-trades-dual-role-0}"

echo "== 1. stop the load =="
for d in generator-trades generator-accounts; do
  $KUBECTL -n streaming scale "deploy/$d" --replicas=0 2>/dev/null \
    && echo "  scaled $d to 0" || echo "  WARN could not scale $d (already gone?)"
done

# Postgres row count stops moving once the generators are down; that is the fixed
# denominator everything downstream is measured against.
PG=$($KUBECTL -n streaming exec postgres-0 -- \
     psql -U app -d appdb -tAc 'select count(*) from trades;' 2>/dev/null | tr -d '[:space:]')
echo "  postgres trades (final): ${PG:-unavailable}"

echo "== 2. wait for CDC + every consumer group to drain =="
lag_total() {
  # Sum lag across all consumer groups. `describe --all-groups` prints a LAG column;
  # '-' appears for partitions with no committed offset, which are not lag.
  $KUBECTL -n kafka exec "$BROKER_POD" -- bin/kafka-consumer-groups.sh \
    --bootstrap-server localhost:9092 --describe --all-groups 2>/dev/null \
    | awk '$5 ~ /^[0-9]+$/ {s+=$5} END {print s+0}'
}

deadline=$(( $(date +%s) + QUIESCE_TIMEOUT ))
stable=0
while [ "$(date +%s)" -lt "$deadline" ]; do
  L=$(lag_total)
  echo "  total consumer lag: ${L}"
  if [ "${L:-1}" -eq 0 ]; then
    # Two consecutive zeroes: one can be read between a commit and the next fetch.
    stable=$(( stable + 1 ))
    [ "$stable" -ge 2 ] && { echo "  drained."; break; }
  else
    stable=0
  fi
  sleep "$POLL"
done

if [ "${stable:-0}" -lt 2 ]; then
  echo "QUIESCE FAILED: consumer lag did not reach zero within ${QUIESCE_TIMEOUT}s." >&2
  echo "  The snapshot that follows is mid-flight: treat every count as a LOWER BOUND" >&2
  echo "  and do not read the invariant drift as data loss." >&2
  exit 1
fi

# Sinks commit on their own cadence (Spark 15s triggers, Flink 10s checkpoints, Fluss
# tiering 30s freshness). Zero lag means READ, not COMMITTED — give the slowest of those
# a couple of cycles to land before anything reads the tables.
echo "== 3. settle sink commits (2x the slowest commit interval) =="
sleep "${SETTLE_SECONDS:-90}"
echo "quiesced."
