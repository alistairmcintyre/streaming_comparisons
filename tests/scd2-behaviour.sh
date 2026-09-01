#!/usr/bin/env bash
# Does the SCD2 atomic close-out actually produce correct validity ranges?
#
# The compile checks prove the SQL PLANS. They do not prove it is right. This runs the
# real close-out logic over a fixed set of account changes and asserts the output, using
# the scenario that exposed the design flaw in the first place:
#
#   account 1: tier A at lsn 100 (day 1), then tier B at lsn 200 (day 5)
#   account 2: tier X at lsn 150, never changed
#   account 3: tier P at lsn 400, then an OUT-OF-ORDER arrival at lsn 300
#   account 4: tier Z at lsn 500, delivered TWICE (at-least-once CDC re-delivery)
#
# Expected:
#   (1, A) closed  -> effective_to = day 5, is_current false
#   (1, B) open    -> effective_to NULL,    is_current true
#   (2, X) open    -> effective_to NULL,    is_current true     (never closed)
#   (3, P) open    -> not closed by the lsn-300 arrival: the source_lsn > prev_lsn guard
#                     must skip it rather than write a range that runs backwards.
#   (3, Q) closed  -> the late arrival is a HISTORICAL version, not a second current one.
#
# That last case is the one that matters: the first implementation marked it current, so
# account 3 had two current rows. It compiled and planned cleanly. Only running it found it.
#
# Filesystem source (bounded) + print sink, so the job finishes and the output is
# assertable. No Kafka, no Paimon, no AWS.
set -uo pipefail
. "$(dirname "$0")/../scripts/ecr-env.sh"
IMG="${FLINK_PAIMON_IMAGE:-$ECR_REGISTRY/flink-paimon:latest}"
ecr_required || exit 1
W=$(mktemp -d); trap 'rm -rf "$W"' EXIT
mkdir -p "$W/data" "$W/sql"

cat > "$W/data/changes.csv" <<'CSV'
1,A,2026-01-01 00:00:00,100
2,X,2026-01-03 00:00:00,150
1,B,2026-01-05 00:00:00,200
3,P,2026-01-06 00:00:00,400
3,Q,2026-01-07 00:00:00,300
4,Z,2026-01-08 00:00:00,500
4,Z,2026-01-08 00:00:00,500
CSV

cat > "$W/sql/scd2.sql" <<'SQL'
SET 'execution.runtime-mode' = 'streaming';
SET 'parallelism.default'    = '1';

CREATE TEMPORARY TABLE src (
    account_id BIGINT,
    tier       STRING,
    ev         TIMESTAMP(3),
    lsn        BIGINT,
    proc AS PROCTIME()
) WITH (
    'connector' = 'filesystem',
    'path'      = '/data',
    'format'    = 'csv'
);

CREATE TEMPORARY VIEW changes AS
SELECT account_id, tier, ev AS effective_from, lsn AS source_lsn,
       LAG(tier) OVER w AS prev_tier,
       LAG(ev)   OVER w AS prev_effective_from,
       LAG(lsn)  OVER w AS prev_lsn
FROM src
WINDOW w AS (PARTITION BY account_id ORDER BY proc);

CREATE TEMPORARY TABLE out_sink (
    account_id BIGINT, tier STRING, effective_from TIMESTAMP(3),
    effective_to TIMESTAMP(3), is_current BOOLEAN, source_lsn BIGINT
) WITH ('connector' = 'print');

EXECUTE STATEMENT SET
BEGIN
INSERT INTO out_sink
SELECT account_id, tier, effective_from,
       CASE WHEN prev_lsn IS NULL OR source_lsn > prev_lsn
            THEN CAST(NULL AS TIMESTAMP(3)) ELSE prev_effective_from END,
       (prev_lsn IS NULL OR source_lsn > prev_lsn),
       source_lsn
FROM changes
WHERE prev_lsn IS NULL OR source_lsn <> prev_lsn;

INSERT INTO out_sink
SELECT account_id, prev_tier, prev_effective_from, effective_from, FALSE, prev_lsn
FROM changes
WHERE prev_lsn IS NOT NULL AND source_lsn > prev_lsn;
END;
SQL

cat > "$W/run.sh" <<'INNER'
set -u
export FLINK_HOME=/opt/flink
"$FLINK_HOME/bin/start-cluster.sh" >/dev/null 2>&1
for i in $(seq 1 30); do curl -sf localhost:8081/overview >/dev/null 2>&1 && break; sleep 2; done
"$FLINK_HOME/bin/sql-client.sh" -f /sql/scd2.sql 2>&1
for i in $(seq 1 30); do
  RUN=$(curl -s localhost:8081/jobs/overview | grep -o '"state":"RUNNING"' | wc -l)
  [ "$RUN" = "0" ] && break
  sleep 2
done
cat "$FLINK_HOME"/log/*taskexecutor*.out 2>/dev/null
INNER

docker run --rm -v "$W/data:/data:ro" -v "$W/sql:/sql:ro" -v "$W/run.sh:/run.sh:ro" \
  --entrypoint bash "$IMG" /run.sh > "$W/out.txt" 2>&1

raw=$(grep -oE '^\+I\[[^]]*\]' "$W/out.txt" | sed 's/^+I\[//;s/\]$//')
# MODEL THE SINK. The real target is a PK table on (account_id, source_lsn), so when the
# close-out rewrites version N it MERGES onto the original row. The print connector has no
# merge semantics and emits both, which looked like "account 1 has 2 current rows", a
# property of the harness, not of the logic. Collapse by key, last write wins, as the PK
# table would.
rows=$(echo "$raw" | awk -F', *' '{key=$1"|"$6; line[key]=$0} END {for (k in line) print line[k]}' | sort)
[ -n "$raw" ] || { echo "  no output rows, harness problem, not a logic result"; tail -15 "$W/out.txt" | sed 's/^/      /'; exit 1; }
echo "  emitted rows (raw):"; echo "$raw" | sed 's/^/      /'
echo "  after PK merge on (account_id, source_lsn):"; echo "$rows" | sed 's/^/      /'

fail=0
chk() { # description, grep-pattern
  if echo "$rows" | grep -qE "$2"; then echo "  PASS  $1"
  else echo "  FAIL  $1  (no row matching: $2)"; fail=1; fi
}
chk "account 1 v100 CLOSED at day 5"      '^1, ?A, ?2026-01-01.*2026-01-05.*false'
chk "account 1 v200 is current"           '^1, ?B, ?2026-01-05.*(null|NULL).*true'
chk "account 2 never closed"              '^2, ?X, ?2026-01-03.*(null|NULL).*true'
chk "account 3 v400 emitted as current"   '^3, ?P, ?2026-01-06.*(null|NULL).*true'
# A re-delivery must be a NO-OP. Restating it as a non-current row is not harmless: the PK
# table merges it onto the live row and the account ends up with zero current versions.
# The equivalent bug in the Spark staging was found by tests/scd2_hudi_upsert_test.py.
chk "account 4 re-delivery left the row current" '^4, ?Z, ?2026-01-08.*(null|NULL).*true'
if echo "$rows" | grep -qE '^3, ?P.*false'; then
  echo "  FAIL  out-of-order guard: lsn 300 closed lsn 400, validity would run backwards"; fail=1
else
  echo "  PASS  out-of-order arrival did not close the newer version"
fi
# The invariant: exactly one current row per account. The first version of this logic
# marked the out-of-order arrival current too, giving account 3 two current rows, which
# compiles, plans, and silently fans out every downstream join.
for a in 1 2 3 4; do
  n=$(echo "$rows" | grep -cE "^$a, .*, true, ")
  if [ "$n" = 1 ]; then echo "  PASS  account $a has exactly one current row"
  else echo "  FAIL  account $a has $n current rows (must be 1)"; fail=1; fi
done
[ "$fail" = 0 ] || { echo; echo "SCD2 close-out is wrong"; [ "${KEEP:-0}" = 1 ] && cp "$W/out.txt" ./scd2-behaviour.log; exit 1; }
echo "SCD2 close-out behaves correctly"
