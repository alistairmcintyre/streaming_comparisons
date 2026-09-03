#!/usr/bin/env bash
# Count the LIVE Fluss tables and print them as "<table> <rows>", one per line.
#
# The only number in this benchmark that Athena cannot produce. Athena reads
# silver.trades_fluss, which is the tiered Paimon MIRROR, not the hot table, and that
# mirror lags by design. See the header of jobs/flink-fluss/count_live.sql.
#
# Runs the SQL inside the Fluss Flink JobManager pod, which already has the SQL client and
# a route to both the JM and the Fluss coordinator. Nothing else needs to be deployed.
#
# WHAT IS AND IS NOT VERIFIED. The SQL is verified against a real Fluss cluster by
# tests/fluss_live_count_test.sh: seed a known number of rows, count them, assert. The
# kubectl plumbing below is NOT verified off-cluster and will not be until it runs on one.
#
# Prints nothing and exits non-zero if the count cannot be taken; every caller treats that
# as "unavailable" rather than as zero, because a missing count and an empty table must
# never be confused.
set -uo pipefail
KUBECTL="${KUBECTL:-kubectl}"
NS="${FLINK_NAMESPACE:-flink}"
DEPLOY="${FLUSS_FLINK_DEPLOY:-deploy/fluss-flink}"
BOOTSTRAP="${FLUSS_BOOTSTRAP:-fluss-coordinator.fluss.svc:9123}"
SQL_FILE="$(dirname "$0")/../../../jobs/flink-fluss/count_live.sql"
[ -f "$SQL_FILE" ] || SQL_FILE="jobs/flink-fluss/count_live.sql"
[ -f "$SQL_FILE" ] || { echo "count_live.sql not found" >&2; exit 1; }

RENDERED=$(FLUSS_BOOTSTRAP="$BOOTSTRAP" envsubst '${FLUSS_BOOTSTRAP}' < "$SQL_FILE") || exit 1

# Heredoc inside the pod rather than `kubectl cp`: the JM pod is not guaranteed writable
# anywhere useful, /tmp is, and this avoids a second round trip.
OUT=$($KUBECTL -n "$NS" exec "$DEPLOY" -- bash -c "cat > /tmp/count_live.sql <<'FLUSSSQL'
${RENDERED}
FLUSSSQL
\${FLINK_HOME:-/opt/flink}/bin/sql-client.sh -f /tmp/count_live.sql 2>&1" 2>/dev/null) || {
  echo "fluss live count: could not exec into ${NS}/${DEPLOY}" >&2; exit 1; }

# TABLEAU renders  | <tag> | <value> |
parse() { grep -aE "\| *$1 *\|" <<<"$OUT" | tail -1 | awk -F'|' '{gsub(/ /,"",$3); print $3}'; }
S=$(parse 'silver\.trades'); G=$(parse 'gold\.open_positions')
[ -n "$S" ] || { echo "fluss live count: no silver.trades row in output" >&2; exit 1; }
echo "silver.trades ${S}"
[ -n "$G" ] && echo "gold.open_positions ${G}"
exit 0
