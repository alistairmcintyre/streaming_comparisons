#!/usr/bin/env bash
# Does the Flink gold fold produce the RIGHT BOOK? Not "does it compile", the SQL
# validators already answer that, and both bad runs to date compiled perfectly.
#
# The fold under test is EXTRACTED FROM THE REAL JOB FILE, not retyped here: this script
# lifts the gold_book view verbatim and only repoints its FROM at a local CSV. So the
# arithmetic, the sign convention, the OPEN/CLOSED rule and the MIN/MAX choice are the
# deployed ones, and this test cannot drift away from them the way a copy would.
#
# Same trades and same expected book as tests/gold_fold_test.py runs through the three
# SPARK engines, which is the actual claim being checked: five engines, one book.
#
#   acct 1 AAPL  buy 100@10 (d5), buy 50@12 (d3), sell 150@11 (d7), buy 20@9 (d1)
#         -> goes flat then REOPENS; the d1 fill arrives LAST and is the EARLIEST, so
#            opened_at must move back to d1 while last_updated_at stays on d7
#   acct 2 MSFT  buy 10@100 (d4), sell 4@105 (d6)   -> stays OPEN
#   acct 3 TSLA  buy 5@200 (d2),  sell 5@210 (d8)   -> ends CLOSED at exactly zero
set -uo pipefail
. "$(dirname "$0")/../scripts/ecr-env.sh"
IMG="${FLINK_PAIMON_IMAGE:-$ECR_REGISTRY/flink-paimon:latest}"
ecr_required || exit 1
SRC="${1:-jobs/flink-paimon/gold_open_positions.sql}"
W=$(mktemp -d); trap 'rm -rf "$W"' EXIT
mkdir -p "$W/data" "$W/sql"

cat > "$W/data/trades.csv" <<'CSV'
1,AAPL,BUY,100,10.0000,2026-01-05 00:00:00
1,AAPL,BUY,50,12.0000,2026-01-03 00:00:00
2,MSFT,BUY,10,100.0000,2026-01-04 00:00:00
3,TSLA,BUY,5,200.0000,2026-01-02 00:00:00
1,AAPL,SELL,150,11.0000,2026-01-07 00:00:00
2,MSFT,SELL,4,105.0000,2026-01-06 00:00:00
3,TSLA,SELL,5,210.0000,2026-01-08 00:00:00
1,AAPL,BUY,20,9.0000,2026-01-01 00:00:00
CSV

# Lift the real gold_book view and repoint its source. The catalog-qualified table name
# differs between paimon and fluss and the Paimon one carries a scan.mode hint; both are
# source plumbing, not fold logic, so both are replaced by the local CSV table.
FOLD=$(sed -n '/CREATE TEMPORARY VIEW gold_book AS/,/^) p;/p' "$SRC" \
       | sed -e "s|FROM paimon\.silver\.trades|FROM src|" \
             -e "s|FROM fluss_catalog\.silver\.trades|FROM src|" \
             -e "/scan\.mode/d")
if ! echo "$FOLD" | grep -q "FROM src"; then
  echo "  could not extract the fold from $SRC, the view or its FROM clause changed shape"; exit 1
fi

{
cat <<'SQL'
SET 'execution.runtime-mode' = 'streaming';
SET 'parallelism.default'    = '1';

CREATE TEMPORARY TABLE src (
    account_id  BIGINT,
    symbol      STRING,
    side        STRING,
    quantity    INT,
    price       DECIMAL(12,4),
    executed_at TIMESTAMP(3)
) WITH ('connector' = 'filesystem', 'path' = '/data/trades.csv', 'format' = 'csv');

SQL
echo "$FOLD"
cat <<'SQL'

CREATE TEMPORARY TABLE out_sink (
    account_id BIGINT, symbol STRING, net_quantity BIGINT, net_notional DECIMAL(38,4),
    trade_count BIGINT, status STRING, opened_at TIMESTAMP(3), last_updated_at TIMESTAMP(3)
) WITH ('connector' = 'print');

INSERT INTO out_sink
SELECT account_id, symbol, net_quantity, net_notional, trade_count, status,
       opened_at, last_updated_at
FROM gold_book;
SQL
} > "$W/sql/gold.sql"

cat > "$W/run.sh" <<'INNER'
set -u
export FLINK_HOME=/opt/flink
"$FLINK_HOME/bin/start-cluster.sh" >/dev/null 2>&1
for i in $(seq 1 30); do curl -sf localhost:8081/overview >/dev/null 2>&1 && break; sleep 2; done
"$FLINK_HOME/bin/sql-client.sh" -f /sql/gold.sql 2>&1
for i in $(seq 1 40); do
  RUN=$(curl -s localhost:8081/jobs/overview | grep -o '"state":"RUNNING"' | wc -l)
  [ "$RUN" = "0" ] && break
  sleep 2
done
cat "$FLINK_HOME"/log/*taskexecutor*.out 2>/dev/null
INNER

docker run --rm -v "$W/data:/data:ro" -v "$W/sql:/sql:ro" -v "$W/run.sh:/run.sh:ro" \
  --entrypoint bash "$IMG" /run.sh > "$W/out.txt" 2>&1

# A streaming GROUP BY emits the running aggregate: +I on first sight of a key, then
# -U/+U pairs as it revises. The gold table is a PK upsert sink, so the row that survives
# is the LAST +I/+U per (account_id, symbol), retractions are not rows, they are the
# sink being told to forget the previous value.
raw=$(grep -oE '^[+-][IU]\[[^]]*\]' "$W/out.txt")
[ -n "$raw" ] || { echo "  no output rows, harness problem, not a logic result"; tail -15 "$W/out.txt" | sed 's/^/      /'; exit 1; }
book=$(echo "$raw" | grep -E '^\+[IU]\[' | sed 's/^+[IU]\[//;s/\]$//' \
       | awk -F', *' '{key=$1"|"$2; line[key]=$0} END {for (k in line) print line[k]}' | sort)

echo "  final book (last upsert per account+symbol), from $SRC:"
echo "$book" | sed 's/^/      /'

fail=0
want() { # account, symbol, expected-remainder
  got=$(echo "$book" | grep -E "^$1, ?$2," | sed -E "s/^$1, ?$2, ?//")
  if [ "$got" = "$3" ]; then echo "  PASS  $1/$2 folds to $3"
  else echo "  FAIL  $1/$2"; echo "          got  $got"; echo "          want $3"; fail=1; fi
}
want 1 AAPL "20, 130.0000, 4, OPEN, 2026-01-01T00:00, 2026-01-07T00:00"
want 2 MSFT "6, 580.0000, 2, OPEN, 2026-01-04T00:00, 2026-01-06T00:00"
want 3 TSLA "0, -50.0000, 2, CLOSED, 2026-01-02T00:00, 2026-01-08T00:00"

n=$(echo "$book" | grep -c .)
if [ "$n" = 3 ]; then echo "  PASS  exactly one row per (account, symbol)"
else echo "  FAIL  book has $n rows, expected 3"; fail=1; fi

[ "$fail" = 0 ] || { echo; echo "gold fold is wrong"; [ "${KEEP:-0}" = 1 ] && cp "$W/out.txt" ./gold-behaviour.log; exit 1; }
echo "gold fold produces the correct book"
