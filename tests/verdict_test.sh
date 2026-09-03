#!/usr/bin/env bash
# Drives every branch of the row-count verdicts in infra/aws/scripts/lib/verdict.sh.
#
# These replaced a check that could not fail. correctness.csv used to write `ok` whenever
# an Athena query returned a number, so on 2026-09-01 it passed a Hudi bronze over-count
# of +80,810 rows and a Fluss gap of 2,025,500 without comment. The last two cases below
# are that run's real numbers, and they must come out EXCESS and SHORTFALL.
set -uo pipefail
cd "$(dirname "$0")/.."
. infra/aws/scripts/lib/verdict.sh

pass=0; fail=0
expect() { # expect <want> <got> <label>
  if [ "$1" = "$2" ]; then printf '  \033[32mPASS\033[0m  %s\n' "$3"; pass=$((pass+1))
  else printf '  \033[31mFAIL\033[0m  %s (want %s, got %s)\n' "$3" "$1" "$2"; fail=$((fail+1)); fi
}

# count_verdict <actual> <expected> <tolerance> <quiesced>
expect ok         "$(count_verdict 5387000 5387000 0 true)"  "exact match is ok"
expect SHORTFALL  "$(count_verdict 5386999 5387000 0 true)"  "one row short is a SHORTFALL"
expect EXCESS     "$(count_verdict 5387001 5387000 0 true)"  "one row over is an EXCESS"
expect ok         "$(count_verdict 5386990 5387000 10 true)" "inside tolerance is ok"
expect SHORTFALL  "$(count_verdict 5386989 5387000 10 true)" "outside tolerance is not"
expect unquiesced "$(count_verdict 5387000 5387000 0 false)" "an unquiesced run gets no verdict"
expect unquiesced "$(count_verdict 5387000 5387000 0 unknown)" "unknown quiesce is not a pass"
# the two defects the old check waved through
expect EXCESS     "$(count_verdict 5467810 5387000 0 true)"  "hudi bronze +80,810 is an EXCESS"
expect SHORTFALL  "$(count_verdict 3361500 5387000 0 true)"  "a 2,025,500 gap is a SHORTFALL"

# hop_verdict <upstream> <downstream> <quiesced>
expect ok         "$(hop_verdict 5387000 5387000 true)"      "silver matching bronze is ok"
expect INCOMPLETE "$(hop_verdict 5387000 5174500 true)"      "iceberg silver 212,500 short is INCOMPLETE"
expect INCOMPLETE "$(hop_verdict 5387000 5241000 true)"      "delta silver 146,000 short is INCOMPLETE"
expect EXCESS     "$(hop_verdict 5387000 5387001 true)"      "a deduped hop gaining a row is an EXCESS"
expect unquiesced "$(hop_verdict 5387000 5174500 false)"     "mid-flight lag is not a failure"

printf '\n════ %d passed, %d failed ════\n' "$pass" "$fail"
[ "$fail" -eq 0 ]
