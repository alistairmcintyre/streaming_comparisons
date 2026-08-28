#!/usr/bin/env bash
# Run every pre-deploy check locally, and — just as importantly — verify each check
# CAN STILL FAIL.
#
# WHY THE SECOND HALF EXISTS: on 2026-08-27 roughly $42 of clusters was spent on bugs
# that were all catchable for free. Several of the checks written to catch them were
# themselves broken in a way that made them silently pass:
#   * the SQL check asserted "at least one job submitted" and passed with 3 of 4 while
#     the gold job compiled to nothing — the exact hole that let the bug reach AWS
#   * the prometheus checks shelled out to `wget`, which does not exist in that
#     distroless container, so they could never pass regardless of reality
#   * the manifest variable guard scanned YAML COMMENTS and blocked a valid deploy
#   * the teardown verifier used `grep -c`, which exits 1 on zero, and failed a clean run
# A check nobody has watched fail is a check nobody knows works. So every checker here
# is also run against a deliberately broken fixture and MUST reject it.
#
#   tests/run-checks.sh          fast checks only (seconds, no Docker)
#   tests/run-checks.sh --all    + Flink SQL compile and its meta-tests (needs Docker)
set -uo pipefail
cd "$(dirname "$0")/.."
ALL=""; [ "${1:-}" = "--all" ] && ALL=1
PASS=0; FAIL=0; declare -a PROBLEMS
ok()  { printf '  \033[32mPASS\033[0m  %s\n' "$1"; PASS=$((PASS+1)); }
bad() { printf '  \033[31mFAIL\033[0m  %s\n' "$1"; FAIL=$((FAIL+1)); PROBLEMS+=("$1"); }
# $1 description, $2 expected rc, rest: command
expect() { local d="$1" want="$2"; shift 2; "$@" >/dev/null 2>&1; local rc=$?
           [ "$rc" = "$want" ] && ok "$d" || bad "$d (rc=$rc, wanted $want)"; }

echo "== checks: the real repo must PASS =="
expect "manifests lint clean"            0 ./infra/aws/scripts/preflight-manifests.sh
expect "shell scripts parse"             0 bash -c 'for f in $(git ls-files "*.sh"); do bash -n "$f" || exit 1; done'
expect "python jobs parse"               0 bash -c 'for f in $(git ls-files "jobs/**/*.py" "generators/*.py"); do python3 -c "import ast,sys;ast.parse(open(sys.argv[1]).read())" "$f" || exit 1; done'
expect "workflows are valid YAML"        0 python3 -c "import yaml,glob;[list(yaml.safe_load_all(open(f))) for f in glob.glob('.github/workflows/*.yml')]"
expect "every manifest var is exported"  0 bash -c '
  need=$(grep -rhv "^[[:space:]]*#" infra/aws/k8s/*.yaml | grep -oE "\\\$\{[A-Z_][A-Z0-9_]*\}" | tr -d "\${}" | sort -u)
  # Match any assignment form the workflow uses: `export X=`, `X=`, `name: X`, and the
  # top-level `env:` block form `X: value`. The first version missed the env: block and
  # flagged four perfectly-exported variables — a false alarm in the TEST, which is
  # exactly the failure mode this suite exists to prevent, so it is worth the comment.
  miss=""; for v in $need; do grep -qE "(^|[^A-Z_])${v}[:=]" .github/workflows/eks-run.yml || miss="$miss $v"; done
  [ -z "$miss" ] || { echo "unexported:$miss"; exit 1; }'

expect "alert pipeline count matches the code" 0 bash -c '
  n=$( { grep -rho "attach_latency_listener(spark, \"[a-z-]*\"" jobs/spark-*/*.py | sed "s/.*\"\(.*\)\"/\1/";
         grep -rho "\"pipeline\":\"[a-z-]*\"" jobs/flink-*/*.sql | sed "s/.*:\"\(.*\)\"/\1/"; } | sort -u | wc -l )
  want=$(grep -oE "pipeline_latency_events_total\)\) < [0-9]+" infra/aws/k8s/96-alerts.yaml | grep -oE "[0-9]+$")
  [ "$n" = "$want" ] || { echo "code emits $n pipelines, PipelinesMissing expects $want"; exit 1; }'

# Pure DataFrame logic, no table format, so it belongs in the FAST tier — seconds, and it
# holds the Spark engines to the same SCD2 definition the Flink behaviour test enforces.
expect "scd2 staging logic (shared by the Spark engines)" 0 \
  docker run --rm -u 0 -v "$PWD:/w" -w /w --entrypoint python3 \
    167217327348.dkr.ecr.eu-west-1.amazonaws.com/streaming-comparison/spark-delta:latest \
    /w/tests/scd2_spark_test.py

echo "== meta: each checker must REJECT a known-bad input =="
expect "rejects an RFC1123-invalid name" 1 ./infra/aws/scripts/preflight-manifests.sh tests/fixtures
expect "rejects a duplicate YAML key"    1 bash -c 'cd "$(git rev-parse --show-toplevel)"; d=$(mktemp -d); cp tests/fixtures/bad-dupkey.yaml "$d/"; ./infra/aws/scripts/preflight-manifests.sh "$d"'
expect "rejects envsubst-eaten \$labels" 1 bash -c 'cd "$(git rev-parse --show-toplevel)"; d=$(mktemp -d); cp tests/fixtures/bad-envsubst.yaml "$d/"; ./infra/aws/scripts/preflight-manifests.sh "$d"'
expect "accepts a clean manifest dir"    0 bash -c 'cd "$(git rev-parse --show-toplevel)"; d=$(mktemp -d); cp infra/aws/k8s/00-namespaces.yaml "$d/"; ./infra/aws/scripts/preflight-manifests.sh "$d"'
expect "image hashes are deterministic"  0 bash -c '[ "$(./infra/aws/scripts/image-hashes.sh)" = "$(./infra/aws/scripts/image-hashes.sh)" ]'
expect "image hash changes with content" 0 bash -c '
  a=$(./infra/aws/scripts/image-hashes.sh spark-hudi | awk "{print \$2}")
  echo "# probe" >> docker/spark-hudi/Dockerfile
  b=$(./infra/aws/scripts/image-hashes.sh spark-hudi | awk "{print \$2}")
  git checkout -- docker/spark-hudi/Dockerfile
  [ "$a" != "$b" ]'

if [ -n "$ALL" ]; then
  echo "== slow: Flink SQL compiles (Docker) =="
  expect "paimon SQL compiles, every file yields a job" 0 ./infra/aws/scripts/validate-flink-sql.sh
  echo "== slow: SCD2 against a REAL Delta table, across micro-batches =="
  # The unit test passes an EMPTY `current`, so it only covers versions arriving in one
  # batch. This covers the path that actually happens — version N in one micro-batch, N+1
  # in a later one, the close-out finding N by READING THE TABLE — plus the MERGE column
  # list against the real schema, which no unit test can check.
  expect "scd2 delta MERGE closes across micro-batches" 0 \
    docker run --rm -u 0 -v "$PWD:/w" -w /w --entrypoint python3 \
      167217327348.dkr.ecr.eu-west-1.amazonaws.com/streaming-comparison/spark-delta:latest \
      /w/tests/scd2_delta_merge_test.py

  echo "== slow: SCD2 against REAL Iceberg and Hudi tables =="
  # Same scenario, different WRITE MECHANISMS — and they do not agree by default. Iceberg's
  # MERGE is a separate planner and row-level-operation path; Hudi has no MERGE at all here
  # and rides entirely on the composite record key plus precombine. Running it on Hudi is
  # what found the two staging bugs that Delta's MERGE clauses had been hiding: a close row
  # with nulled attributes ERASES the version it closes, and a re-delivery restated as a
  # non-current row overwrites the live one. Both were invisible on Delta and Iceberg.
  expect "scd2 iceberg MERGE closes across micro-batches" 0 \
    docker run --rm -u 0 -v "$PWD:/w" -w /w --entrypoint python3 \
      167217327348.dkr.ecr.eu-west-1.amazonaws.com/streaming-comparison/spark-iceberg:latest \
      /w/tests/scd2_iceberg_merge_test.py
  expect "scd2 hudi upsert closes across micro-batches" 0 \
    docker run --rm -u 0 -v "$PWD:/w" -w /w --entrypoint python3 \
      167217327348.dkr.ecr.eu-west-1.amazonaws.com/streaming-comparison/spark-hudi:latest \
      /w/tests/scd2_hudi_upsert_test.py

  echo "== slow: SILVER dedupe survives a restart, on the RocksDB state store =="
  # The only place a re-delivered trade_id can be removed on Delta/Iceberg — gold folds
  # `+=` over (account_id, symbol) and never sees trade_id, so it cannot repair a
  # duplicate. Checks the state is genuinely CHECKPOINTED (the re-delivery arrives in a
  # later query run) and that the RocksDB provider the manifests set really loads.
  expect "silver dedupe holds across a restart" 0 \
    docker run --rm -u 0 -v "$PWD:/w" -w /w --entrypoint python3 \
      167217327348.dkr.ecr.eu-west-1.amazonaws.com/streaming-comparison/spark-delta:latest \
      /w/tests/dedupe_state_test.py
  expect "dedupe check catches a missing dedupe" 1 \
    docker run --rm -u 0 -v "$PWD:/w" -w /w -e NO_DEDUPE=1 --entrypoint python3 \
      167217327348.dkr.ecr.eu-west-1.amazonaws.com/streaming-comparison/spark-delta:latest \
      /w/tests/dedupe_state_test.py

  echo "== slow: GOLD fold against REAL tables, all three Spark engines =="
  # Drives each engine's OWN fold_to_book over three micro-batches — not a copy of it —
  # and asserts the exact book. The scenario forces every incremental rule: a position
  # that nets to zero (CLOSED) and reopens, and a LATE fill that must pull opened_at back
  # without dragging last_updated_at with it. All three must agree to the last decimal.
  for eng in delta iceberg hudi; do
    expect "gold fold is correct ($eng)" 0 \
      docker run --rm -u 0 -v "$PWD:/w" -w /w --entrypoint python3 \
        167217327348.dkr.ecr.eu-west-1.amazonaws.com/streaming-comparison/spark-$eng:latest \
        /w/tests/gold_fold_test.py "$eng"
  done

  echo "== slow: GOLD fold BEHAVIOUR for the Flink engines =="
  # Same trades, same expected book — that is the claim: five engines, one book. The fold
  # is lifted verbatim out of the real job file, so it cannot drift from what deploys.
  expect "gold fold is correct (paimon)" 0 ./tests/gold-behaviour.sh jobs/flink-paimon/gold_open_positions.sql
  expect "gold fold is correct (fluss)"  0 ./tests/gold-behaviour.sh jobs/flink-fluss/gold_open_positions.sql

  echo "== meta: the gold checks must catch a broken fold =="
  # Flip the sign convention so SELL adds instead of subtracting. Every engine still
  # runs, still commits, still produces a book — just the wrong one. This is the exact
  # shape of bug that no compile check can see.
  expect "gold flink check catches an inverted sign" 1 bash -c '
    d=$(mktemp -d); cp jobs/flink-paimon/gold_open_positions.sql "$d/g.sql"
    sed -i "s|ELSE -CAST(quantity AS BIGINT) END|ELSE CAST(quantity AS BIGINT) END|" "$d/g.sql"
    ./tests/gold-behaviour.sh "$d/g.sql"'
  expect "gold spark check catches an inverted sign" 1 bash -c '
    d=$(mktemp -d); cp -r jobs "$d/jobs"
    sed -i "s|.otherwise(-col(\"quantity\"))|.otherwise(col(\"quantity\"))|" "$d/jobs/spark-delta/gold_open_positions.py"
    docker run --rm -u 0 -v "$PWD:/w" -v "$d:/j" -w /w -e JOBS_ROOT=/j/jobs --entrypoint python3 \
      167217327348.dkr.ecr.eu-west-1.amazonaws.com/streaming-comparison/spark-delta:latest \
      /w/tests/gold_fold_test.py delta'

  echo "== slow: SCD2 close-out BEHAVIOUR (not just that it compiles) =="
  # The compile checks prove the SQL plans. This proves it is right — it runs the real
  # close-out over fixed account changes and asserts the validity ranges, including the
  # invariant that matters: exactly ONE current row per account. The first implementation
  # compiled and planned cleanly and produced TWO current rows for an out-of-order arrival.
  expect "scd2 close-out produces correct validity ranges" 0 ./tests/scd2-behaviour.sh

  echo "== meta: the SQL checker must catch the two bugs that cost a day =="
  # #85 — schema-valid, runtime-invalid: first-row cannot be stream-read with 'none'
  expect "catches changelog-producer=none" 1 bash -c '
    d=$(mktemp -d); cp jobs/flink-paimon/*.sql "$d/"
    sed -i "s|=\s*.lookup.,|= \x27none\x27,|" "$d/create_tables.sql"
    JOBS_DIR="$d" ./infra/aws/scripts/validate-flink-sql.sh'
  # #80 — an append-only sink fed by a GROUP BY. The SQL is written by python so the
  # quoting does not depend on nested bash heredoc escaping; the first version emitted
  # literal \x27 instead of quotes and therefore never reproduced the bug at all.
  expect "catches updating-stream into append sink" 1 bash -c '
    d=$(mktemp -d); cp jobs/flink-paimon/*.sql "$d/"
    python3 - "$d" <<PYEOF
import sys, pathlib
f = pathlib.Path(sys.argv[1]) / "gold_open_positions.sql"
f.write_text(f.read_text() + """
CREATE TEMPORARY TABLE probe_sink (\`value\` STRING) WITH (
    '"'"'connector'"'"' = '"'"'kafka'"'"',
    '"'"'topic'"'"' = '"'"'probe'"'"',
    '"'"'properties.bootstrap.servers'"'"' = '"'"'localhost:9092'"'"',
    '"'"'format'"'"' = '"'"'raw'"'"'
);
INSERT INTO probe_sink SELECT CAST(account_id AS STRING) FROM gold_book;
""")
PYEOF
    JOBS_DIR="$d" ./infra/aws/scripts/validate-flink-sql.sh'

  echo "== slow: Fluss SQL compiles (Docker: zookeeper + coordinator + tablet server) =="
  expect "fluss SQL compiles, every file yields a job" 0 ./infra/aws/scripts/validate-fluss-sql.sh

  echo "== meta: the Fluss checker must catch bad SQL and bad table options =="
  # Same shape as paimon bug #80: an append-only Kafka sink fed by a GROUP BY.
  expect "catches updating-stream into append sink (fluss)" 1 bash -c '
    d=$(mktemp -d); cp jobs/flink-fluss/*.sql "$d/"
    python3 - "$d" <<PYEOF
import sys, pathlib
f = pathlib.Path(sys.argv[1]) / "gold_open_positions.sql"
f.write_text(f.read_text() + """
CREATE TEMPORARY TABLE probe_sink (\`value\` STRING) WITH (
    '"'"'connector'"'"' = '"'"'kafka'"'"',
    '"'"'topic'"'"' = '"'"'probe'"'"',
    '"'"'properties.bootstrap.servers'"'"' = '"'"'localhost:9092'"'"',
    '"'"'format'"'"' = '"'"'raw'"'"'
);
INSERT INTO probe_sink SELECT CAST(account_id AS STRING) FROM gold_book;
""")
PYEOF
    JOBS_DIR="$d" ./infra/aws/scripts/validate-fluss-sql.sh'

  # A table option the SERVER rejects. TableDescriptorValidation throws for
  # first_row + delete.behavior=allow — verified in the Fluss source. This is the
  # class the offline checks cannot see at all.
  expect "catches a server-rejected table option" 1 bash -c '
    d=$(mktemp -d); cp jobs/flink-fluss/*.sql "$d/"
    sed -i "s|.table.merge-engine.  *= .first_row.,|'table.merge-engine' = 'first_row', 'table.delete.behavior' = 'allow',|" "$d/create_tables.sql"
    JOBS_DIR="$d" ./infra/aws/scripts/validate-fluss-sql.sh'

else
  echo "  (skipping Docker checks — run with --all)"
fi

echo
echo "════ $PASS passed, $FAIL failed ════"
if [ "$FAIL" -gt 0 ]; then for p in "${PROBLEMS[@]}"; do echo "  - $p"; done; exit 1; fi
echo "all checks green"
