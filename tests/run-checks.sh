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
