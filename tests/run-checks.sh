#!/usr/bin/env bash
# Run every pre-deploy check locally, and (just as importantly) verify each check
# CAN STILL FAIL.
#
# WHY THE SECOND HALF EXISTS: on 2026-08-27 roughly $42 of clusters was spent on bugs
# that were all catchable for free. Several of the checks written to catch them were
# themselves broken in a way that made them silently pass:
#   * the SQL check asserted "at least one job submitted" and passed with 3 of 4 while
#     the gold job compiled to nothing, the exact hole that let the bug reach AWS
#   * the prometheus checks shelled out to `wget`, which does not exist in that
#     distroless container, so they could never pass regardless of reality
#   * the manifest variable guard scanned YAML COMMENTS and blocked a valid deploy
#   * the teardown verifier used `grep -c`, which exits 1 on zero, and failed a clean run
# A check nobody has watched fail is a check nobody knows works. So every checker here
# is also run against a deliberately broken fixture and must reject it.
#
#   tests/run-checks.sh          fast checks only (seconds, no Docker)
#   tests/run-checks.sh --all    + Flink SQL compile and its meta-tests (needs Docker)
set -uo pipefail
cd "$(dirname "$0")/.."
ALL=""; [ "${1:-}" = "--all" ] && ALL=1
# Image registry for the Docker-backed checks, resolved from env/aws.env in one place.
# shellcheck disable=SC1091
. "$(dirname "$0")/../scripts/ecr-env.sh"
# EXPORTED: several meta-tests run their docker command inside a single-quoted
# `bash -c`, where $ECR is expanded by the inner shell. Unexported, it resolves to
# an empty string there and docker exits 125 with an unparseable image reference.
export ECR="$ECR_REGISTRY"
PASS=0; FAIL=0; declare -a PROBLEMS
ok()  { printf '  \033[32mPASS\033[0m  %s\n' "$1"; PASS=$((PASS+1)); }
bad() { printf '  \033[31mFAIL\033[0m  %s\n' "$1"; FAIL=$((FAIL+1)); PROBLEMS+=("$1"); }
# $1 description, $2 expected rc, rest: command
expect() { local d="$1" want="$2"; shift 2; "$@" >/dev/null 2>&1; local rc=$?
           [ "$rc" = "$want" ] && ok "$d" || bad "$d (rc=$rc, wanted $want)"; }

echo "== checks: the real repo must PASS =="
expect "manifests lint clean"            0 ./infra/aws/scripts/preflight-manifests.sh
expect "shell scripts parse"             0 bash -c 'for f in $(git ls-files "*.sh"); do bash -n "$f" || exit 1; done'
expect "no AWS account id is hardcoded" 0 python3 tests/check_no_account_id.py

expect "python jobs parse"               0 bash -c 'for f in $(git ls-files "jobs/**/*.py" "generators/*.py"); do python3 -c "import ast,sys;ast.parse(open(sys.argv[1]).read())" "$f" || exit 1; done'
expect "workflows are valid YAML"        0 python3 -c "import yaml,glob;[list(yaml.safe_load_all(open(f))) for f in glob.glob('.github/workflows/*.yml')]"
expect "every manifest var is exported"  0 bash -c '
  need=$(grep -rhv "^[[:space:]]*#" infra/aws/k8s/*.yaml | grep -oE "\\\$\{[A-Z_][A-Z0-9_]*\}" | tr -d "\${}" | sort -u)
  # Match any assignment form the workflow uses: `export X=`, `X=`, `name: X`, and the
  # top-level `env:` block form `X: value`. The first version missed the env: block and
  # flagged four perfectly-exported variables, a false alarm in the TEST, which is
  # exactly the failure mode this suite exists to prevent, so it is worth the comment.
  miss=""; for v in $need; do grep -qE "(^|[^A-Z_])${v}[:=]" .github/workflows/eks-run.yml || miss="$miss $v"; done
  [ -z "$miss" ] || { echo "unexported:$miss"; exit 1; }'

expect "alert pipeline count matches the code" 0 bash -c '
  n=$( { grep -rho "attach_latency_listener(spark, \"[a-z-]*\"" jobs/spark-*/*.py | sed "s/.*\"\(.*\)\"/\1/";
         grep -rho "\"pipeline\":\"[a-z-]*\"" jobs/flink-*/*.sql | sed "s/.*:\"\(.*\)\"/\1/"; } | sort -u | wc -l )
  want=$(grep -oE "pipeline_latency_events_total\)\) < [0-9]+" infra/aws/k8s/96-alerts.yaml | grep -oE "[0-9]+$")
  [ "$n" = "$want" ] || { echo "code emits $n pipelines, PipelinesMissing expects $want"; exit 1; }'

# Pure DataFrame logic, no table format, so it belongs in the FAST tier, seconds, and it
# holds the Spark engines to the same SCD2 definition the Flink behaviour test enforces.
# The only Docker check outside the --all gate: it needs a Spark image, so it is
# skipped rather than failed when no registry is configured. Everything else in this
# section is pure python/bash and runs on a fresh clone with no AWS setup at all.
if ecr_required 2>/dev/null; then
  expect "scd2 staging logic (shared by the Spark engines)" 0 \
    docker run --rm -u 0 -v "$PWD:/w" -w /w --entrypoint python3 \
      $ECR/spark-delta:latest \
      /w/tests/scd2_spark_test.py
else
  echo "  SKIP  scd2 staging logic (no container registry configured)"
fi

# Valid YAML is not valid SHELL. A doubled line-continuation backslash inside a run:
# block keeps the YAML perfectly parseable and breaks bash, which is how a broken
# validate job reached GitHub and failed there instead of here.
expect "every workflow run: block is valid shell" 0 \
  python3 tests/check_workflow_shell.py

# clean_lake is what makes a run a FRESH benchmark. It is a hand-maintained prefix list,
# so a manifest that starts writing somewhere new is silently uncovered, which is how
# _flink_ha and _flink_savepoints accrued objects across every run since HA was added.
expect "every manifest S3 prefix is wiped or deliberately preserved" 0 \
  python3 tests/check_clean_lake.py

# Once terraform apply has run the cluster EXISTS and bills until teardown, so what runs
# between a failed apply and `terraform destroy` is money. Guards the four shapes that
# cost: the sleep must not be always(), quiesce/snapshot must be gated on the apply, and
# diagnostics + destroy must still run on failure.
expect "post-run steps are gated so a failed apply does not bill" 0 \
  python3 tests/check_workflow_gating.py

# The kind pre-flight can only validate a kind whose CRD it installs, and it used to
# report "ok" for any manifest that failed with "no matches for kind", so 14
# SparkApplications went unchecked while the gate reported green. This needs no cluster:
# it maps every custom kind to the thing that installs its CRD, and fails on a blind spot.
expect "every custom resource is covered by the kind pre-flight" 0 \
  python3 tests/check_kind_coverage.py

# `kubectl apply --dry-run=server` CREATES NOTHING, so a namespace that is only ever
# dry-run does not exist when the manifests inside it are checked, and the gate reports
# them REJECTED with "namespaces ... not found". A real run died on exactly that: fluss.
expect "every namespace exists before the server dry-run" 0 \
  python3 tests/check_namespace_prereqs.py

# ATHENA IS SELECT-ONLY. Its DDL engine is a second, weaker path to the same catalog: it
# rejected the Delta tables outright ("Delta protocol version is too new for Athena DDL
# engine") while its QUERY engine reads them perfectly well. Everything that creates a
# catalog object goes through the Glue API, which works for every format here and is
# testable offline (tests/glue-registration-test.sh).
# The kill switch is the BACKSTOP, it runs when the workflow's destroy did not. It was
# weaker than the path it backs up: every teardown fix went into the workflow and none into
# killswitch.tf, so when it finally ran it died on a SG DependencyViolation and left a VPC,
# an EIP and 190GB of EBS billing for ~5 hours.
expect "both teardown paths handle every blocker we have hit" 0 \
  python3 tests/check_teardown_parity.py

expect "nothing creates catalog objects with Athena DDL" 0 bash -c '
  hits=$(grep -rnE "athena \"(CREATE|DROP|ALTER|MSCK)" infra/aws/scripts/ .github/workflows/ tests/ 2>/dev/null || true)
  [ -z "$hits" ] || { echo "Athena DDL found, use the Glue API instead:"; echo "$hits"; exit 1; }'

# A DDL constrains the TABLE, not the frame written to it. Iceberg matches a positional
# append BY POSITION and crash-looped on `source_lsn is out of order`; Delta appends by
# NAME and never noticed the same defect. Every Spark write must be pinned to the canon, 
# by conform() for positional appends, OR by an explicit MERGE column list.
expect "every Spark write path is pinned to the canon" 0 \
  python3 tests/check_write_paths.py

expect "job imports: all shipped, none shadowed" 0 \
  python3 tests/check_configmaps.py

# All five pipelines must write the SAME FIELDS or the benchmark compares different
# tables. This checks every DECLARED schema (delta, iceberg, paimon, fluss) for
# silver.trades, silver.accounts and gold.open_positions. Hudi has no DDL, so it is pinned
# by conform() at each write and MEASURED on the real table by gold_fold_test.py; its gold
# net_notional had inferred to decimal(33,4) against the declared (38,4).
expect "all declared schemas match jobs/_shared/schemas.py" 0 \
  python3 tests/schema_parity_test.py

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

expect "schema parity check catches a drifted field type" 1 bash -c '
  d=$(mktemp -d); mkdir -p "$d/jobs/flink-paimon" "$d/jobs/flink-fluss"
  cp -r jobs/_shared "$d/jobs/_shared"
  cp jobs/flink-fluss/create_tables.sql "$d/jobs/flink-fluss/"
  sed "s|net_notional    DECIMAL(38,4)|net_notional    DECIMAL(20,4)|" \
    jobs/flink-paimon/create_tables.sql > "$d/jobs/flink-paimon/create_tables.sql"
  cd "$d" && python3 "$OLDPWD/tests/schema_parity_test.py"'

expect "write-path check catches an unpinned projection" 1 bash -c '
  d=$(mktemp -d); cp -r jobs "$d/jobs"
  sed -i "s|    parsed = conform(parsed, BRONZE_TRADES)||" "$d/jobs/spark-iceberg/bronze_trades.py"
  cd "$d" && python3 "$OLDPWD/tests/check_write_paths.py"'

expect "namespace check catches an only-dry-run namespace" 1 bash -c '
  d=$(mktemp -d); mkdir -p "$d/.github/workflows" "$d/infra/aws"
  cp -r infra/aws/k8s "$d/infra/aws/k8s"
  grep -v "envsubst < infra/aws/k8s/00-namespaces.yaml | kubectl apply -f -" \
    .github/workflows/eks-run.yml > "$d/.github/workflows/eks-run.yml"
  cd "$d" && python3 "$OLDPWD/tests/check_namespace_prereqs.py"'

expect "kind-coverage check catches an uninstalled CRD" 1 bash -c '
  d=$(mktemp -d); mkdir -p "$d/infra/aws/scripts"
  cp -r infra/aws/k8s "$d/infra/aws/k8s"
  grep -v "spark-operator/spark-operator" infra/aws/scripts/validate-against-kind.sh \
    > "$d/infra/aws/scripts/validate-against-kind.sh"
  cd "$d" && python3 "$OLDPWD/tests/check_kind_coverage.py"'

expect "teardown parity catches a weaker backstop" 1 bash -c '
  d=$(mktemp -d); mkdir -p "$d/.github/workflows" "$d/infra/aws"
  cp .github/workflows/eks-run.yml "$d/.github/workflows/"
  grep -v "revoke-security-group-ingress" infra/aws/killswitch.tf > "$d/infra/aws/killswitch.tf"
  cd "$d" && python3 "$OLDPWD/tests/check_teardown_parity.py"'

expect "gating check catches an ungated quiesce" 1 bash -c '
  d=$(mktemp -d); mkdir -p "$d/.github/workflows"
  python3 -c "
import pathlib, sys
s = pathlib.Path(\".github/workflows/eks-run.yml\").read_text()
gate = \"        if: \${{ always() && steps.apply.outcome == '"'"'success'"'"' }}\"
assert gate in s
s = s.replace(gate, \"        if: always()\", 1)   # first is quiesce
pathlib.Path(sys.argv[1] + \"/.github/workflows/eks-run.yml\").write_text(s)
" "$d"
  cd "$d" && python3 "$OLDPWD/tests/check_workflow_gating.py"'

expect "clean_lake check catches a prefix dropped from the wipe" 1 bash -c '
  d=$(mktemp -d); mkdir -p "$d/.github/workflows" "$d/infra/aws"
  cp -r infra/aws/k8s "$d/infra/aws/k8s"
  sed "s|for p in paimon fluss _flink_chk _flink_ha _flink_savepoints; do|for p in paimon fluss _flink_chk; do|" \
    .github/workflows/eks-run.yml > "$d/.github/workflows/eks-run.yml"
  cd "$d" && python3 "$OLDPWD/tests/check_clean_lake.py"'

expect "workflow shell check catches \$? tested under set -e" 1 bash -c '
  d=$(mktemp -d); mkdir -p "$d/.github/workflows"
  sed "s|if \[ \"\$RC\" -ne 0 \] \&\& ! echo \"\$R\"|if [ \$? -ne 0 ] \&\& ! echo \"\$R\"|" \
    .github/workflows/eks-run.yml > "$d/.github/workflows/eks-run.yml"
  cd "$d" && python3 "$OLDPWD/tests/check_workflow_shell.py"'

expect "workflow shell check catches a doubled backslash" 1 bash -c '
  d=$(mktemp -d); mkdir -p "$d/.github/workflows"
  python3 tests/fixtures/inject_doubled_backslash.py "$d/.github/workflows/eks-run.yml"
  cd "$d" && python3 "$OLDPWD/tests/check_workflow_shell.py"'

expect "account-id check catches a committed id" 1 bash -c '
  d=$(mktemp -d); cd "$d"; git init -q .
  # built at runtime, or this fixture would trip the very checker it is testing
  A=1234; A="${A}56789012"
  mkdir -p infra; echo "role-to-assume: arn:aws:iam::${A}:role/x" > infra/role.yml
  git add -A
  python3 "$OLDPWD/tests/check_no_account_id.py"'

expect "import check catches a shadowed shared name" 1 bash -c '
  d=$(mktemp -d); mkdir -p "$d/.github/workflows"; cp -r jobs "$d/jobs"
  cp .github/workflows/eks-run.yml "$d/.github/workflows/"
  sed -i "s|from hudi_tables import SILVER_ACCOUNTS as SILVER_ACCOUNTS_PATH, silver_accounts_opts|from hudi_tables import SILVER_ACCOUNTS, silver_accounts_opts|" \
    "$d/jobs/spark-hudi/silver_accounts.py"
  cd "$d" && python3 "$OLDPWD/tests/check_configmaps.py"'

expect "configmap check catches an unshipped shared module" 1 bash -c '
  d=$(mktemp -d); mkdir -p "$d/.github/workflows"; cp -r jobs "$d/jobs"
  python3 - "$d" <<PYEOF
import sys, pathlib
s = pathlib.Path(".github/workflows/eks-run.yml").read_text()
s = s.replace("            --from-file=jobs/_shared/ \\", "            --from-file=jobs/_shared/latency.py \\")
(pathlib.Path(sys.argv[1]) / ".github/workflows/eks-run.yml").write_text(s)
PYEOF
  cd "$d" && python3 "$OLDPWD/tests/check_configmaps.py"'

if [ -n "$ALL" ] && ! ecr_required; then
  echo "  (skipping Docker checks: no registry configured)"
  ALL=""
fi
if [ -n "$ALL" ]; then
  echo "== slow: Flink SQL compiles (Docker) =="
  expect "paimon SQL compiles, every file yields a job" 0 ./infra/aws/scripts/validate-flink-sql.sh
  echo "== slow: SCD2 against a REAL Delta table, across micro-batches =="
  # The unit test passes an EMPTY `current`, so it only covers versions arriving in one
  # batch. This covers the path that actually happens, version N in one micro-batch, N+1
  # in a later one, the close-out finding N by READING THE TABLE, plus the MERGE column
  # list against the real schema, which no unit test can check.
  expect "scd2 delta MERGE closes across micro-batches" 0 \
    docker run --rm -u 0 -v "$PWD:/w" -w /w --entrypoint python3 \
      $ECR/spark-delta:latest \
      /w/tests/scd2_delta_merge_test.py

  echo "== slow: SCD2 against REAL Iceberg and Hudi tables =="
  # Same scenario, different WRITE MECHANISMS, and they do not agree by default. Iceberg's
  # MERGE is a separate planner AND row-level-operation path; Hudi has no MERGE at all here
  # and rides entirely on the composite record key plus precombine. Running it on Hudi is
  # what found the two staging bugs that Delta's MERGE clauses had been hiding: a close row
  # with nulled attributes ERASES the version it closes, and a re-delivery restated as a
  # non-current row overwrites the live one. Both were invisible on Delta and Iceberg.
  expect "scd2 iceberg MERGE closes across micro-batches" 0 \
    docker run --rm -u 0 -v "$PWD:/w" -w /w --entrypoint python3 \
      $ECR/spark-iceberg:latest \
      /w/tests/scd2_iceberg_merge_test.py
  expect "scd2 hudi upsert closes across micro-batches" 0 \
    docker run --rm -u 0 -v "$PWD:/w" -w /w --entrypoint python3 \
      $ECR/spark-hudi:latest \
      /w/tests/scd2_hudi_upsert_test.py

  # ...and the same write path under a REAL streaming query, with MANY keys per batch.
  # The test above hand-calls stage_scd2 over 4 tiny batches, so it never built a big
  # `current` lookup, and that is exactly what killed hudi-silver-accounts on AWS.
  # Pruning the read with `account_id IN (<one literal per key>)` made Hudi's column-stats
  # data skipping expand it into a ~600-deep nested binary predicate, and Catalyst's
  # RECURSIVE resolver overflowed the driver stack. Depth == distinct keys in the batch,
  # so it is invisible at 4 keys and certain at 1000. Driver heap is pinned to the 2g in
  # infra/aws/k8s/95-spark-hudi.yaml: the failure mode is heap-sensitive, and a probe at a
  # different heap says nothing about AWS.
  expect "hudi silver.accounts survives a wide-key streaming batch" 0 \
    docker run --rm -u 0 -v "$PWD:/w" -w /w --entrypoint python3 \
      -e SOAK_BATCHES=6 -e SOAK_EVENTS_PER=300 -e SOAK_DRIVER_MEM=2g \
      $ECR/spark-hudi:latest \
      /w/tests/hudi_accounts_soak.py

  echo "== slow: an executor must fit the pod memory it is actually given =="
  # ExecutorDeadException (delta-bronze) and MetadataFetchFailedException (iceberg-silver)
  # both mean the single executor vanished, and OOMKill was the first suspect. Container
  # capped at what the operator really grants a `type: Python` app declaring memory: "2g"
 # (2048 heap + Spark's 0.4 non-JVM overhead = 2867MiB) because the variable under
  # test is the CGROUP LIMIT, not the data volume, and that needs no cluster.
  # `state` drives past a full 2h dedupe window of distinct trade_ids on the RocksDB
  # store; `batch` drives delta-bronze's 200k-row micro-batches through PySpark.
  expect "iceberg-silver state store fits its pod (2h window of keys)" 0 \
    docker run --rm -u 0 --memory=2867m --memory-swap=2867m -v "$PWD:/w" -w /w \
      --entrypoint python3 -e MEM_MODE=state -e MEM_ROWS=8000000 -e MEM_HEAP=2g \
      $ECR/spark-iceberg:latest \
      /w/tests/executor_memory_soak.py
  expect "delta-bronze 200k-row batches fit their pod" 0 \
    docker run --rm -u 0 --memory=2867m --memory-swap=2867m -v "$PWD:/w" -w /w \
      --entrypoint python3 -e MEM_MODE=batch -e MEM_ROWS=4000000 -e MEM_HEAP=2g \
      $ECR/spark-delta:latest \
      /w/tests/executor_memory_soak.py

  echo "== slow: ensure_all() must not disturb tables other jobs are streaming =="
  # ALTER TABLE ... SET TBLPROPERTIES writes a Delta metadata commit even when nothing
  # changed, and all four Delta jobs call ensure_all() over all four tables at startup, 
  # so any job start killed the others' source streams with DELTA_METADATA_CHANGED.
  # delta-silver-trades finished a live run 37 versions behind bronze because of it.
  expect "ensure_all writes nothing when properties match" 0 \
    docker run --rm -u 0 -v "$PWD:/w" -w /w --entrypoint python3 \
      $ECR/spark-delta:latest \
      /w/tests/delta_converge_test.py

  echo "== slow: SILVER dedupe survives a restart, on the RocksDB state store =="
  # The only place a re-delivered trade_id can be removed on Delta/Iceberg, gold folds
  # `+=` over (account_id, symbol) and never sees trade_id, so it cannot repair a
  # duplicate. Checks the state is genuinely CHECKPOINTED (the re-delivery arrives in a
  # later query run) and that the RocksDB provider the manifests set really loads.
  expect "silver dedupe holds across a restart" 0 \
    docker run --rm -u 0 -v "$PWD:/w" -w /w --entrypoint python3 \
      $ECR/spark-delta:latest \
      /w/tests/dedupe_state_test.py
  expect "dedupe check catches a missing dedupe" 1 \
    docker run --rm -u 0 -v "$PWD:/w" -w /w -e NO_DEDUPE=1 --entrypoint python3 \
      $ECR/spark-delta:latest \
      /w/tests/dedupe_state_test.py

  echo "== slow: Glue registration, against a local AWS emulator (floci) =="
  # register-glue-tables.sh once registered paimon and fluss with "Columns": [], and Athena
  # reads an Iceberg table's columns FROM THE GLUE DEFINITION, so count(*) worked while
  # SELECT * failed with "Relation contains no accessible columns". Three of five engines
  # unqueryable, found by hand on a live cluster. This runs the real script against floci
  # and asserts every table carries the canonical column list.
  # It does not test whether Athena can READ these formats: floci's Athena is a DuckDB
  # sidecar that is not wired to the Glue catalog. Deletion vectors, Hudi timelines and
  # Iceberg deletes were settled against real AWS and are out of scope here.
  expect "glue registration builds valid tables" 0 ./tests/glue-registration-test.sh

  echo "== slow: HUDI's written schemas, measured (it has no DDL to check) =="
  # Every other engine DECLARES its schema and schema_parity_test.py reads the
  # declaration. A Hudi table's schema is whatever DataFrame was written, so the only
  # truth is the table on disk, which is how gold's decimal(33,4) got past every
  # value-based check. bronze, silver.trades and silver.accounts, each through the same
  # conform() call the job uses; gold is measured by gold_fold_test.py.
  expect "hudi's written schemas match the canon" 0 \
    docker run --rm -u 0 -v "$PWD:/w" -w /w --entrypoint python3 \
      $ECR/spark-hudi:latest \
      /w/tests/hudi_schema_test.py
  # Meta-check that cannot self-cancel: mutating the CANON would just make conform() cast
  # to the new type and the comparison would still pass. Dropping executed_date from
  # PARTITION_ARTEFACTS instead leaves a real column unaccounted for, which the
  # comparison must report as EXTRA.
  expect "glue check catches an empty Columns list" 1 bash -c '
    d=$(mktemp -d); cp -r jobs infra tests "$d/"
    sed -i "s|\\\\\"Columns\\\\\": \${COLS}|\\\\\"Columns\\\\\": []|g" \
      "$d/infra/aws/scripts/register-glue-tables.sh"
    cd "$d" && FLOCI_PORT=4601 ./tests/glue-registration-test.sh'

  expect "hudi schema check catches an unaccounted column" 1 bash -c '
    d=$(mktemp -d); cp -r jobs "$d/jobs"
    sed -i "s|^PARTITION_ARTEFACTS = .*|PARTITION_ARTEFACTS = set()|" "$d/jobs/_shared/schemas.py"
    docker run --rm -u 0 -v "$PWD:/w" -v "$d/jobs:/w/jobs" -w /w --entrypoint python3 \
      $ECR/spark-hudi:latest \
      /w/tests/hudi_schema_test.py'

  echo "== slow: GOLD fold against REAL tables, all three Spark engines =="
  # Drives each engine's OWN fold_to_book over three micro-batches, not a copy of it, 
  # and asserts the exact book. The scenario forces every incremental rule: a position
  # that nets to zero (CLOSED) and reopens, and a LATE fill that must pull opened_at back
  # without dragging last_updated_at with it. All three must agree to the last decimal.
  for eng in delta iceberg hudi; do
    expect "gold fold is correct ($eng)" 0 \
      docker run --rm -u 0 -v "$PWD:/w" -w /w --entrypoint python3 \
        $ECR/spark-$eng:latest \
        /w/tests/gold_fold_test.py "$eng"
  done

  echo "== slow: GOLD fold BEHAVIOUR for the Flink engines =="
  # Same trades, same expected book, that is the claim: five engines, one book. The fold
  # is lifted verbatim out of the real job file, so it cannot drift from what deploys.
  expect "gold fold is correct (paimon)" 0 ./tests/gold-behaviour.sh jobs/flink-paimon/gold_open_positions.sql
  expect "gold fold is correct (fluss)"  0 ./tests/gold-behaviour.sh jobs/flink-fluss/gold_open_positions.sql

  echo "== meta: the gold checks must catch a broken fold =="
  # Flip the sign convention so SELL adds instead of subtracting. Every engine still
  # runs, still commits, still produces a book, just the wrong one. This is the exact
  # shape of bug that no compile check can see.
  expect "gold flink check catches an inverted sign" 1 bash -c '
    d=$(mktemp -d); cp jobs/flink-paimon/gold_open_positions.sql "$d/g.sql"
    sed -i "s|ELSE -CAST(quantity AS BIGINT) END|ELSE CAST(quantity AS BIGINT) END|" "$d/g.sql"
    ./tests/gold-behaviour.sh "$d/g.sql"'
  expect "gold spark check catches an inverted sign" 1 bash -c '
    d=$(mktemp -d); cp -r jobs "$d/jobs"
    sed -i "s|.otherwise(-col(\"quantity\"))|.otherwise(col(\"quantity\"))|" "$d/jobs/spark-delta/gold_open_positions.py"
    docker run --rm -u 0 -v "$PWD:/w" -v "$d:/j" -w /w -e JOBS_ROOT=/j/jobs --entrypoint python3 \
      $ECR/spark-delta:latest \
      /w/tests/gold_fold_test.py delta'

  echo "== slow: SCD2 close-out BEHAVIOUR (not just that it compiles) =="
  # The compile checks prove the SQL plans. This proves it is right, it runs the real
  # close-out over fixed account changes and asserts the validity ranges, including the
  # invariant that matters: exactly one current row per account. The first implementation
  # compiled and planned cleanly and produced two current rows for an out-of-order arrival.
  expect "scd2 close-out produces correct validity ranges" 0 ./tests/scd2-behaviour.sh

  echo "== meta: the SQL checker must catch the two bugs that cost a day =="
  # #85, schema-valid, runtime-invalid: first-row cannot be stream-read with 'none'
  expect "catches changelog-producer=none" 1 bash -c '
    d=$(mktemp -d); cp jobs/flink-paimon/*.sql "$d/"
    sed -i "s|=\s*.lookup.,|= \x27none\x27,|" "$d/create_tables.sql"
    JOBS_DIR="$d" ./infra/aws/scripts/validate-flink-sql.sh'
  # #80, an append-only sink fed by a GROUP BY. The SQL is written by python so the
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
  # first_row + delete.behavior=allow, verified in the Fluss source. This is the
  # class the offline checks cannot see at all.
  expect "catches a server-rejected table option" 1 bash -c '
    d=$(mktemp -d); cp jobs/flink-fluss/*.sql "$d/"
    sed -i "s|.table.merge-engine.  *= .first_row.,|'table.merge-engine' = 'first_row', 'table.delete.behavior' = 'allow',|" "$d/create_tables.sql"
    JOBS_DIR="$d" ./infra/aws/scripts/validate-fluss-sql.sh'

else
  echo "  (skipping Docker checks, run with --all)"
fi

echo
echo "════ $PASS passed, $FAIL failed ════"
if [ "$FAIL" -gt 0 ]; then for p in "${PROBLEMS[@]}"; do echo "  - $p"; done; exit 1; fi
echo "all checks green"
