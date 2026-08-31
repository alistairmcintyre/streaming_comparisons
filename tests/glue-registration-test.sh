#!/usr/bin/env bash
# Does register-glue-tables.sh build VALID Glue tables? Locally, for nothing, re-runnably.
#
# WHY THIS EXISTS: the script registered paimon and fluss with "Columns": [], and Athena
# reads an Iceberg table's columns FROM THE GLUE DEFINITION — so `SELECT count(*)` worked
# while `SELECT *` failed with "Relation contains no accessible columns". Three of five
# engines were effectively unqueryable and it was found on a live cluster, late, by hand.
# A column-count assertion would have caught it in seconds.
#
# WHAT THIS DOES *NOT* TEST, and must never be claimed to:
#   floci's Athena is a DuckDB sidecar, NOT Trino, and it is not wired to the Glue catalog
#   (`SELECT ... FROM gold.open_positions_delta` -> `Catalog Error: Table does not exist`).
#   So NOTHING here says whether real Athena can read Delta deletion vectors, Hudi's
#   timeline, Paimon's Iceberg-compat metadata or Iceberg MOR deletes. Every one of those
#   was settled against real AWS and none of it is reproducible here.
#   This tests the SHAPE OF OUR API CALLS. That is a real class of bug — it is the class
#   that actually bit — but it is not engine compatibility.
set -uo pipefail
cd "$(dirname "$0")/.."
IMG="${FLOCI_IMAGE:-floci/floci:latest}"
NAME="floci-glue-$$"
PORT="${FLOCI_PORT:-4599}"
cleanup() { docker rm -f "$NAME" >/dev/null 2>&1; }
trap cleanup EXIT

docker image inspect "$IMG" >/dev/null 2>&1 || docker pull "$IMG" >/dev/null 2>&1 || {
  echo "  floci image unavailable — skipping"; exit 0; }
docker run -d --name "$NAME" -p "${PORT}:4566" "$IMG" >/dev/null 2>&1
for _ in $(seq 1 25); do
  curl -sf --max-time 2 "http://localhost:${PORT}/_localstack/health" >/dev/null 2>&1 && break
  sleep 2
done

export AWS_ENDPOINT_URL="http://localhost:${PORT}"
export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test
export AWS_REGION=eu-west-1 AWS_DEFAULT_REGION=eu-west-1
unset AWS_PROFILE AWS_SESSION_TOKEN
export WAREHOUSE_BUCKET=wh PAIMON_BUCKET=pm

for b in wh pm; do
  aws s3api create-bucket --bucket "$b" \
    --create-bucket-configuration LocationConstraint=eu-west-1 >/dev/null 2>&1
done

# Seed exactly what the script gates on: a _delta_log for Delta, and REAL Iceberg metadata
# for paimon/fluss — the columns it registers are derived from that file, so a fake one
# also exercises glue_columns_from_metadata.
TMP=$(mktemp -d)
python3 - "$TMP" <<'PYEOF'
import json, sys, pathlib
sys.path.insert(0, "jobs/_shared")
import schemas
I = {"bigint":"long","int":"int","string":"string","boolean":"boolean","timestamp":"timestamp"}
def meta(cols):
    return {"format-version":2,"table-uuid":"00000000-0000-0000-0000-000000000000",
            "location":"s3://pm/x","last-updated-ms":0,"last-column-id":len(cols),
            "current-schema-id":0,"default-spec-id":0,"default-sort-order-id":0,
            "last-partition-id":999,"current-snapshot-id":-1,"refs":{},"snapshots":[],
            "partition-specs":[{"spec-id":0,"fields":[]}],
            "sort-orders":[{"order-id":0,"fields":[]}],"properties":{},
            "schemas":[{"type":"struct","schema-id":0,"fields":[
                {"id":i,"name":n,"required":False,
                 "type":(t if t.startswith("decimal") else I.get(t,"string"))}
                for i,(n,t) in enumerate(cols)]}]}
out = pathlib.Path(sys.argv[1])
for rel, canon in [("paimon/iceberg/bronze/trades","BRONZE_TRADES"),
                   ("paimon/iceberg/silver/trades","SILVER_TRADES"),
                   ("paimon/iceberg/silver/accounts","SILVER_ACCOUNTS"),
                   ("paimon/iceberg/gold/open_positions","GOLD_OPEN_POSITIONS"),
                   ("fluss/paimon/iceberg/silver/trades","SILVER_TRADES"),
                   ("fluss/paimon/iceberg/gold/open_positions","GOLD_OPEN_POSITIONS")]:
    d = out / rel / "metadata"; d.mkdir(parents=True, exist_ok=True)
    (d / "v1.metadata.json").write_text(json.dumps(meta(getattr(schemas, canon))))
PYEOF
for p in bronze/trades silver/trades silver/accounts gold/open_positions; do
  echo '{"commitInfo":{}}' | aws s3 cp - "s3://wh/delta/${p}/_delta_log/00000000000000000000.json" >/dev/null 2>&1
done
aws s3 cp "$TMP/paimon" s3://pm/paimon --recursive >/dev/null 2>&1
aws s3 cp "$TMP/fluss"  s3://pm/fluss  --recursive >/dev/null 2>&1
rm -rf "$TMP"

echo "== running register-glue-tables.sh against floci =="
./infra/aws/scripts/register-glue-tables.sh 2>&1 | sed 's/^/    /' | grep -E "registered|FAILED|skip" | head -20

echo "== assertions =="
python3 - <<'PYEOF'
import json, subprocess, sys
sys.path.insert(0, "jobs/_shared")
import schemas
EXPECT = [
    ("bronze","trades_delta","BRONZE_TRADES","delta"),
    ("silver","trades_delta","SILVER_TRADES","delta"),
    ("silver","accounts_delta","SILVER_ACCOUNTS","delta"),
    ("gold","open_positions_delta","GOLD_OPEN_POSITIONS","delta"),
    ("bronze","trades_paimon","BRONZE_TRADES","ICEBERG"),
    ("silver","trades_paimon","SILVER_TRADES","ICEBERG"),
    ("silver","accounts_paimon","SILVER_ACCOUNTS","ICEBERG"),
    ("gold","open_positions_paimon","GOLD_OPEN_POSITIONS","ICEBERG"),
    ("silver","trades_fluss","SILVER_TRADES","ICEBERG"),
    ("gold","open_positions_fluss","GOLD_OPEN_POSITIONS","ICEBERG"),
]
fails = []
def chk(d, ok, detail=""):
    print(("  PASS  " if ok else "  FAIL  ") + d + (f"  {detail}" if detail else ""))
    if not ok: fails.append(d)
for db, tbl, canon, ttype in EXPECT:
    r = subprocess.run(["aws","glue","get-table","--database-name",db,"--name",tbl],
                       capture_output=True, text=True)
    if r.returncode != 0:
        chk(f"{db}.{tbl} registered", False, r.stderr.strip().splitlines()[-1][:80] if r.stderr else "")
        continue
    t = json.loads(r.stdout)["Table"]
    got = [(c["Name"], c["Type"]) for c in t["StorageDescriptor"].get("Columns", [])]
    want = getattr(schemas, canon)
    chk(f"{db}.{tbl} has the canonical {len(want)} columns", got == want,
        "" if got == want else f"got {len(got)}: {[c for c,_ in got][:4]}...")
    chk(f"{db}.{tbl} table_type={ttype}",
        t.get("Parameters", {}).get("table_type") == ttype,
        f"got {t.get('Parameters',{}).get('table_type')!r}")
    if ttype == "delta":
        chk(f"{db}.{tbl} declares the native Delta provider",
            t.get("Parameters", {}).get("spark.sql.sources.provider") == "delta")
print(("\nregister-glue-tables.sh builds valid Glue tables" if not fails
       else f"\n{len(fails)} assertion(s) failed"))
sys.exit(1 if fails else 0)
PYEOF
