"""All five gold tables must return the SAME FIELDS. Fast half: the Flink DDL.

The five pipelines are only comparable if their gold tables have the same columns in the
same order with the same types. Nothing enforced that: validate-run.sh's "gold schema
parity" check only does an `aws s3 ls` to see whether the Delta table exists, and the
fold tests compare VALUES, which agree happily across differently-typed columns.

This half parses the two Flink CREATE TABLE statements, which declare their schema, and
checks them against jobs/_shared/gold_schema.py. The Spark half is measured rather than
parsed — tests/gold_fold_test.py asserts the schema of the table it actually builds, which
is the only way to cover Hudi (no DDL: its schema is whatever DataFrame gets written).
"""
import pathlib, re, sys
sys.path.insert(0, "jobs/_shared")
from gold_schema import GOLD_OPEN_POSITIONS

# Flink spellings -> the canonical (Spark) spelling of the same type.
NORM = {"timestamp(6)": "timestamp", "timestamp(3)": "timestamp(3)"}
SOURCES = [
    ("paimon", "jobs/flink-paimon/create_tables.sql", "paimon.gold.open_positions"),
    ("fluss",  "jobs/flink-fluss/create_tables.sql",  "fluss_catalog.gold.open_positions"),
]

fails = []
def chk(d, ok):
    print(("  PASS  " if ok else "  FAIL  ") + d)
    if not ok: fails.append(d)

for engine, path, table in SOURCES:
    sql = pathlib.Path(path).read_text()
    m = re.search(r"CREATE TABLE IF NOT EXISTS " + re.escape(table) + r"\s*\((.*?)\n\)\s*WITH",
                  sql, re.S)
    if not m:
        chk(f"{engine}: found the gold CREATE TABLE", False); continue
    cols = []
    for line in m.group(1).split("\n"):
        line = line.strip().rstrip(",")
        if not line or line.startswith("--") or line.upper().startswith("PRIMARY KEY"):
            continue
        parts = line.split(None, 1)
        if len(parts) != 2:
            continue
        name, typ = parts[0], parts[1]
        typ = re.sub(r"\s+NOT NULL$", "", typ.strip(), flags=re.I).strip().lower()
        typ = re.sub(r"\s+", "", typ)
        cols.append((name, NORM.get(typ, typ)))
    if cols == GOLD_OPEN_POSITIONS:
        chk(f"{engine} gold declares the canonical {len(cols)} fields", True)
    else:
        chk(f"{engine} gold declares the canonical fields", False)
        want = dict(GOLD_OPEN_POSITIONS); got = dict(cols)
        for n, t in GOLD_OPEN_POSITIONS:
            if got.get(n) != t:
                print(f"          {n}: got {got.get(n, '<missing>')}  want {t}")
        for n, t in cols:
            if n not in want:
                print(f"          {n}: EXTRA column ({t})")
        if [c for c, _ in cols] != [c for c, _ in GOLD_OPEN_POSITIONS]:
            print(f"          order: got {[c for c,_ in cols]}")

print(("\ngold field parity holds for the Flink engines" if not fails
       else "\ngold field parity BROKEN: " + "; ".join(fails)))
sys.exit(1 if fails else 0)
