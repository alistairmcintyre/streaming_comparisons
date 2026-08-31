"""Every Spark write must be pinned to jobs/_shared/schemas.py — one way or the other.

A table's DDL constrains the TABLE. It does not constrain the DataFrame written to it, and
the two are only kept in step by hand. That gap is what crash-looped iceberg-bronze-trades
on a live cluster:

    Cannot write incompatible dataset ... source_lsn is out of order, before kafka_partition

The job emitted source_lsn between event_ts and ingest_ts; the DDL put it last. DELTA
NEVER NOTICED, because it appends by column NAME — Iceberg matches by POSITION. So the
same defect sat in the Delta job too, invisible until pointed at a positional writer.

Spark has two write shapes here and each needs a different guarantee:

  POSITIONAL APPEND (writeStream to a path/table)   -> must project through conform(),
      which selects and casts to the canon, so order and types cannot drift.
  MERGE with an explicit INSERT column list         -> order-independent by construction,
      but the LIST must still name the canon's columns in its order, or the table and the
      canon have silently diverged.

FLINK IS NOT CHECKED HERE, deliberately. Its INSERT INTO ... SELECT is also positional, but
Flink resolves and TYPE-CHECKS it at planning time, and validate-flink-sql.sh /
validate-fluss-sql.sh compile every SQL file against a real planner on every run. A
column-count or type mismatch there is a compile error, not a runtime surprise. Spark's
writeStream is the one that defers the check until a micro-batch fails in production.
"""
import ast, pathlib, re, sys
sys.path.insert(0, "jobs/_shared")
import schemas

TABLE_FOR = {
    "bronze_trades": "BRONZE_TRADES", "silver_trades": "SILVER_TRADES",
    "silver_accounts": "SILVER_ACCOUNTS", "gold_open_positions": "GOLD_OPEN_POSITIONS",
}
fails = []
def chk(desc, ok, detail=""):
    print(("  PASS  " if ok else "  FAIL  ") + desc + (f"  {detail}" if detail else ""))
    if not ok:
        fails.append(desc)

for job in sorted(pathlib.Path("jobs").glob("spark-*/*.py")):
    want_name = TABLE_FOR.get(job.stem)
    if not want_name:
        continue
    src = job.read_text()
    tree = ast.parse(src)
    label = f"{job.parent.name}/{job.name}"

    # local name -> schemas name, so an alias (SILVER_TRADES_FIELDS) still resolves
    alias = {}
    for n in ast.walk(tree):
        if isinstance(n, ast.ImportFrom) and n.module == "schemas":
            for a in n.names:
                alias[a.asname or a.name] = a.name

    def schema_arg(call):
        """Which canon does this conform() call pin to?

        conform(df)                              -> the gold default
        conform(df, SILVER_TRADES)               -> that name
        conform(df, hive_order(GOLD_..., (...))) -> the name inside hive_order, which
            reorders partition columns last for Hive/Athena but keeps the same fields.
        """
        if len(call.args) == 1:
            return "GOLD_OPEN_POSITIONS"
        a = call.args[1]
        if isinstance(a, ast.Name):
            return a.id
        if (isinstance(a, ast.Call) and isinstance(a.func, ast.Name)
                and a.func.id == "hive_order" and a.args and isinstance(a.args[0], ast.Name)):
            return a.args[0].id
        return None

    conform_args = [
        schema_arg(c) for c in ast.walk(tree)
        if isinstance(c, ast.Call) and isinstance(c.func, ast.Name) and c.func.id == "conform"
    ]
    resolved = {alias.get(a, a) for a in conform_args if a}

    if "MERGE INTO" in src:
        m = re.search(r"WHEN NOT MATCHED.*?INSERT\s*\(([^)]*)\)", src, re.S)
        if not m:
            chk(f"{label}: MERGE has an explicit INSERT column list", False)
            continue
        cols = [c.strip() for c in m.group(1).replace("\n", " ").split(",") if c.strip()]
        canon = [c for c, _ in getattr(schemas, want_name)]
        chk(f"{label}: MERGE INSERT list == {want_name}", cols == canon,
            "" if cols == canon else f"got {cols}")
    else:
        chk(f"{label}: conforms to {want_name}", want_name in resolved,
            "" if want_name in resolved else f"conform() called with {sorted(resolved) or 'nothing'}")

print(("\nevery Spark write path is pinned to the canon" if not fails
       else f"\n{len(fails)} write path(s) not pinned — a DDL change can diverge silently"))
sys.exit(1 if fails else 0)
