"""The five pipelines must write the SAME FIELDS, or the benchmark compares different tables.

Checks every DECLARED schema — Delta and Iceberg (Python/SQL DDL in jobs/_shared) and
Paimon and Fluss (CREATE TABLE in each engine's create_tables.sql) — against the single
definition in jobs/_shared/schemas.py, for silver.trades, silver.accounts and
gold.open_positions.

HUDI IS NOT HERE, because it has no DDL: a Hudi table's schema is whatever DataFrame gets
written to it. It is pinned instead by conform() at each write site, and MEASURED on the
real table by tests/gold_fold_test.py. That distinction is not academic — Hudi's gold
net_notional had inferred to decimal(33,4) against the decimal(38,4) the other four
declare, and no value-based check could see it.

Nothing enforced any of this before: validate-run.sh's "gold schema parity" check only
does an `aws s3 ls` to see whether the Delta table exists.
"""
import pathlib, re, sys
sys.path.insert(0, "jobs/_shared")
from schemas import BRONZE_TRADES, GOLD_OPEN_POSITIONS, SILVER_TRADES, SILVER_ACCOUNTS

DELTA_TYPES = {"LongType": "bigint", "StringType": "string", "IntegerType": "int",
               "TimestampType": "timestamp", "BooleanType": "boolean"}
# Flink spells microsecond timestamps TIMESTAMP(6); Spark just says TIMESTAMP.
NORM = {"timestamp(6)": "timestamp"}


def from_delta(func):
    """Columns of a create_* function in delta_tables.py, from its StructType."""
    src = pathlib.Path("jobs/_shared/delta_tables.py").read_text()
    body = re.search(rf"def {func}\(spark\):(.*?)\n\ndef ", src + "\n\ndef ", re.S).group(1)
    block = re.search(r"schema = StructType\(\[(.*?)\n    \]\)", body, re.S).group(1)
    out = []
    for name, typ, args in re.findall(
            r'StructField\("(\w+)",\s*(\w+)(\([^)]*\))?', block):
        if typ == "DecimalType":
            p, sc = re.findall(r"\d+", args)
            out.append((name, f"decimal({p},{sc})"))
        else:
            out.append((name, DELTA_TYPES[typ]))
    return out


def _split_top(text):
    """Split on commas at paren depth 0. A naive split breaks DECIMAL(12,4) in half —
    which is exactly what the first version of this parser did, reporting six false
    failures against schemas that were in fact correct."""
    parts, depth, cur = [], 0, ""
    for ch in text:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        if ch == "," and depth == 0:
            parts.append(cur); cur = ""
        else:
            cur += ch
    parts.append(cur)
    return [p.strip() for p in parts if p.strip()]


def from_sql(path, table):
    """Columns of a CREATE TABLE, for both the Iceberg f-strings and the Flink SQL."""
    text = pathlib.Path(path).read_text()
    m = re.search(r"CREATE TABLE IF NOT EXISTS " + re.escape(table)
                  + r"\s*\((.*?)\n?\s*\)?\s*(?:USING iceberg|WITH \()", text, re.S)
    if not m:
        return None
    # Drop comments and the PRIMARY KEY clause, then split the column list properly.
    body = "\n".join(l for l in m.group(1).split("\n")
                     if l.strip() and not l.strip().startswith("--")
                     and not l.strip().upper().startswith("PRIMARY KEY"))
    out = []
    for part in _split_top(body):
        bits = part.split(None, 1)
        if len(bits) != 2 or not re.match(r"^\w+$", bits[0]):
            continue
        typ = re.sub(r"\s+NOT NULL$", "", bits[1].strip(), flags=re.I)
        typ = re.sub(r"\s+", "", typ).lower().rstrip(")")
        if typ.count("(") > typ.count(")"):
            typ += ")"
        out.append((bits[0], NORM.get(typ, typ)))
    return out


ICE, PAI, FLU = ("jobs/_shared/iceberg_tables.py",
                 "jobs/flink-paimon/create_tables.sql", "jobs/flink-fluss/create_tables.sql")
CASES = [
    # FLUSS IS ABSENT FROM bronze BY DESIGN, not by omission: it has no bronze hop — its
    # PK table is the cleaned landing table, one hop fewer, recorded as `hops` in
    # results.json. Inventing one to make the table counts match would misrepresent it.
    ("bronze.trades", BRONZE_TRADES, [
        ("delta",   lambda: from_delta("create_bronze_trades")),
        ("iceberg", lambda: from_sql(ICE, "{_CAT}.bronze.trades_spark")),
        ("paimon",  lambda: from_sql(PAI, "paimon.bronze.trades"))]),
    ("silver.trades", SILVER_TRADES, [
        ("delta",   lambda: from_delta("create_silver_trades")),
        ("iceberg", lambda: from_sql(ICE, "{_CAT}.silver.trades_spark")),
        ("paimon",  lambda: from_sql(PAI, "paimon.silver.trades")),
        ("fluss",   lambda: from_sql(FLU, "fluss_catalog.silver.trades"))]),
    ("silver.accounts", SILVER_ACCOUNTS, [
        ("delta",   lambda: from_delta("create_silver_accounts")),
        ("iceberg", lambda: from_sql(ICE, "{_CAT}.silver.accounts_spark")),
        ("paimon",  lambda: from_sql(PAI, "paimon.silver.accounts")),
        ("fluss",   lambda: from_sql(FLU, "fluss_catalog.silver.accounts"))]),
    ("gold.open_positions", GOLD_OPEN_POSITIONS, [
        ("delta",   lambda: from_delta("create_gold_open_positions")),
        ("iceberg", lambda: from_sql(ICE, "{_CAT}.gold.open_positions_spark")),
        ("paimon",  lambda: from_sql(PAI, "paimon.gold.open_positions")),
        ("fluss",   lambda: from_sql(FLU, "fluss_catalog.gold.open_positions"))]),
]

fails = []
for table, canon, engines in CASES:
    for engine, get in engines:
        try:
            got = get()
        except Exception as e:
            print(f"  FAIL  {table:22} {engine:8} could not parse its DDL: {e}")
            fails.append(f"{table}/{engine}"); continue
        if got == canon:
            print(f"  PASS  {table:22} {engine:8} {len(canon)} fields")
            continue
        print(f"  FAIL  {table:22} {engine:8}")
        fails.append(f"{table}/{engine}")
        want, have = dict(canon), dict(got or [])
        for n, t in canon:
            if have.get(n) != t:
                print(f"            {n}: got {have.get(n, '<missing>')}  want {t}")
        for n, t in (got or []):
            if n not in want:
                print(f"            {n}: EXTRA ({t})")
        if [c for c, _ in (got or [])] != [c for c, _ in canon] and set(have) == set(want):
            print(f"            order differs: {[c for c,_ in got]}")

print(("\nall declared schemas match jobs/_shared/schemas.py" if not fails
       else f"\nSCHEMA PARITY BROKEN in {len(fails)}: " + ", ".join(fails)))
sys.exit(1 if fails else 0)
