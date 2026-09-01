"""Import hygiene for the pipeline jobs: what they import must exist and mean what it says.

TWO CHECKS.

1. Every shared module a job imports must actually be SHIPPED to the cluster.

The Spark jobs mount jobs/_shared at /opt/shared and import from it by bare module name.
Nothing local catches a missing one: the import resolves fine in the repo, the manifests
apply cleanly, the pods start, and then the driver dies with ModuleNotFoundError.

This caught jobs/_shared/scd2.py, which all three spark-*/silver_accounts.py import and
which the workflow's hand-maintained --from-file list never included.

2. No job may import the SAME NAME from two modules. hudi_tables and schemas both export
BRONZE_TRADES / SILVER_TRADES / SILVER_ACCOUNTS, a PATH in one, a FIELD LIST in the other
so the second import shadows the first and conform(df, "s3a://...") is what
actually runs. Python raises nothing; the failure appears at runtime on the cluster.
"""
import ast, pathlib, re, sys

wf = pathlib.Path(".github/workflows/eks-run.yml").read_text()
shared = pathlib.Path("jobs/_shared")
modules = {p.stem for p in shared.glob("*.py")}

# What the spark-shared configmap actually ships: either the whole directory or a list.
block = re.search(r"create configmap spark-shared(.*?)--dry-run", wf, re.S)
if not block:
    print("  FAIL  no spark-shared configmap found in the workflow"); sys.exit(1)
body = block.group(1)
# The directory form must be the WHOLE argument. Testing for the substring
# "--from-file=jobs/_shared/" also matches "--from-file=jobs/_shared/delta_tables.py",
# so a hand-maintained list would read as "ships everything" and this checker would
# pass on exactly the bug it exists to catch. (It did, until the meta-check caught it.)
ships_dir = bool(re.search(r"--from-file=jobs/_shared/(?=[\s\\]|$)", body, re.M))
shipped = modules if ships_dir else {
    pathlib.Path(m).stem for m in re.findall(r"--from-file=jobs/_shared/(\S+\.py)", body)}

fails = []
for job in sorted(pathlib.Path("jobs").glob("spark-*/*.py")):
    tree = ast.parse(job.read_text())
    for node in ast.walk(tree):
        names = ([a.name for a in node.names] if isinstance(node, ast.Import)
                 else [node.module] if isinstance(node, ast.ImportFrom) and node.level == 0
                 else [])
        for n in names:
            if n and n.split(".")[0] in modules and n.split(".")[0] not in shipped:
                fails.append(f"{job} imports '{n}'. Not in the spark-shared configmap")

# ── 2. shadowed imports ──────────────────────────────────────────────────────
_TREES = {}
def tree_of(path):
    if path not in _TREES:
        _TREES[path] = ast.parse(path.read_text())
    return _TREES[path]

for job in sorted(pathlib.Path("jobs").glob("spark-*/*.py")):
    seen = {}
    for node in ast.walk(tree_of(job)):
        if not isinstance(node, ast.ImportFrom):
            continue
        for alias in node.names:
            bound = alias.asname or alias.name
            if bound in seen and seen[bound] != node.module:
                fails.append(f"{job} imports '{bound}' from both {seen[bound]} and "
                             f"{node.module}, the second silently shadows the first")
            seen[bound] = node.module
    # …and a module-level ASSIGNMENT that shadows an imported name. This is the same bug
    # wearing different clothes, and the import-vs-import check above walked straight past
    # it: spark-delta/silver_trades.py imports SILVER_TRADES (the field list) and then
    # defines SILVER_TRADES = f"{_BASE}/silver/trades" (the path), so conform() would have
    # been handed a string. Caught only because the write then failed.
    for node in ast.walk(tree_of(job)):
        if isinstance(node, ast.Assign) and getattr(node, "col_offset", 1) == 0:
            for t in node.targets:
                if isinstance(t, ast.Name) and t.id in seen:
                    fails.append(f"{job} imports '{t.id}' from {seen[t.id]} and then "
                                 f"reassigns it at module level, the import is dead")

for f in sorted(set(fails)):
    print("  FAIL  " + f)
if fails:
    print(f"\n{len(set(fails))} import problem(s)")
    sys.exit(1)
print(f"  every shared module imported by a Spark job is shipped "
      f"({'whole jobs/_shared/ directory' if ships_dir else str(len(shipped)) + ' listed files'})")
