"""Every S3 prefix the manifests write to must be WIPED or deliberately PRESERVED.

clean_lake is what makes a run a fresh benchmark. It was written as a hand-maintained
list of prefixes, so a manifest that starts writing somewhere new is simply not covered, 
silently. That happened with _flink_ha and _flink_savepoints: both were added with the
Flink HA config and neither was ever wiped, so their objects accrued across every run.

This reads the prefixes straight out of infra/aws/k8s/*.yaml and asserts each one is
accounted for, either in the wipe loops or in the explicit preserve list below.
"""
import pathlib, re, sys

# Prefixes that must SURVIVE a clean. Each needs a reason.
PRESERVE = {
    "tfstate":    "terraform state, destroying it orphans the whole run",
    "teardown":   "the kill-switch bundle CodeBuild destroys from",
    "benchmarks": "results archive; the point is that it outlives the cluster",
}

wf = pathlib.Path(".github/workflows/eks-run.yml").read_text()
step = re.search(r"name: Clean lake.*?(?=\n      - name: )", wf, re.S)
if not step:
    print("  FAIL  no 'Clean lake' step found in the workflow"); sys.exit(1)
wiped = set()
for m in re.finditer(r"for p in ([A-Za-z0-9_ .-]+); do", step.group(0)):
    wiped.update(m.group(1).split())

used = set()
for f in sorted(pathlib.Path("infra/aws/k8s").glob("*.yaml")):
    for m in re.finditer(r"s3a?://\$\{(?:PAIMON_BUCKET|WAREHOUSE_BUCKET)\}/([A-Za-z0-9_.-]+)",
                         f.read_text()):
        used.add(m.group(1))

fails = []
for prefix in sorted(used):
    if prefix in wiped:
        print(f"  PASS  {prefix:20} wiped by clean_lake")
    elif prefix in PRESERVE:
        print(f"  PASS  {prefix:20} preserved, {PRESERVE[prefix]}")
    else:
        print(f"  FAIL  {prefix:20} written by a manifest but neither wiped nor preserved")
        fails.append(prefix)

if fails:
    print(f"\n{len(fails)} prefix(es) unaccounted for: " + ", ".join(fails)
          + "\nadd them to the clean_lake wipe, or to PRESERVE here with a reason")
    sys.exit(1)
print(f"\nall {len(used)} manifest S3 prefixes are accounted for")
