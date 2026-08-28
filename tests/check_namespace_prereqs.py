"""Every namespace the manifests use must EXIST before the server dry-run runs.

`kubectl apply --dry-run=server` creates nothing. So if 00-namespaces.yaml is only
dry-run, every resource living in one of its namespaces is rejected with

    Error from server (NotFound): namespaces "fluss" not found

and the gate reports three "REJECTED" manifests that have nothing wrong with them. That
happened on a real run: `fluss` was declared in 00-namespaces.yaml and created nowhere
else, so 00/20/60-*.yaml all failed, the run aborted, and the cluster was torn down for a
defect that did not exist.

The kind pre-flight has always created namespaces for real for this reason — but it only
proves the MANIFESTS are valid, never that the workflow's apply ORDER is. This closes that
gap statically: a namespace used by any manifest must be created for real by the workflow
before it dry-runs anything.
"""
import pathlib, re, sys

wf = pathlib.Path(".github/workflows/eks-run.yml").read_text()
manifests = sorted(pathlib.Path("infra/aws/k8s").glob("*.yaml"))

used = {}
for f in manifests:
    for ns in set(re.findall(r"^\s*namespace:\s*([a-z0-9-]+)", f.read_text(), re.M)):
        used.setdefault(ns, []).append(f.name)

created = set(re.findall(r"kubectl create ns ([a-z0-9-]+)", wf))
created |= set(re.findall(r"-n ([a-z0-9-]+) --create-namespace", wf))
created |= set(re.findall(r"--create-namespace -n ([a-z0-9-]+)", wf))

# 00-namespaces.yaml counts only if it is applied FOR REAL (no --dry-run on that line).
for line in wf.split("\n"):
    if "00-namespaces.yaml" in line and "kubectl apply" in line and "--dry-run" not in line:
        decl = pathlib.Path("infra/aws/k8s/00-namespaces.yaml").read_text()
        blocks = decl.split("---")
        for b in blocks:
            if re.search(r"kind:\s*Namespace", b):
                m = re.search(r"name:\s*([a-z0-9-]+)", b)
                if m:
                    created.add(m.group(1))
        break

fails = []
for ns in sorted(used):
    if ns in created:
        print(f"  PASS  {ns:12} created before the dry-run")
    else:
        print(f"  FAIL  {ns:12} used by {', '.join(sorted(used[ns]))} but never created for real")
        fails.append(ns)

print(("\nevery namespace exists before the dry-run" if not fails
       else f"\n{len(fails)} namespace(s) only ever dry-run: " + ", ".join(fails)
            + "\nthe dry-run will report their manifests REJECTED with 'namespaces ... not found'"))
sys.exit(1 if fails else 0)
