"""Post-run steps must be gated so a failed apply does not bill for work it cannot do.

The run job provisions a real EKS cluster before it applies any manifest, so once apply
fails the cluster EXISTS and is billing until teardown. What runs between the failure and
`terraform destroy` is therefore money.

Four rules, each of which has a concrete cost if broken:

  sleep         must not be always(), otherwise a failed apply still sleeps run_minutes
                (30-120 minutes of a live cluster doing nothing).
  quiesce /     must be gated on the apply succeeding. There is no load to drain and no
  register /    table to snapshot if the manifests never applied, and quiesce polls
  snapshot      consumer lag for QUIESCE_TIMEOUT (600s) + a 90s settle before giving up, 
                ~11 minutes of billing, ahead of teardown, for nothing.
  diagnostics   must be always(). It is the evidence that makes destroy_mode=always safe
                to run; skipping it on failure is exactly when it is needed.
  destroy       must be always()-based. Anything else risks leaving a cluster billing to
                the 2.5h kill switch.
"""
import re, sys, yaml

job = yaml.safe_load(open(".github/workflows/eks-run.yml"))["jobs"]["run"]
steps = job["steps"]
by_name = {str(s.get("name", "")): s for s in steps}

def find(pat):
    for s in steps:
        if re.search(pat, str(s.get("name", "")), re.I):
            return s
    return None

fails = []
def chk(desc, ok, detail=""):
    print(("  PASS  " if ok else "  FAIL  ") + desc + (f"  {detail}" if detail else ""))
    if not ok:
        fails.append(desc)

apply_step = find(r"apply manifests")
chk("the apply step carries id: apply", bool(apply_step) and apply_step.get("id") == "apply")
GATE = "steps.apply.outcome == 'success'"

sleep_step = find(r"^Run for ")
cond = str((sleep_step or {}).get("if", ""))
chk("the run/sleep step is not always()", bool(sleep_step) and "always()" not in cond,
    f"if={cond or '<default success()>'}")

for pat, label in [(r"Quiesce", "quiesce"), (r"Register .*Glue", "glue registration"),
                   (r"Snapshot correctness", "snapshot")]:
    st = find(pat)
    cond = str((st or {}).get("if", ""))
    chk(f"{label} is gated on a successful apply", bool(st) and GATE in cond, f"if={cond}")

for pat, label in [(r"Collect diagnostics", "diagnostics"), (r"Upload diagnostics", "upload")]:
    st = find(pat)
    cond = str((st or {}).get("if", ""))
    chk(f"{label} still runs on failure", bool(st) and "always()" in cond, f"if={cond}")

destroy = find(r"Terraform destroy")
cond = str((destroy or {}).get("if", ""))
chk("destroy still runs on failure", bool(destroy) and "always()" in cond, f"if={cond}")

print("\npost-run gating is correct" if not fails
      else f"\n{len(fails)} gating problem(s), each one bills a live cluster")
sys.exit(1 if fails else 0)
