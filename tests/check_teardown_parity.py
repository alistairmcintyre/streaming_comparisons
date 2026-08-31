"""The kill switch must be at least as capable as the GHA destroy path.

There are two teardown paths: the workflow's `Terraform destroy` step, and the EventBridge
kill switch that fires at apply+150min via CodeBuild (infra/aws/killswitch.tf). The second
is the BACKSTOP — it runs precisely when the first did not, or could not.

It was weaker. Every fix for a real teardown failure went into the workflow and none into
the kill switch, so when the kill switch finally ran in anger it died on:

    DependencyViolation: resource sg-031e4809e47926dc1 has a dependent object

and left a VPC, two subnets, an IGW, an Elastic IP and 190 GB of EBS billing for ~5 hours.
The workflow path had had the fix for that since earlier the same day.

This asserts both paths handle each blocker we have actually hit. It compares capability,
not text, so the two can be written differently — but neither can silently lose one.
"""
import pathlib, re, sys

WF = pathlib.Path(".github/workflows/eks-run.yml").read_text()
KS = pathlib.Path("infra/aws/killswitch.tf").read_text()

# blocker -> a pattern proving the path handles it
CAPABILITIES = {
    "terminate orphaned instances":      r"terminate-instances",
    "delete lingering CNI ENIs":         r"delete-network-interface",
    "POLL for ENIs (they detach late)":  r"seq 1 6|for _ in \$\(seq",
    "delete EFS mount targets":          r"delete-mount-target",
    "clear SG cross-references":         r"revoke-security-group-ingress",
    "sweep orphaned EBS volumes":        r"delete-volume",
    "targeted EKS destroy first":        r"destroy -target=module\.eks",
}
fails = []
print(f"  {'capability':38} {'workflow':>10} {'kill switch':>12}")
for name, pat in CAPABILITIES.items():
    in_wf = bool(re.search(pat, WF))
    in_ks = bool(re.search(pat, KS))
    ok = in_wf and in_ks
    print(f"  {name:38} {'yes' if in_wf else 'NO':>10} {'yes' if in_ks else 'NO':>12}"
          + ("" if ok else "   <- gap"))
    if not ok:
        fails.append(name)

print(("\nboth teardown paths handle every blocker we have hit" if not fails
       else f"\n{len(fails)} capability gap(s) — the backstop must not be weaker than the "
            "path it backs up"))
sys.exit(1 if fails else 0)
