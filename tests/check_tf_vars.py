"""Every place that runs terraform must supply the variables that have no default.

Removing a variable's default is safe in the root module and quietly unsafe everywhere
that invokes it. aws_account_id lost its default so the account would stop being
hardcoded, which broke the EventBridge kill-switch: it runs `terraform destroy` inside a
CodeBuild project with no workflow env and no env/aws.env around it, so the 2.5h
dead-man's switch would have failed on "No value for required variable" and a run could
have billed until someone noticed by hand.

Checks that each terraform caller (the two workflows, and the kill-switch buildspec)
provides TF_VAR_<name> for every variable declared without a default.
"""
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
TF_DIR = ROOT / "infra" / "aws"
# Callers that run terraform against infra/aws, and where each sets its variables.
CALLERS = [
    ROOT / ".github" / "workflows" / "eks-run.yml",
    ROOT / ".github" / "workflows" / "teardown.yml",
    TF_DIR / "killswitch.tf",
]

def required_vars(path: Path) -> set:
    """Variables declared with no default, so terraform will demand a value."""
    text = "\n".join(p.read_text() for p in path.glob("*.tf"))
    out = set()
    for m in re.finditer(r'variable\s+"([a-z0-9_]+)"\s*\{(.*?)\n\}', text, re.S):
        name, body = m.group(1), m.group(2)
        if not re.search(r"^\s*default\s*=", body, re.M):
            out.add(name)
    return out

def main() -> int:
    need = required_vars(TF_DIR)
    if not need:
        print("no variables without defaults; nothing to check")
        return 0
    bad = []
    for caller in CALLERS:
        if not caller.exists():
            bad.append(f"{caller.relative_to(ROOT)} is missing")
            continue
        text = caller.read_text()
        for v in sorted(need):
            # supplied as TF_VAR_<name>, or passed explicitly as -var "<name>=..."
            if f"TF_VAR_{v}" not in text and not re.search(rf'-var\s+"?{v}=', text):
                bad.append(f"{caller.relative_to(ROOT)} never supplies {v} "
                           f"(TF_VAR_{v} or -var {v}=...)")
    if bad:
        print("terraform callers missing a required variable:")
        for b in bad:
            print("  " + b)
        return 1
    print(f"all {len(CALLERS)} terraform callers supply "
          f"{', '.join(sorted(need))}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
