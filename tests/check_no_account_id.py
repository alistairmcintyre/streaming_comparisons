"""No AWS account id may be committed.

An account id is not a credential, but it identifies the account and a fork cannot use
a repo that hardcodes someone else's. There is exactly one place it is configured:
env/aws.env (gitignored) for local work, and the AWS_ACCOUNT_ID repository variable for
CI. scripts/ecr-env.sh is the only thing that derives anything from it.

Run from the repo root. Scans tracked files only, so an untracked env/aws.env is out of
scope by construction.
"""
import re
import subprocess
import sys

PLACEHOLDER = "000000000000"          # used by validate-against-kind.sh on purpose
PATTERNS = [
    (re.compile(r"\b(\d{12})\.dkr\.ecr\."), "ECR registry"),
    (re.compile(r"arn:aws:iam::(\d{12}):"), "IAM ARN"),
    (re.compile(r"aws_account_id\s*=\s*\"(\d{12})\""), "terraform default"),
    (re.compile(r"^\s*ACCOUNT\s*:\s*\"?(\d{12})\"?"), "workflow env"),
]

def main() -> int:
    files = subprocess.run(["git", "ls-files"], capture_output=True, text=True,
                           check=True).stdout.split()
    bad = []
    for f in files:
        if f == "env/aws.env":
            continue
        try:
            text = open(f, encoding="utf-8", errors="ignore").read()
        except (OSError, IsADirectoryError):
            continue
        for i, line in enumerate(text.split("\n"), 1):
            for pat, what in PATTERNS:
                m = pat.search(line)
                if m and m.group(1) != PLACEHOLDER:
                    bad.append(f"{f}:{i}  hardcoded account id in {what}\n    {line.strip()[:100]}")
    if bad:
        print("account id must not be committed:")
        for b in bad:
            print("  " + b)
        print("\nconfigure it in env/aws.env (local) or the AWS_ACCOUNT_ID repository")
        print("variable (CI); scripts/ecr-env.sh derives the registry from it.")
        return 1
    print(f"no hardcoded account id in {len(files)} tracked files")
    return 0

if __name__ == "__main__":
    sys.exit(main())
