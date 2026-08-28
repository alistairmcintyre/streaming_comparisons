"""Every `run:` block in a workflow must be valid SHELL, not just valid YAML.

The existing "workflows are valid YAML" check parses the file. It cannot see inside a
run: block, and that is exactly where a workflow breaks:

    for c in "a:${{ steps.a.outcome }}" \\
             "b:${{ steps.b.outcome }}"; do

A doubled backslash is an ESCAPED BACKSLASH, so the line does not continue: the for-list
ends, the next line starts a new command, and bash dies with `syntax error near
unexpected token`. The YAML is perfectly valid throughout. This shipped, and the failure
appeared only when the validate job actually ran.

${{ }} expressions are substituted with a placeholder first — GitHub expands them before
bash ever sees the script, so leaving them in would test the wrong grammar.

SECOND CHECK: `[ $? -ne 0 ]` after a bare `VAR=$(cmd)`.

GitHub runs these blocks as `bash --noprofile --norc -eo pipefail`, so a failing command
substitution in an assignment IS a failing command and the shell exits THERE. Any test of
$? on a later line is unreachable on the path it was written for. The apply step's
server-dry-run loop captured kubectl's output into R and then checked $? — so a rejected
manifest exited the step with the header printed and the rejection reason discarded, which
is the least debuggable possible outcome. Use `cmd && RC=0 || RC=$?` instead: the ||
suppresses set -e and RC carries the real status.

(shellcheck would flag this as SC2181, but it is not installed in this environment, so the
check is targeted rather than general.)
"""
import pathlib, re, subprocess, sys, yaml

fails = []
for wf in sorted(pathlib.Path(".github/workflows").glob("*.y*ml")):
    doc = yaml.safe_load(wf.read_text())
    for jname, job in (doc.get("jobs") or {}).items():
        for i, step in enumerate(job.get("steps") or []):
            run = step.get("run")
            if not run:
                continue
            shell = step.get("shell", "bash")
            if shell not in ("bash", "sh"):
                continue
            script = re.sub(r"\$\{\{[^}]*\}\}", "X", run)
            p = subprocess.run([shell, "-n"], input=script, text=True, capture_output=True)
            if p.returncode != 0:
                err = (p.stderr.strip().splitlines() or ["?"])[0]
                fails.append(f"{wf.name} :: {jname} :: step {i} "
                             f"({step.get('name', 'unnamed')!r}) — {err}")
            for ln, line in enumerate(script.split("\n"), 1):
                # Ignore comments — the fix for this very bug is documented in a comment
                # containing the offending idiom, and the first version of this lint
                # flagged its own explanation.
                code = line.split("#", 1)[0]
                if re.search(r"\[\[? +\$\? ", code):
                    fails.append(f"{wf.name} :: {jname} :: step {i} "
                                 f"({step.get('name', 'unnamed')!r}) line {ln} — tests $? under "
                                 f"`set -e`, which never runs after a failing assignment; "
                                 f"use `cmd && RC=0 || RC=$?`")

for f in fails:
    print("  FAIL  " + f)
if fails:
    print(f"\n{len(fails)} workflow run: block(s) are not valid shell")
    sys.exit(1)
print("  every workflow run: block is valid shell")
