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

for f in fails:
    print("  FAIL  " + f)
if fails:
    print(f"\n{len(fails)} workflow run: block(s) are not valid shell")
    sys.exit(1)
print("  every workflow run: block is valid shell")
