"""Copy eks-run.yml with the first line-continuation backslash DOUBLED.

The meta-test for check_workflow_shell.py needs a workflow carrying exactly the bug that
checker exists to catch: `foo \\` at end of line, where YAML's block scalar turns the
doubled backslash into a literal one and the shell stops seeing a continuation.

It used to be injected by `sed "145s|...|"`, addressed by line number. Adding seven
lines to an input block far above it made line 145 something else entirely, so sed
matched nothing, the copy was pristine, the checker correctly passed it, and the
meta-test failed with "rc=0, wanted 1". The checker had never broken; the fixture had
stopped injecting. Addressing by content removes that whole class of false alarm, and
exiting 99 on no-match means a fixture that stops injecting SAYS SO instead of quietly
turning green.
"""
import sys

src = ".github/workflows/eks-run.yml"
dst = sys.argv[1]
lines = open(src).read().split("\n")
for i, line in enumerate(lines):
    if line.endswith(" \\") and not line.endswith("\\\\"):
        lines[i] = line + "\\"
        break
else:
    print("fixture injected nothing: no single trailing backslash found", file=sys.stderr)
    sys.exit(99)
open(dst, "w").write("\n".join(lines))
