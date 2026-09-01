#!/usr/bin/env bash
# Validate every k8s manifest BEFORE terraform builds anything.
#
# Why: an invalid manifest is currently only discovered by applying it to a live
# cluster, roughly 25 minutes and a cluster's worth of money into a run. That is
# exactly how `metadata.name: pipeline_latency` (illegal '_' in an RFC 1123 name) burned
# a deploy: catchable offline in under a second.
#
# Deliberately not `kubectl apply --dry-run=client`: the name violation above is
# rejected SERVER-side, so client dry-run does not catch it, and the CRDs here
# (KafkaTopic, FlinkDeployment, PodMonitor, SparkApplication) have no local schema
# anyway. These are the checks that actually work without a cluster.
set -uo pipefail
DIR="${1:-infra/aws/k8s}"
python3 - "$DIR" <<'PY'
import glob, os, re, sys
d = sys.argv[1]
files = sorted(glob.glob(os.path.join(d, "*.yaml")))
if not files:
    print(f"preflight: no manifests in {d}", file=sys.stderr); sys.exit(1)

try:
    import yaml
    class Dup(yaml.SafeLoader): pass
    def no_dup(loader, node, deep=False):
        seen, out = set(), {}
        for k, v in node.value:
            key = loader.construct_object(k, deep=deep)
            if key in seen:
                raise yaml.YAMLError(f"duplicate key {key!r}, the later value silently wins")
            seen.add(key)
            out[key] = loader.construct_object(v, deep=deep)
        return out
    Dup.add_constructor(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, no_dup)
except ImportError:
    yaml = None
    print("  WARN pyyaml missing, structural checks skipped, running regex checks only")

RFC1123 = re.compile(r'^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$')
problems = []

for f in files:
    raw = open(f).read()
    base = os.path.basename(f)

    # envsubst runs over these at apply time and replaces ANY $NAME, not just ${NAME}.
    # Go templating ({{ $labels.pod }}) is therefore silently blanked unless the file is
    # excluded from substitution in the workflow. That shipped once and broke 11 alerts.
    bare = sorted(set(re.findall(r'\$(?!\{)([A-Za-z_][A-Za-z0-9_]*)', raw)))
    if bare:
        exempt = "96-alerts.yaml"     # applied without envsubst; keep in sync with eks-run.yml
        if base != exempt:
            problems.append(f"{base}: bare $-vars {bare} will be eaten by envsubst "
                            f"(exclude the file from substitution, or use ${{...}})")

    if yaml is None:
        continue
    try:
        docs = list(yaml.load_all(re.sub(r'\$\{[^}]*\}', 'PLACEHOLDER', raw), Loader=Dup))
    except Exception as e:
        problems.append(f"{base}: {str(e).splitlines()[0]}")
        continue

    for i, doc in enumerate(docs):
        if doc is None:
            continue
        if not isinstance(doc, dict):
            problems.append(f"{base} doc[{i}]: not a mapping"); continue
        for req in ("apiVersion", "kind"):
            if req not in doc:
                problems.append(f"{base} doc[{i}]: missing {req}")
        name = (doc.get("metadata") or {}).get("name")
        if name and not name.startswith("PLACEHOLDER") and not RFC1123.match(str(name)):
            problems.append(f"{base} doc[{i}]: {doc.get('kind')} name {name!r} is not a valid "
                            f"RFC 1123 name (no '_', no uppercase), the apiserver will reject it")

if problems:
    print("preflight FAILED:")
    for p in problems: print(f"  - {p}")
    sys.exit(1)
print(f"preflight OK: {len(files)} manifests parse, names valid, no duplicate keys")
PY
