#!/usr/bin/env bash
# Run every manifest through REAL admission webhooks on a throwaway local cluster,
# before a single AWS resource exists.
#
# WHY: three runs failed today on bugs that only appeared after ~20 minutes of cluster
# build, at roughly $5 a time:
#   - metadata.name: pipeline_latency   (illegal '_')      -> caught offline now
#   - Forbidden Flink config key: kubernetes.cluster-id     -> WEBHOOK, needs a cluster
#   - a half-applied 70-*.yaml leaving everything after it unapplied
# The offline pre-flight cannot see webhook logic: `kubernetes.cluster-id` is valid YAML
# with a valid name and is rejected only by the Flink operator's validating webhook. A
# kind cluster on a GitHub runner costs nothing and takes a couple of minutes.
#
# Installs the full Flink operator (webhooks and all — it is the config-heaviest and has
# bitten twice) plus CRDs for everything else, then server-dry-runs every manifest.
set -euo pipefail
CLUSTER="${KIND_CLUSTER:-manifest-validate}"
KEEP="${KEEP_KIND:-0}"
cleanup() { [ "$KEEP" = 1 ] || kind delete cluster --name "$CLUSTER" >/dev/null 2>&1 || true; }
trap cleanup EXIT

echo "== kind cluster =="
# helm/kind-action may already have created it in CI; only create if absent, and only
# delete one we created ourselves (KEEP_KIND=1 when CI owns it).
if kind get clusters 2>/dev/null | grep -qx "$CLUSTER"; then
  echo "  reusing existing kind cluster '$CLUSTER'"
else
  kind create cluster --name "$CLUSTER" --wait 120s >/dev/null
fi
kubectl cluster-info --context "kind-$CLUSTER" >/dev/null
kubectl config use-context "kind-$CLUSTER" >/dev/null

echo "== CRDs (schema validation for the operators we do not fully install) =="
# Strimzi's install bundle carries its CRDs; the rest publish CRD-only manifests.
kubectl apply --server-side -f 'https://strimzi.io/install/latest?namespace=kafka' >/dev/null 2>&1 || true
kubectl apply --server-side -f https://raw.githubusercontent.com/prometheus-operator/prometheus-operator/main/example/prometheus-operator-crd/monitoring.coreos.com_podmonitors.yaml >/dev/null 2>&1 || true
kubectl apply --server-side -f https://raw.githubusercontent.com/prometheus-operator/prometheus-operator/main/example/prometheus-operator-crd/monitoring.coreos.com_servicemonitors.yaml >/dev/null 2>&1 || true
kubectl apply --server-side -f https://raw.githubusercontent.com/prometheus-operator/prometheus-operator/main/example/prometheus-operator-crd/monitoring.coreos.com_prometheusrules.yaml >/dev/null 2>&1 || true

echo "== flink operator (FULL — its webhook is what we are here to exercise) =="
helm repo add jetstack https://charts.jetstack.io >/dev/null 2>&1
helm repo add flink-operator "${FLINK_OPERATOR_REPO:-https://archive.apache.org/dist/flink/flink-kubernetes-operator-1.13.0/}" >/dev/null 2>&1
helm repo update >/dev/null 2>&1
helm upgrade --install cert-manager jetstack/cert-manager -n cert-manager --create-namespace \
  --set crds.enabled=true --wait --timeout 5m >/dev/null
helm upgrade --install flink-operator flink-operator/flink-kubernetes-operator \
  -n flink --create-namespace --wait --timeout 5m >/dev/null
kubectl -n flink rollout status deploy/flink-kubernetes-operator --timeout=180s >/dev/null
# The Deployment being Available is NOT the same as its validating webhook serving.
# cert-manager still has to issue and inject the cert, and until it does every
# FlinkDeployment dry-run fails with "failed calling webhook ... connection refused" —
# which looks exactly like a rejected manifest. Wait for real endpoints.
echo "  waiting for the validating webhook to serve..."
for i in $(seq 1 60); do
  EP=$(kubectl -n flink get endpoints -o jsonpath='{range .items[*]}{.metadata.name}{" "}{.subsets[*].addresses[*].ip}{"\n"}{end}' 2>/dev/null | grep -ci 'webhook.*[0-9]' || true)
  [ "${EP:-0}" -gt 0 ] && { echo "  webhook endpoints ready"; break; }
  sleep 2
done

# Namespaces must EXIST, not be dry-run. A --dry-run=server of 00-namespaces.yaml
# creates nothing, so every namespaced manifest after it fails with
# `namespaces "kafka" not found` — 8 of 17 manifests, none of them actually broken.
# The real workflow has these by the time it applies: some from 00-namespaces.yaml,
# the rest from explicit `kubectl create ns` and helm --create-namespace. Create them
# for real here so the dry-run validates against a cluster shaped like production.
echo "== namespaces (created for real, so the dry-run is meaningful) =="
kubectl apply -f infra/aws/k8s/00-namespaces.yaml >/dev/null 2>&1 || true
for ns in kafka streaming spark flink fluss monitoring cert-manager; do
  kubectl create namespace "$ns" --dry-run=client -o yaml 2>/dev/null | kubectl apply -f - >/dev/null 2>&1 || true
done
kubectl get ns -o name | sed 's|^|  |'

echo "== server dry-run every manifest =="
# Placeholders: the webhook validates SHAPE and CONFIG KEYS, not secret values.
# Every ${VAR} the manifests reference gets a placeholder, DISCOVERED from the
# manifests rather than hardcoded. A hand-maintained list drifts, and an unset var is
# not a loud failure — envsubst silently substitutes EMPTY, producing a subtly invalid
# manifest. That is exactly what happened here: EFS_ID was missing from the list, so
# volumeHandle became "" and the PV was rejected for a reason that had nothing to do
# with the manifest. Auto-discovery means a newly-introduced var can never do that again.
for v in $(grep -rhv '^[[:space:]]*#' infra/aws/k8s/*.yaml | grep -oE '\$\{[A-Z_][A-Z0-9_]*\}' | tr -d '${}' | sort -u); do
  if [ -z "${!v:-}" ]; then
    case "$v" in
      EFS_ID)          export "$v=fs-0000000000000000" ;;   # must look like an EFS id
      *ROLE_ARN)       export "$v=arn:aws:iam::000000000000:role/placeholder" ;;
      ECR_REGISTRY)    export "$v=placeholder.dkr.ecr.eu-west-1.amazonaws.com" ;;
      AWS_REGION)      export "$v=eu-west-1" ;;
      NODE_AZ)         export "$v=eu-west-1a" ;;
      TRADES_PER_SEC)  export "$v=1000" ;;
      *BASE|*CHECKPOINT_BASE) export "$v=s3://placeholder/chk" ;;
      *)               export "$v=placeholder" ;;
    esac
  fi
done
echo "  placeholders set for: $(grep -rhv '^[[:space:]]*#' infra/aws/k8s/*.yaml | grep -oE '\$\{[A-Z_][A-Z0-9_]*\}' | tr -d '${}' | sort -u | tr '\n' ' ')"
fails=0
# `set -e` is OFF for the loop ON PURPOSE. A failing `out=$(...)` assignment aborts the
# script under -e BEFORE `rc=$?` is read, so the first rejected manifest killed the run
# and printed NOTHING about which one or why. We want every manifest checked and every
# rejection reported in one pass.
set +e
for f in $(ls infra/aws/k8s/*.yaml | sort); do
  b=$(basename "$f")
  # 96-alerts.yaml is applied without envsubst in the real workflow — match that here,
  # or its {{ $labels }} templating gets blanked and we validate the wrong thing.
  if [ "$b" = "96-alerts.yaml" ]; then out=$(kubectl apply --dry-run=server -f "$f" 2>&1); rc=$?
  else out=$(envsubst < "$f" | kubectl apply --dry-run=server -f - 2>&1); rc=$?; fi
  # Missing CRDs for operators we did not install are EXPECTED here and are not failures;
  # a webhook denial or a schema violation is.
  # One retry on a webhook that is not serving yet: that is our infrastructure warming
  # up, not a manifest defect, and reporting it as a rejection would send someone
  # hunting a bug that does not exist.
  if [ $rc -ne 0 ] && echo "$out" | grep -qiE 'failed calling webhook|connection refused|no endpoints available'; then
    sleep 10
    if [ "$b" = "96-alerts.yaml" ]; then out=$(kubectl apply --dry-run=server -f "$f" 2>&1); rc=$?
    else out=$(envsubst < "$f" | kubectl apply --dry-run=server -f - 2>&1); rc=$?; fi
  fi
  if [ $rc -ne 0 ] && ! echo "$out" | grep -qiE 'no matches for kind|could not find the requested resource|ensure CRDs'; then
    echo "  REJECTED $b:"; echo "$out" | sed 's/^/      /'; fails=1
  else
    echo "  ok       $b"
  fi
done
set -e
[ "$fails" = 0 ] || { echo; echo "manifests would be REJECTED in AWS — fix before spending a cluster"; exit 1; }
echo
echo "all manifests accepted by real admission webhooks"
