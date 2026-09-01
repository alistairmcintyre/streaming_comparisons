#!/usr/bin/env bash
# Resolve the container registry from ONE place, for every script that needs an image.
#
# Order: an explicit ECR_REGISTRY in the environment, else derived from AWS_ACCOUNT_ID,
# which comes from env/aws.env (gitignored, yours to create from env/aws.example.env).
# No account id is hardcoded anywhere in this repo.
#
# Deliberately NOT fatal when nothing is configured. The offline checks are the ones
# every contributor runs and they need no registry at all, so this leaves ECR_REGISTRY
# empty and lets the caller decide whether that is a problem.
_ecr_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
if [ -z "${ECR_REGISTRY:-}" ] && [ -f "$_ecr_root/env/aws.env" ]; then
  # shellcheck disable=SC1091
  set -a; . "$_ecr_root/env/aws.env"; set +a
fi
if [ -z "${ECR_REGISTRY:-}" ] && [ -n "${AWS_ACCOUNT_ID:-}" ] && [ "${AWS_ACCOUNT_ID}" != "REPLACE_ME" ]; then
  ECR_REGISTRY="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION:-eu-west-1}.amazonaws.com/${ECR_NAMESPACE:-streaming-comparison}"
fi
export ECR_REGISTRY="${ECR_REGISTRY:-}"

# Print why images are unavailable, for a caller that needs them.
ecr_required() {
  [ -n "$ECR_REGISTRY" ] && return 0
  echo "  no container registry configured: set AWS_ACCOUNT_ID in env/aws.env" >&2
  echo "  (copy env/aws.example.env), or export ECR_REGISTRY directly." >&2
  return 1
}
