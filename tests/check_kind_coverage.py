"""Every custom resource in the manifests must be VALIDATED by the kind pre-flight.

validate-against-kind.sh server-dry-runs each manifest against real admission webhooks.
It can only validate a kind whose CRD is installed on that throwaway cluster — and it used
to report `ok` for any manifest that failed with "no matches for kind", which meant a
missing CRD looked exactly like a passing manifest.

That silently left 14 SparkApplication and 2 ScheduledSparkApplication resources, across
four files and every Spark pipeline, completely unchecked while the gate reported green.

This check needs no cluster: it maps every custom kind in infra/aws/k8s to the thing that
installs its CRD in validate-against-kind.sh, and fails if a kind is neither installed nor
on that script's explicit allowlist. It is what makes "we validate the manifests before
provisioning" a true statement rather than an aspiration.
"""
import pathlib, re, sys

BUILTIN = {
    "Namespace", "ServiceAccount", "Service", "ConfigMap", "Secret", "Role", "RoleBinding",
    "ClusterRole", "ClusterRoleBinding", "Deployment", "StatefulSet", "DaemonSet", "Job",
    "CronJob", "Pod", "PersistentVolume", "PersistentVolumeClaim", "StorageClass",
}
# custom kind -> the marker in validate-against-kind.sh that installs its CRD
INSTALLERS = {
    "Kafka": "strimzi.io/install", "KafkaNodePool": "strimzi.io/install",
    "KafkaTopic": "strimzi.io/install", "KafkaConnect": "strimzi.io/install",
    "KafkaConnector": "strimzi.io/install",
    "PodMonitor": "monitoring.coreos.com_podmonitors",
    "ServiceMonitor": "monitoring.coreos.com_servicemonitors",
    "PrometheusRule": "monitoring.coreos.com_prometheusrules",
    "FlinkDeployment": "flink-kubernetes-operator",
    "SparkApplication": "spark-operator/spark-operator",
    "ScheduledSparkApplication": "spark-operator/spark-operator",
}

script = pathlib.Path("infra/aws/scripts/validate-against-kind.sh").read_text()
m = re.search(r'^ALLOW_UNVALIDATED="([^"]*)"', script, re.M)
allowlist = set((m.group(1) if m else "").split())

fails = []
for f in sorted(pathlib.Path("infra/aws/k8s").glob("*.yaml")):
    kinds = {k for k in re.findall(r"^kind:\s*([A-Za-z0-9]+)", f.read_text(), re.M)}
    custom = kinds - BUILTIN
    if not custom:
        continue
    missing = [k for k in sorted(custom)
               if INSTALLERS.get(k) is None or INSTALLERS[k] not in script]
    if not missing:
        print(f"  PASS  {f.name:24} {', '.join(sorted(custom))}")
    elif f.name in allowlist:
        print(f"  PASS  {f.name:24} {', '.join(missing)} — allowlisted as unvalidatable")
    else:
        print(f"  FAIL  {f.name:24} {', '.join(missing)} — no CRD installed and not allowlisted")
        fails.append(f"{f.name}: {', '.join(missing)}")

print(("\nevery custom resource is validated by the kind pre-flight" if not fails
       else f"\n{len(fails)} manifest(s) would be silently skipped: " + "; ".join(fails)))
sys.exit(1 if fails else 0)
