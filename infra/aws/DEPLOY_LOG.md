# AWS/EKS deployment — error log

Every failure hit taking the streaming comparison from local compose to EKS, with
its root cause and fix. One line each, in the order encountered. Companion to
`HEALTHCHECK.txt` (how to check) and `README.md` (how to run).

Pattern worth knowing: almost every entry below was found only by fixing the one
before it, so a "fixed" pipeline usually just means the next layer became reachable.

## Phase 1 — first cluster stand-up (EKS 1.30)

| # | Symptom | Cause → Fix |
|---|---|---|
| 1 | `terraform apply` fails on IAM name | name_prefix >38 chars → shorten resource base to `sc-<run_id>` |
| 2 | EKS create denied on KMS | module wants `kms:*` the deploy role lacks → `create_kms_key=false`, `cluster_encryption_config={}` |
| 3 | EKS access entry 409 | creator is already admin via `enable_cluster_creator_admin_permissions` → drop the explicit `access_entries` |
| 4 | `build-fluss.sh` exit 126 | not executable → `chmod +x` (git mode 100755) |
| 5 | flink-operator helm "could not load config with mediatype" | OCI ref is not a valid chart → Apache archive HTTP repo, pinned 1.13.0 |
| 6 | Strimzi CRDs missing after helm install | the strimzi.io/charts chart ships ZERO CRDs → use the official install bundle (server-side apply) |
| 7 | `no matches for kind Kafka in kafka.strimzi.io/v1beta2` | current Strimzi serves `v1` → bump all Kafka manifests |
| 8 | Kafka 3.9.0 rejected | latest Strimzi needs 4.2+ → Kafka 4.2.0 |
| 9 | KafkaConnect "Invalid value null" | v1 promoted `groupId`/`*StorageTopic` to required top-level fields → move them out of `config` |
| 10 | Flink JM 403 "cannot list pods" | native-k8s JM runs as the `lake` SA → add flink Role/RoleBinding |
| 11 | fluss-flink Pending, "PVC fluss-remote not found" | PVCs are namespace-scoped; dynamic EFS AP isolates → STATIC PVs sharing one `volumeHandle` across namespaces |
| 12 | Fluss coordinator `UnsupportedSchemeException: s3` | paimon-s3 1.4.2 REQUIRES static keys (no IRSA chain) → scoped IAM user, key in SSM SecureString, injected at deploy |

## Phase 2 — version + teardown hardening

| # | Symptom | Cause → Fix |
|---|---|---|
| 13 | Cluster on EKS 1.30 | past standard support; 1.31–1.33 cost 6x in extended support → `cluster_version = 1.34` |
| 14 | `terraform destroy` leaves VPC/subnet (DependencyViolation) | Karpenter nodes are not in tf state; their CNI ENIs block deletion → terminate cluster-tagged EC2, sweep orphan EBS |
| 15 | "Error acquiring the state lock" | a cancelled/timed-out apply leaves the DynamoDB lock → `terraform force-unlock -force <ID>` |
| 16 | Next apply: `CreateLogGroup ResourceAlreadyExistsException` | EKS auto-creates `/aws/eks/<cluster>/cluster`; a forced teardown leaves it out of state → sweep `/aws/eks/$CN/*` in both teardown paths (`0d8e5a5`) |
| 17 | Destroy drags ~20 min on EFS | terraform retries the mount-target dependency slowly → delete mount targets up front, then destroy |

## Phase 3 — teardown actually failing (kill-switch first live firing)

| # | Symptom | Cause → Fix |
|---|---|---|
| 18 | Workflow destroy step: `RequestExpired` | OIDC creds (~1h) expire during the 120-min run window → re-assume the role after the sleep, `if: always()` (`605f425`) |
| 19 | Kill-switch CodeBuild FAILED | teardown role had no `elasticfilesystem:*` and only lock-table `dynamodb` → add efs/dynamodb/kms/ssm (`cf3dd40`) |
| 20 | Terminating nodes *increased* node count (3→4) | EC2 was terminated while the control plane was alive, so Karpenter relaunched them → destroy FIRST (kills Karpenter), then terminate, sweep `available` `aws-K8S-*` ENIs, destroy again (`cf3dd40`) |

## Phase 4 — pipelines: getting data to flow (EKS 1.34)

| # | Symptom | Cause → Fix |
|---|---|---|
| 21 | `generator-trades` CrashLoop: `can't open file '/app/gen_trades.py'` | lean image has no source (compose volume-mounts it) → deliver `generators/*.py` via a `generator-src` ConfigMap at `/app` (`ef837d1`) |
| 22 | `invalid literal for int(): 'tcp://10.x.x.x:5432'` | manifest set `PG*` but `common.py` reads `POSTGRES_*`, so k8s service-links injected `POSTGRES_PORT` → set the right names + `enableServiceLinks: false` (`ef837d1`) |
| 23 | `debezium-connect-build` Error: "authentication required" | node role has ECR pull only → `build.output.pushSecret` from an ECR token (`ef837d1`) |
| 24 | `pg-trades` connector stuck, JMX "Unable to register metrics" loop | two connectors shared `topic.prefix: app`, but both tables need that prefix → ONE connector capturing both tables (`ef837d1`) |
| 25 | Connector 400: "max.queue.size ... must be larger than max batch size" | queue default 8192 equals `max.batch.size` → `max.queue.size: 32768` (`ef837d1`) |
| 26 | Fluss gold + tiering stuck RESTARTING, `NoResourceAvailableException` | native-k8s JM GETs its own Deployment as owner-ref; Role lacked `apps/deployments` → 403 on every TaskManager create → add the rule (`f4ae620`) |
| 27 | flink-paimon JM had ZERO jobs; "The scheme (hdfs://, file://, etc) is null" | `FLINK_CHECKPOINT_BASE` unset → `state.checkpoints.dir` rendered scheme-less → set `s3://$PAIMON_BUCKET/_flink_chk` (`f4ae620`) |
| 28 | All 8 SparkApplications ignored (status `<none>`, no driver pods, no events) | kubeflow chart watches only the `default` namespace → install with `--set spark.jobNamespaces={spark}` (`f4ae620`) |
| 29 | Every Spark driver: "Failed to get main class in JAR ... /opt/spark/work-dir (Is a directory)" | image ENTRYPOINT is compose-only (`--master local[2]`, MinIO creds, `"$JOB_FILE"`) and swallowed the operator's args → delegate to `/opt/entrypoint.sh` for `driver`/`executor` (`2792db6`) |
| 30 | Paimon commits: `NoClassDefFoundError ... hive/metastore/api/NoSuchObjectException` | aws branch used the Glue/Hive Iceberg committer; image carries no Hive/Glue jars → `metadata.iceberg.storage = hadoop-catalog` (`2792db6`) |
| 31 | Rebuilt image not picked up | `:latest` with default `IfNotPresent` reuses the cached layer → `imagePullPolicy: Always` on all 8 apps (`2792db6`) |
| 32 | Drivers: `ClassNotFoundException com.amazonaws.auth.WebIdentityTokenCredentialsProvider` | that is an AWS SDK **v1** class; image ships Hadoop 3.4.1 + SDK v2 → `software.amazon.awssdk...DefaultCredentialsProvider` (`7b44b31`) |
| 33 | Executors stuck Pending; Karpenter thrashing | 8 apps x 2 executors x 2 cores exceeded the NodePool 64-CPU cap → 1 executor x 1 core x 2g (equal for every engine, so the comparison stays fair) (`7b44b31`) |
| 34 | Delta drivers: `ClassNotFoundException io.delta.storage.S3DynamoDBLogStore` | that class ships in `delta-storage-s3-dynamodb`, a separate artifact from `delta-storage` → fetch it too (`6985436`) |
| 35 | Iceberg drivers: `NoSuchFieldError: ADAPTIVE_V2` at `GlueCatalog.initialize` | standalone `bundle-2.24.6` shadowed the newer SDK inside iceberg-aws-bundle → bump the bundle to 2.31.78 (`6985436`) |
| 36 | Paimon still wrote `hive-catalog` after the fix | Paimon persists table options INTO the S3 schema file; `CREATE TABLE IF NOT EXISTS` will not update them → drop the (data-free) stale tables so they are recreated |
| 37 | Duplicate Paimon jobs per table | the submitter Job was re-run without cancelling the previous jobs → cancel duplicates (two writers per table conflict on commit) |
| 38 | All silver-accounts pipelines failed on a missing Kafka topic | `gen_accounts.py` existed but was never deployed, so `accounts` stayed empty and Debezium never created the topic → add a `generator-accounts` Deployment (`6985436`) |
| 39 | Every Spark driver: `ConfigException: No resolvable bootstrap urls` | jobs read `KAFKA_BROKERS` (default `kafka:9092`) but the manifests set `KAFKA_BOOTSTRAP`, so all 8 silently used the compose-era default → rename in the Spark manifests (same class as #22; Flink's submit.sh genuinely reads `KAFKA_BOOTSTRAP`) |
| 40 | `gold_open_positions` + `silver_accounts` (both engines): `NameError: name 'ensure_all' is not defined` | 4 job files call `ensure_all(spark)` without importing it — a pre-existing repo bug, not AWS-specific → add `from <engine>_tables import ensure_all` |
| 41 | Delta drivers: `NoClassDefFoundError com/amazonaws/services/dynamodbv2/...` | `delta-storage-s3-dynamodb` is compiled against the AWS SDK **v1**, absent from this SDK-v2 image → drop the `S3DynamoDBLogStore` config entirely: one streaming writer per table and VACUUM writes no commits, so the default single-driver log store is correct (this reverses the jar added in #34) |
| 42 | One app kept the old env var after a bulk patch | a `kubectl replace` loop skipped an app whose fetch failed mid-loop → always re-verify the whole set, not the loop's echo output |

## Recurring gotchas (cost us time more than once)

- A fresh cluster only trusts its creator: re-add your SSO access entry or the console/kubectl says **Unauthorized**.
- `terraform destroy` piped through `grep` looks hung — it is block-buffered; check `aws eks describe-cluster`, not the pipe.
- `pgrep -f <pattern>` inside a script whose own command line contains that pattern kills the script's own shell; use `pgrep -x`.
- Destroying with `-refresh=false` leaves stale entries in state; reconcile with one refresh-enabled destroy before the next run.
- `busybox nslookup` does NOT apply search domains the way glibc/Java do: it returned NXDOMAIN for `svc.kafka.svc` and sent me chasing a DNS problem that did not exist (`getent` inside the real image resolved it fine). Test DNS with the resolver the app actually uses.
- Two bugs of the same shape (#22, #39): a manifest sets one env-var name while the code reads another, and a *compose-era default* in the code hides the mismatch instead of failing loudly.
