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
| 41 | Delta drivers: `NoClassDefFoundError com/amazonaws/services/dynamodbv2/...` | `delta-storage-s3-dynamodb` is compiled against the AWS SDK **v1**, absent from this SDK-v2 image. First disabled the log store to unblock — **wrong call, since reverted**: VACUUM does commit, so there really are two committers. Correct fix = install SDK v1 (dynamodb + core + jmespath) alongside the v2 bundle, plus `jackson-dataformat-cbor` (the v1 DynamoDB CBOR codec, absent from the base Spark image) |
| 42 | One app kept the old env var after a bulk patch | a `kubectl replace` loop skipped an app whose fetch failed mid-loop → always re-verify the whole set, not the loop's echo output |
| 43 | Pods Pending ~15 min; Karpenter loops "could not schedule pod" / "failed launching nodeclaim" | NOT a k8s problem: EC2 rejected every launch with `VcpuLimitExceeded: current vCPU limit of 32`. The NodePool cap (64) was above the ACCOUNT quota, so Karpenter created NodeClaims that could never launch → align the NodePool cap with the real quota, and raise both together via Service Quotas (L-1216C47A on-demand, L-34B43A08 spot) |
| 44 | All nodes launched on-demand, spot quota unused | on-demand and spot are SEPARATE 32-vCPU quotas; spot was reported UnfulfillableCapacity for the chosen types, so everything fell back to on-demand and burned the on-demand quota alone → widen the instance-type set so spot has more chance to fill, doubling usable headroom |
| 45 | (found by review, not a failure) `autoCompact` was a table property on only 1 of 4 Delta tables | the other three relied on the session conf, so any writer that does not set it (ad-hoc query, VACUUM app, future job) would skip compaction → set `delta.autoOptimize.autoCompact` on all four; table properties are the durable contract, session confs are per-app |
| 46 | (found by review) the VACUUM app still had `S3DynamoDBLogStore` while the streaming writers had it removed | the two committers would have used DIFFERENT log stores and coordinated through nothing at all — strictly worse than either choice alone → keep 91-spark-delta.yaml and 93-maintenance.yaml in sync |
| 47 | Delta drivers: `SdkClientException ... WebIdentityTokenCredentialsProvider: To use assume role profiles the aws-java-sdk-sts module must be on the class path` | restoring S3DynamoDBLogStore pulled in the AWS SDK **v1**, and IRSA authenticates by exchanging a web-identity token through STS → add `aws-java-sdk-sts` to the image. My local test missed it because it injected STATIC env credentials, so the STS path was never exercised |
| 48 | Then: `AmazonDynamoDBException ... sc-iter-workload is not authorized to perform: dynamodb:DescribeTable` | the workload IRSA role had S3/Glue/Athena but no DynamoDB → add a `DeltaLogStore` statement (DescribeTable/Get/Put/Update/Delete/Query) scoped to the log-store table ARN in `irsa.tf` |
| 49 | After adding `autoCompact` to the DDL, every Delta driver died with `DELTA_CREATE_TABLE_WITH_DIFFERENT_PROPERTY` | `createIfNotExists()` FAILS when the table already exists with a different property set, so changing the property list breaks every restart against tables an earlier build created → keep drifting properties OUT of the create and converge them with `ALTER TABLE ... SET TBLPROPERTIES` in `ensure_all` (same shape as Paimon persisting table options into its S3 schema, #36) |
| 50 | Every engine RUNNING and consuming, but committing EMPTY batches forever (Iceberg: 620 metadata objects, 0 parquet; Delta: commit v259 with 0 add actions) | Connect's JsonConverter defaults to `schemas.enable=TRUE`, wrapping each record as `{"schema":…,"payload":…}`. Every consumer expects the payload at top level (Spark parses `op`/`after` directly; Flink's debezium-json defaults to `schema-include=false`), so `after` parsed as NULL and the not-null filter dropped 100% of rows → set `key/value.converter.schemas.enable: false` on the KafkaConnect. docker-compose already set these; a pure local→AWS gap |
| 51 | `price` would land NULL even after #50 | `price` is NUMERIC(12,4) in Postgres and Debezium's default `decimal.handling.mode=precise` emits base64 Connect Decimal bytes that the JSON consumers cannot decode. First set `double` — **wrong for money, since reverted**: IEEE754 cannot represent decimal fractions exactly and the error compounds in `net_notional = SUM(quantity × price)`. Correct fix = `decimal.handling.mode: string` (exact) with every money column typed DECIMAL: price DECIMAL(12,4) matching the source, net_notional DECIMAL(38,4) for headroom |
| 52 | (found by review) Flink declared `price DECIMAL(12,4)` directly in the Kafka source ROW | with `decimal.handling.mode=string` the wire value is a quoted string, and these sources set `json.ignore-parse-errors: true`, so a type mismatch would have silently produced NULL prices instead of failing → read `price STRING` and `CAST(... AS DECIMAL(12,4))` explicitly, matching the Spark jobs |
| 53 | `terraform destroy` failed twice: `DeleteConflict: Cannot delete entity, must delete policies first` on `sc-iter-workload` | an inline policy attached by hand (`aws iam put-role-policy`, to unblock the live run) is invisible to Terraform, and an IAM role cannot be deleted while it holds one → put the policy in `irsa.tf` instead; the refresh-enabled reconcile pass cleared the orphan |
| 54 | Fluss writes 196k rows but the lake stays EMPTY for the whole run | tiering fails at WARN level so the job stays RUNNING and looks healthy. `TieringSourceEnumerator: Fail to generate Tiering splits` → `FlussRuntimeException: Leader not found after retry 3 times for TableBucket{tableId=0}` → `StaleMetadataException: Alive tablet server is empty`. RULED OUT: the sink (Sink read=196500), the source config (byte-identical to flink-paimon, which writes 5M rows), DNS/reachability (resolves, 9123 reachable, endpoint populated, pod Ready), and table config (datalake.enabled=true, freshness=30s). The WRITE client sees the tablet server; the TIERING client does not — and `tableId=0` looks like an uninitialised lookup. Images are built from Fluss **main (1.0-SNAPSHOT)**, so a released version is the first thing to try |
| 55 | Hudi tables sync to Glue but EVERY Athena query fails `HIVE_UNKNOWN_ERROR` | **Athena supports Hudi 0.14.0 and 0.15.0 ONLY** and explicitly "cannot guarantee read compatibility with tables created with later versions" ([AWS docs](https://docs.aws.amazon.com/athena/latest/ug/querying-hudi-in-athena-considerations-and-limitations.html)). We write Hudi **1.2.0** → `hoodie.table.version=9`, `timeline.layout.version=2`. Affects `_ro` AND `_rt`. `hoodie.write.table.version=6` reaches the driver (verified in the mounted file) but is IGNORED. Note Athena DOES support snapshot + read-optimized queries — it is *incremental* queries it lacks — so this is a version problem, not a query-type one. Hudi data on S3 is correct (DECIMAL preserved); it is just not Athena-readable, and DuckDB has no Hudi reader either, so Spark is the only path |

## Recurring gotchas (cost us time more than once)

- A fresh cluster only trusts its creator: re-add your SSO access entry or the console/kubectl says **Unauthorized**.
- `terraform destroy` piped through `grep` looks hung — it is block-buffered; check `aws eks describe-cluster`, not the pipe.
- `pgrep -f <pattern>` inside a script whose own command line contains that pattern kills the script's own shell; use `pgrep -x`.
- Destroying with `-refresh=false` leaves stale entries in state; reconcile with one refresh-enabled destroy before the next run.
- `busybox nslookup` does NOT apply search domains the way glibc/Java do: it returned NXDOMAIN for `svc.kafka.svc` and sent me chasing a DNS problem that did not exist (`getent` inside the real image resolved it fine). Test DNS with the resolver the app actually uses.
- Karpenter reporting `karpenter.sh/initialized In [true]` / `registered In [true]` inside a "no instance type has enough resources" error means it is only considering EXISTING nodes — the real failure is in `failed launching nodeclaim`, not the scheduling message. Always read both.
- **A granted Service Quotas request shows as `CASE_CLOSED`, and the new value takes ~30 minutes to appear in `get-service-quota`.** Closed does not mean refused — I called an approved increase a refusal by checking the API immediately. Read the case correspondence, or wait out the propagation delay.
- **Hand-attached IAM policies block `terraform destroy`.** Anything added with `put-role-policy` to unblock a live run must be mirrored into Terraform, or the role cannot be deleted at teardown.
- **Money is never a float.** price is NUMERIC(12,4) at the source; every engine now carries DECIMAL(12,4) and DECIMAL(38,4) for the running sum. Floating point would not fail — it would drift silently in the gold aggregate, which is precisely what the benchmark measures.
- **Lenient parsers turn type errors into silent NULLs.** `json.ignore-parse-errors: true` (Flink) and permissive `from_json` (Spark) both mean a schema mismatch produces NULL rather than an error. Cast explicitly at the boundary.
- **"RUNNING" is not "working".** Both engines sat RUNNING, consuming Kafka and advancing checkpoints, while writing empty commits for hours. Offsets advancing only proves the SOURCE is read, not that rows survive the transform. Check add-actions/data files, not pod status.
- **Compare AWS config against docker-compose when behaviour differs.** Two silent data-loss bugs (#50, #51) were settings compose had and the k8s manifests did not.
- **Table properties are part of the create contract.** Both Delta (`createIfNotExists`) and Paimon (schema file in S3) refuse or silently ignore a changed property set on an existing table. Any property you might want to change later must be converged explicitly (ALTER TABLE), not just declared at create time.
- **Verify with the credential path production uses.** Testing the DynamoDB LogStore locally with static env credentials passed, then failed on EKS twice — first missing `aws-java-sdk-sts` (IRSA needs STS), then missing DynamoDB IAM permissions. Neither is reachable with static creds. Run the check in-cluster under the real ServiceAccount.
- Two bugs of the same shape (#22, #39): a manifest sets one env-var name while the code reads another, and a *compose-era default* in the code hides the mismatch instead of failing loudly.
