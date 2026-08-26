# Malformed-record handling — what each layer can actually do

Settled after two silent-data-loss bugs (DEPLOY_LOG #50, #51), where every pipeline
ran "healthy" for hours writing empty or NULL-priced batches.

## The key correction

**`json.ignore-parse-errors: true` is not a dead-letter queue.** Flink's JSON format
has no DLQ. On a parse failure it sets the field (or whole row) to NULL and moves on —
the record is gone, nothing is recorded, and the job reports healthy. That is strictly
worse than crashing, because a crash is visible. The same applies to Spark's
`from_json`, which defaults to PERMISSIVE and yields NULLs.

A DLQ has to exist at a layer that can *hold* the bad record. In this stack that is
Kafka Connect.

## Where each layer lands

| Layer | Setting | Value | Why |
|---|---|---|---|
| **Kafka Connect (Debezium)** | `errors.tolerance` | `all` | Real DLQ support — the only layer with it |
| | `errors.deadletterqueue.topic.name` | `debezium-dlq` | Bad record preserved with failure cause in headers |
| | `errors.deadletterqueue.context.headers.enable` | `true` | Without it you get the payload but not *why* |
| | `errors.log.include.messages` | `false` | Payloads may hold PII; the headers carry the cause |
| **Flink SQL** | `json.ignore-parse-errors` | **`false`** | No DLQ available — so fail loudly rather than NULL silently |
| **Spark** | `from_json` mode | PERMISSIVE (default) | See "Spark option" below |
| **Debezium** | `decimal.handling.mode` | **`string`** | Exact decimal, JSON-safe. See below |

`errors.tolerance: all` covers **conversion and transform** errors only. Genuine
connector failures — lost database connection, invalid replication slot — still fail
the task loudly, which is what you want.

## decimal.handling.mode

| Mode | Exact? | JSON-usable? | Verdict |
|---|---|---|---|
| `precise` (default) | yes | no — base64 Connect Decimal bytes | Correct **with Avro**, unusable with plain JSON |
| `double` | **no** | yes | Never for money: error compounds in SUM() |
| `string` | yes | yes | **Our choice** — exact on the wire, cast at each boundary |

With plain JSON, `string` is the only option that is both exact and decodable. The
consumers cast explicitly (`CAST(... AS DECIMAL(12,4))` in Flink, `.cast(DecimalType)`
in Spark) so the value never passes through a float.

**The stronger answer is Avro + Schema Registry**, where `precise` works natively via
the decimal logical type, and the registry makes the whole class of bug we hit
impossible: a producer cannot silently change the wire format (schemas.enable,
decimal mode) because the schema is registered and compatibility-checked. Worth
considering if this graduates past a benchmark.

## Fail-fast vs DLQ, and why fail-fast here

For a **benchmark**, fail-fast is right: a parse error means the pipeline and the data
disagree, and every measurement after that point is meaningless. A crash-looping job is
obvious within seconds; NULL columns are not.

For **production**, you want neither NULLs nor a crash loop — you want the bad record
quarantined and the stream to continue. Since Flink SQL cannot do that, the options are:

1. Handle it at Connect (done here) so malformed records never reach Flink.
2. Drop to the DataStream API and use a side output for unparseable records.
3. Read the topic as `raw`/BYTES, parse in a UDF, and branch valid/invalid yourself.

## Spark option (not applied)

`from_json` accepts a corrupt-record column, which gives DLQ-like behaviour without
crashing:

```python
from_json(col("value").cast("string"), SCHEMA,
          {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": "_corrupt"})
# then: .filter("_corrupt IS NOT NULL") -> write to a quarantine table
```

Left unapplied deliberately: with Connect quarantining at the source, a corrupt record
reaching Spark means a *schema* mismatch, which should be loud. Revisit if the Spark
jobs ever consume a topic Connect does not own.

## Checking the DLQ

```bash
kubectl -n kafka exec trades-dual-role-0 -- bin/kafka-get-offsets.sh \
  --bootstrap-server localhost:9092 --topic debezium-dlq       # expect 0
kubectl -n kafka exec trades-dual-role-0 -- bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic debezium-dlq \
  --from-beginning --max-messages 5 --property print.headers=true
```

A non-zero DLQ offset means records are being rejected — treat it as a run-invalidating
signal, not a warning.
