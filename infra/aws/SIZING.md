# Sizing map — source ingestion rate → resources

How to scale the run for a target `TRADES_PER_SEC`. The **1k/s column is measured**
on EKS 1.34; the 10k and 30k columns are derived from it and are *starting points to
validate*, not verified numbers. Re-measure and correct them after each rate step.

Two hard rules, both learned the painful way:

1. **`limits.cpu` in `10-karpenter.yaml` must never exceed the account's EC2 vCPU
   quota.** If it does, Karpenter plans nodes EC2 refuses to launch and loops
   create → `VcpuLimitExceeded` → delete, with pods Pending and no useful error at
   the pod level. Raise the quota FIRST, then the cap.
2. **Raise the quota before the rate.** Quota increases go through AWS Support
   (`CASE_OPENED`) and can take hours to days — they are not same-day.

## Per-rate settings

| | **1k/s (measured)** | **10k/s (estimate)** | **30k/s (estimate)** |
|---|---|---|---|
| **Total vCPU demand** | ~33 | ~90–110 | ~230–280 |
| **NodePool `limits.cpu`** | 28¹ | 96 | 256 |
| **EC2 quota needed** (on-demand, L-1216C47A) | 32 | 128 | 320 |
| **Generator** `TRADES_PER_SEC` / batch | 1000 / 500 | 10000 / 2000 | 30000 / 5000 |
| Generator cpu req | 1 | 2 | 4 (or 2 replicas) |
| **Kafka** brokers | 3 | 3 | 5 |
| Kafka cpu req/broker | 1 | 2 | 4 |
| `app.public.trades` partitions | 3 | 12 | 24–36 |
| **Spark** driver `coreRequest` / `memory` | 500m / 2g | 500m / 2g | 1 / 4g |
| Spark executor instances × cores × mem | 1 × 1 × 2g | 2 × 2 × 4g | 4 × 2 × 8g |
| **Flink** TM cpu × mem | 2 × 4096m | 4 × 8192m | 8 × 16384m |
| Flink `numberOfTaskSlots` (fluss / paimon) | 4 / 6 | 8 / 12 | 16 / 24 |
| **Fluss** tablet cpu req / limit | 1 / 4 | 2 / 8 | 4 / 16 |
| Checkpoint interval | 10 s | 10 s | 30 s² |

¹ 28 = 32 quota − ~4 held by the EKS managed nodegroup. Karpenter's cap covers only
the nodes it manages.
² Longer checkpoints at high rate: commit overhead per interval grows with volume,
and sub-30s intervals start to dominate p99 latency.

## Scaling rules of thumb

- **Kafka partitions are the parallelism ceiling.** A Spark/Flink source cannot use
  more parallelism than there are partitions, so raising executor counts without
  raising partitions buys nothing. Target ~1 partition per executor core.
- **Spark scales out (executors), Flink scales up (slots + TM size).** Adding Flink
  slots without TM memory just moves the bottleneck to GC.
- **Storage scales with rate × retention, not just rate.** At 10k/s the CDC topic is
  ~10× the bytes/s — check Kafka PVC size and Fluss remote-data before the run, not
  during.
- **Small files get worse with rate, not better.** More microbatches per second means
  more files per commit. Compaction settings matter more at high rate, not less —
  see the compaction note below.
- **Driver sizing is about coordination, not throughput.** `coreRequest: 500m` holds
  up to ~10k/s; beyond that the driver's own scheduling of many small tasks starts to
  matter and it wants a full core.

## Compaction / maintenance interaction

The engines put compaction in different places, which matters when reading latency
results at any rate:

- **Delta** — Optimized Writes + Auto Compaction run IN the streaming job, so
  compaction cost lands inside the microbatch and shows up in p99. VACUUM is separate
  (and commits — hence the DynamoDB LogStore).
- **Paimon** — self-compacts in the writer, same inline cost profile as Delta.
- **Iceberg** — no in-writer compaction; a SEPARATE rewrite/expire job is required, so
  its write path looks cheaper while small files accumulate between runs.

At 1k/s on a quota-capped cluster the maintenance jobs compete with the streaming
pipelines for slots and were suspended to let the comparison run. At higher rates,
give maintenance its own headroom in the vCPU budget rather than suspending it —
otherwise Iceberg's numbers flatter it relative to Delta/Paimon.

## Before raising the rate — checklist

1. Request the EC2 quota increase (on-demand L-1216C47A **and** spot L-34B43A08) and
   wait for it to be granted, not just submitted.
2. Raise `limits.cpu` in `10-karpenter.yaml` to the new value ≤ quota.
3. Raise Kafka partitions on `app.public.trades` (they cannot be reduced later).
4. Apply the column above for the target rate.
5. Re-check `HEALTHCHECK.txt` stage by stage; correct this table with what you measure.

## Cost note

m6a on-demand in eu-west-1 is ~$0.048/vCPU-hr; spot ~$0.031. A 2.5h run costs roughly
`vCPU × rate × 2.5`, so ~$4 at 1k/s, ~$12 at 10k/s, ~$30 at 30k/s on-demand — about
35% less if spot fills. Spot has its own separate quota; on the runs so far every node
landed on-demand because spot was `UnfulfillableCapacity` for m/r gen>5 pinned to a
single AZ, leaving the spot quota entirely unused. Widening the instance-type set is
the cheapest way to gain headroom at high rates.
