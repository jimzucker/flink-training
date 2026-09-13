# Journal

What drove each step, what was decided, how it was verified. Times are local
(UTC-4). The measurement harness is `~/.claude/skills/prove-it-scales/harness`,
used verbatim; this project supplies the pipeline and `pipeline.json`.

## 0. The interview that did not happen

No human was available, so the questions are written down with the answer taken
in their place. Each is an assumption, and each is falsifiable from the results.

| # | question | assumption taken | why |
|---|---|---|---|
| 1 | What is the input event, and what comes out? | A block trade arrives on `block-trades`; the job allocates it across accounts and emits the running position for the symbol and for each account. | Given in the task. |
| 2 | Does one input become several outputs? | 4: one symbol-position update per block trade, one account-position update per allocation, 3 accounts per trade. | "Allocates across accounts" needs ≥2; 3 keeps the write side ~4× the read side, which is where the load lands. |
| 3 | What are the keys, and how many distinct ones? | 512 symbols, 4096 accounts. | Started at 64/128 and **measured** that it was wrong — see §3 below. |
| 4 | What has to be exactly right? | Running sums, so: exactly-once *checkpointing*; at-least-once *sink*, made idempotent by emitting the absolute position per key. | The skill's usual answer, and the sink payload is an absolute value, so a replay rewrites the same number. |
| 5 | Who watches, and what must they believe? | An engineering audience: that the numbers are the pipeline's and nothing was lost. | Hence completeness gates the table and the worker-kill arm runs. |
| 6 | Where does it run? | This laptop, in Docker. | Given in the task. |
| 7 | What claim? | *"This pipeline, on one worker, scales linearly from 1 to 2 to 4 cores: each doubling of cores and parallelism bought a proportional doubling of block trades drained per second."* | Given in the task: the headline is the two step ratios. |
| 8 | Which axis? | One worker growing: one task-manager container capped at N cores, parallelism N, N slots. | The laptop proxy; recorded in the results header. |
| 9 | Which API level? | Flink DataStream API, hand-written operators. | Given in the task: no SQL, no Table API. |

Open question I could not settle without a human: whether the audience wants the
1-core case at all. The skill says not to run it when the claim starts at two
units, and it is the structurally weakest case; the task asked for 1→2, so it ran.

## 1. Build

`job/` is one shaded jar holding three programs so there is one definition of
each rule: `PositionsJob` (the Flink job), `GenerateBacklog` (deterministic
generator + manifest), `VerifyCompleteness` (the verifier). `Gen.allocate` is a
pure function of the trade record — the job has no seed — so the manifest and
the job cannot disagree about what an allocation is.

Graph, read off the running plan and identical at every parallelism:

```
V1  Source: kafka-source -> parse-block-trade -> allocate     (chained)
V2  [HASH by symbol]   symbol-position  -> sink positions-by-symbol
V3  [HASH by account]  account-position -> sink positions-by-account
```

Two defects found before any measurement:

- **jackson-core/databind mismatch.** kafka-clients pulls an older jackson-core;
  the first `writeValueAsBytes` threw `NoSuchMethodError` on a producer thread
  and the generator still printed "produced 100,000 records" and exited 0. Fixed
  with a jackson BOM, and the generator now propagates a dead thread instead of
  reporting success. The harness caught it anyway — `verify_backlog` read 0
  records back against a manifest saying 6,000,000.

## 2. Rig work: five hypotheses, four wrong

The worker has to be the constraint or nothing else matters. At 4 cores it was
reading **90.3–96.2%** of its cap against a 95% floor, and the 1-core baseline
95.1–99.3% against a 98% floor. Each hypothesis below was a controlled
comparison on one rig and one build, both arms measured.

| hypothesis | test | result |
|---|---|---|
| host/VM CPU contention | VM-wide `/proc/stat` while a 4-core case ran | **ruled out** — VM busy 4.74 of 8 cores |
| barrier alignment at higher parallelism | unaligned checkpoints on/off | **wrong** — 476.9k @ 95.0% vs 507.7k @ 95.4% |
| checkpoint interval | 10 s vs 20 s, same build, same rig | **wrong** — 92.5% vs 91.4% |
| sink batching too small | 1 MB batches, 100 ms linger, 96 MB buffer | **wrong, and backwards** — 378k @ 94.5%, GC 9.1% |
| broker fetch parallelism | 4 / 8 / 16 input partitions | 4 partitions starved the source (idle 7.2%, cap 85.5%); 8 is right |

Three things were true and were fixed:

1. **Key skew.** Thread dumps showed the four `symbol-position` subtasks burning
   23.6 s, 34.0 s, 42.8 s and 51.5 s of CPU over the same 283 s — a 2.2× load
   spread. Flink hashes keys into 128 key groups; 64 symbols cannot fill them
   evenly, and the imbalance was being charged to scaling. 512 symbols / 4096
   accounts made the subtasks equal to within a few percent.
2. **Source prefetch.** `source.reader.element.queue.capacity` 2 → 8. 4-core cap
   fraction 94.3% → 96.7%.
3. **The worker was I/O-blocked, not CPU-bound.** `account-position` reported
   93% *busy* while consuming 0.36 of a core: "busy" counts time blocked inside
   `producer.send()`. CPU sampled every 2 s sat at 4.00 of 4 cores except in
   windows containing the occasional 1.8–2.0 s checkpoint (most were 75 ms) —
   the sink flush. Two changes made the worker CPU-dense instead of I/O-dense:
   - a realistic block-trade message (22 fields, ~430 B, as a FIX-derived JSON
     document actually looks) instead of an 8-field stub, and
   - `zstd` instead of `lz4` on the sinks: more worker CPU per record, fewer
     bytes for the broker to absorb.

   1 core went 96.8% → **99.6%** of cap; 4 cores 92.8% → **100.1%**, throttled
   99.9% of periods, vantage disagreement 11.8% → 0.5%.

The record of what the last change bought is in the report. The mechanism for
the remaining ~0.2% is not claimed.

## 3. The chain

`prove.py all` — up → preflight → completeness → tinyproof → fill → suite →
report, one stack session. Nothing was hand-typed between steps.
