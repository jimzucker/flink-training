# Journal

What drove each step, what was decided, how it was verified. Section 2 of the skill.

## Step 1 — read the harness contract, write the interview down

No human to interview, so the questions and the assumptions taken in their place are
in `INTERVIEW.md`. The harness ships with the skill and is used verbatim: I supply
`pipeline.json`, a job jar, a deterministic generator and a verifier, and nothing else.

## Step 2 — the pipeline

`job/src/main/java/scaletest/`:

| file | what |
|---|---|
| `Blocks.java` | the domain in one place — symbol/account naming, the deterministic trade at index *i*, scheme weights, and the pro-rata split. The generator, the job and the verifier all call it, so they cannot disagree. |
| `PositionsJob.java` | the DataStream job: `Source: kafka-source -> allocate` → HASH → `symbol-position -> sink` and HASH → `account-position -> sink`. Three vertices, both keyed edges HASH at every parallelism including 1. |
| `GenerateBacklog.java` | fills the topic and writes the manifest (the expected answer, from the input). |
| `VerifyCompleteness.java` | reads both sinks to their end offsets, compares to the manifest with no tolerances. |

Decisions worth naming:

- **The baseline is not allowed to be a different graph.** Both position paths are
  `keyBy`, so the edge is HASH at parallelism 1 as well as 4; nothing chains into a
  single vertex at the baseline. Verified by reading the running plan (`graph_shape`)
  in the smoke test: 3 vertices, `["HASH"]` on both keyed inputs.
- **Allocation conserves quantity exactly.** Slot 0 absorbs the pro-rata remainder, so
  `sum(allocations) == block quantity` for every trade, which is what lets the verifier
  assert equalities rather than tolerances. `writeManifest` asserts the conservation
  itself before writing.
- **Absolute position per key, at-least-once sink.** The value is the whole current
  position, so a replayed record rewrites the same answer. The producer is idempotent,
  so per-partition order is emission order and the last record for a key is its final
  value — which is what the verifier reads.
- **`maxParallelism` pinned to 128** so the key-group → subtask mapping is decided the
  same way in every case.

## Step 3 — smoke test before the chain (measured, not assumed)

A 2M-record drain at 2 cores, through the harness library, before spending an hour:
graph shape as above, the reporter emitting `busyTime/idleTime/backPressuredTime` for
`Source: kafka-source -> allocate`, fan-out exactly 5 (2,000,000 symbol records +
8,000,000 account records for 2,000,000 inputs), verifier exit 0 with 4,096 symbol keys
and 8,192 account keys.

## Step 4 — three things measured, three things fixed

Each of these was found by running it, not by reasoning about it.

**(a) The generator was heap-bound, not disk-bound.** A 24M fill ran at 286k rec/s
where a 2M fill ran at 870k. First guess was VM disk throughput; a `dd` inside the VM
said otherwise. Cause: 8 producer threads × `buffer.memory=256 MB` against `-Xmx2g`.
Dropped to 32 MB per producer and raised the heap to 3g — one change, re-measured:
**40M in 36.3 s = 1.1M rec/s**, and later 160M at 1.38M rec/s.

**(b) The 4-core task manager was killed by the VM.** With the harness example's
memory (768m base + 1280m per core = 5,888m at 4 cores, broker 4 GiB / 3 G heap) the
worker vanished from the network mid-run (`No route to host`, pekko association
timeout) and the job restart-looped. Measured directly: an idle 4-core worker plus the
broker left `MemAvailable` at 2.9 GB, and the worker's heap grows into that under load.
Reduced to **1024m base + 768m per core** (1,792m / 2,560m / 4,096m) and the broker
heap to 1 G. Re-measured: stable at both ends.

**(c) The broker's cgroup limit hits are a function of how much the VM can spare.**
With `kafkaMemory: 3g`, the *1-core* case hit the broker's memory limit **9,437 times**
inside a 60 s window — while the *4-core* case hit it **zero** times. Not a paradox:
at 1 core the worker is small, the VM has 1.65 GB free, the broker's page cache grows
to 2.0 GB and reaches the 3 GB cgroup limit; at 4 cores the VM is already full, global
reclaim trims the cache first and the cgroup limit is never reached. The harness
refuses any case with a single hit, so this would have voided the baseline. Fix: set
the limit **above anything the VM can hand the broker** — `kafkaMemory: 6g` with a 1 G
heap — so global reclaim, which the guard does not count, does the trimming. This is
the over-commitment the task calls normal on this host. Re-measured at 1 core: **0
hits, 0 refaults**, and the rate reproduced to 0.2% (175,211 → 175,544 rec/s).

## Step 5 — backlog sized from a measurement, not a guess

Pre-chain probes on one build, one rig, cases interleaved with the config above:

| cores | rec/s | % of cap | broker | src idle | GC | broker limit hits |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 175,544 | 99.9% | 0.14 / 2.5 | 0.1% | 8.8% | 0 |
| 4 | 667,732 | 99.0% | 0.48 / 2.5 | 3.0% | 1.7% | 0 |

1→4 = 3.80×, inside the tiny proof's 3.0–5.0 bound. Input is 77.5 B/record on the
broker's disk (measured, not estimated). From the 4-core rate:

- `backlog.count = 240,000,000` — the harness wants rate × (90 + 60 + 30) s × 1.5;
  240M covers up to 888k rec/s and is 18.6 GB on disk.
- `tinyCount = 120,000,000` — warm-up deadline 120 s + 30 s window + headroom ≈ 107M.
- `smallCount = 20,000,000` — 114 s at the 1-core baseline rate, eleven checkpoint
  intervals, so the kill at 35% lands 40 s in with several checkpoints behind it.

Every count divides by 8 partitions exactly.

## Step 6 — the chain

`prove.py down` (asserted nothing with the `bt24` prefix survived; fstrim returned
32.1 GiB), then `prove.py all --quick` from cold, detached, waiting on `results/DONE`.
