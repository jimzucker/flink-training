# Block-trade allocation and running positions: does it scale?

**1→2 cores: 2.17× (109% of linear), range 2.02–2.33× across passes.**
**2→4 cores: 1.85× (92% of linear), range 1.75–1.97× across passes.**

Measured on one growing worker at 1, 2 and 4 cores, three passes per case plus a
sentinel, every case constrained by the worker (95.1–100.4% of its CPU cap) and
every rate read from committed broker offsets. Baseline ratio, quoted second:
**1→4 cores is 4.02× (100% of linear)**, measured against the 1-core case running
the *same* job graph as every other case — the harness read the graph off the
running plan and would have refused any row whose shape differed.

Scope: this is one pipeline on one laptop. It says *this pipeline scaled linearly
from 1 to 4 cores on this rig*. It does not say Flink scales.

| field | value |
|---|---|
| axis | one worker growing: one task manager container capped at N cores, parallelism N, N slots |
| API level | Flink DataStream API, hand-written operators (no SQL, no Table API) |
| guarantee — state | exactly-once checkpointing |
| guarantee — sink | at-least-once, idempotent by emitting the absolute position per key |
| checkpoint interval | 10 000 ms |
| build hash | `6381961fd22c52ef` (completeness passed for the same hash) |
| passes per case | 3, plus the baseline once more as a sentinel |
| rate source | committed broker offsets on `block-trades` (never the engine's meter) |
| CPU source | cgroup `cpu.stat usage_usec` at window open and close |
| backlog | 230,000,000 records, 8 partitions, 5 outputs per input |
| held still | broker 2.5 cores / 4 GB, job manager 0.5 cores, 8 partitions, 10 s checkpoints, 2 GB/partition sink retention, 2560 m worker process memory |

## What it does

A block trade (symbol, side, quantity, price, venue, strategy) arrives on Kafka.
The job **allocates** it across four accounts on a fixed 40/30/20/10 schedule and
maintains two running positions — **net position per symbol** (keyed by symbol)
and **net position per account** (keyed by account) — writing both back to Kafka
as the absolute position for the key. One input becomes **five** outputs, so the
write side is five times the read side: at 4 cores the job consumed 594k trades/s
and wrote 2.97M position updates/s.

```
kafka-source -> parse-trade -> allocate            (one chained vertex)
        |                          |
   keyBy(symbol)              keyBy(account)       (HASH, both)
        v                          v
 symbol-position            account-position
        |                          |
 positions-by-symbol       positions-by-account
```

Three vertices, two HASH edges, identical at every parallelism — verified on the
running plan for all ten cases, so the "chained baseline" artefact that makes
1→N ratios look good is ruled out by a guard, not by assertion.

## The table

| cores | pass | records/s | tm cores | % of cap | throttled | broker cores | src idle | src BP | headroom | vantage |
|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | p1-asc | 154,079 | 1.00 | 100.4% | 100% | 0.09 / 2.5 | 0.0% | 40.0% | 1342 s | 0.40% |
| 2 | p1-asc | 322,907 | 1.99 | 99.6% | 100% | 0.20 / 2.5 | 1.5% | 30.1% | 567 s | 0.31% |
| 4 | p1-asc | 593,313 | 3.80 | 95.1% | 100% | 0.32 / 2.5 | 5.3% | 16.6% | 248 s | 0.31% |
| 4 | p2-desc | 611,800 | 3.83 | 95.8% | 100% | 0.34 / 2.5 | 5.4% | 17.0% | 232 s | 0.35% |
| 2 | p2-desc | 329,033 | 2.00 | 100.0% | 100% | 0.19 / 2.5 | 1.3% | 33.2% | 532 s | 0.16% |
| 1 | p2-desc | 141,040 | 1.00 | 100.1% | 100% | 0.09 / 2.5 | 0.1% | 38.0% | 1486 s | 0.32% |
| 1 | p3-asc | 145,724 | 1.00 | 99.7% | 100% | 0.11 / 2.5 | 0.0% | 38.7% | 1392 s | 0.29% |
| 2 | p3-asc | 310,496 | 2.00 | 100.0% | 100% | 0.21 / 2.5 | 1.1% | 29.6% | 592 s | 0.02% |
| 4 | p3-asc | 575,479 | 4.00 | 99.9% | 100% | 0.40 / 2.5 | 5.6% | 16.8% | 246 s | 0.19% |
| 1 | sentinel | 150,336 | 1.00 | 100.3% | 100% | 0.09 / 2.5 | 0.0% | 38.5% | 1386 s | 0.29% |

| cores | passes | mean records/s | spread | reportable |
|---:|---:|---:|---:|---|
| 1 | 4 | 147,795 | 8.8% | yes |
| 2 | 3 | 320,812 | 5.8% | yes |
| 4 | 3 | 593,531 | 6.1% | yes |

Sentinel — the baseline measured first and last, so a rig that drifted across the
suite would show up as baseline spread: 154,079 → 150,336 rec/s, drift **−2.5%**.
Order effect (descending / ascending): 1c 0.94, 2c 1.04, 4c 1.05.

`src BP` is internal back-pressure inside a capped worker — the source waiting on
aggregation threads that share its core. It is expected, reported, and gated on
nothing. The external boundary is `src idle`, and it stayed at 0.0–5.6%.

## Nothing was lost, and that was proved separately

Completeness is its own run on a backlog small enough to drain to the last
record, twice: clean, and with the task manager killed at 35% of the drain. The
expected answer comes from the generator manifest — from the input — with **no
tolerances**:

| assertion | clean drain | worker killed mid-drain |
|---|---|---|
| distinct symbol keys = 200 predicted | 200 | 200 |
| distinct account keys = 2 000 predicted | 2 000 | 2 000 |
| every per-symbol netQty / tradeCount / notionalCents = manifest | exact | exact |
| every per-account netQty / allocCount = manifest | exact | exact |
| the two paths agree: net qty over symbols = over accounts = manifest | 113,658,400 all three | 113,658,400 all three |

The kill arm re-emitted 76,185 duplicate symbol records and 298,554 duplicate
account records — visible proof the sink is at-least-once — and every final
position was still exactly right, which is what "idempotent by emitting the
absolute position per key" buys. The throughput table is gated on this: the
harness refuses to publish a table for a build that has not passed.

The allocation rule is written **twice** on purpose: once in the generator, to
predict the per-account answer from the input, and once in the job, to compute
it. A shared method would have made the per-account assertion check the pipeline
against itself.

## What was refused, and why

| what refused | why | what I did |
|---|---|---|
| Tiny proof, 4-core case, first attempt | *"task manager used 87.0% of its 4-core cap (floor 95%) — it is not the constraint."* The rate decayed monotonically through the drain: 599k → 541k → 406k → 404k → 356k rec/s. | Measured, did not guess. The broker's cgroup showed **71,717 `memory.events max`** hits, 4.49M pages scanned and stolen, and 1.21M working-set file refaults, with its page cache pinned at 412 MB by a 2 GB container limit — while the VM as a whole had 3.6 GB free. Raised the broker's memory limit 2 GB → 4 GB, one variable, same rig, same build, re-measured: **99.7% of cap**, no decay, 617k rec/s. |
| Tiny proof, 4-core case, second attempt | *"vantage points disagree by 20.2%."* | From the tick series: the closing tick landed on a partially committed checkpoint — one boundary split into two ticks 0.5 s apart (3.03M then 2.98M records) because the four source subtasks commit independently. Measured frequency: **1 split in 21 recorded commit boundaries** across four cases, and none at parallelism 1. Left alone rather than shortening the checkpoint interval (the harness's own record says 2 s checkpoints cost the worker 6–17 points of cap fraction). Re-ran; the published suite's ten cases disagreed by ≤ 0.40%. |
| Guard self-test | 29 guards broken on purpose — wrong cap, busy cluster, truncated backlog, dead sampler, no job running, host-side watcher, spread ceiling, sentinel drift, disk budget, chain-stops-at-first-failure. | **29/29 fired as expected**, for this build, before the suite was allowed to start. |
| The published suite | Nothing. Ten cases, ten OK. | — |

Preflight was 13/13 PASS: native arm64 images, host JDK 17 matching the engine
image, writable state directory, no duplicated metrics reporter, host disk
budget, retention on both undrained sink topics, a byte-identical manifest from
two runs of one seed, one CPU-cap mechanism read back from `NanoCpus` and
`cpu.max`, slots ≥ parallelism, per-run consumer-group scoping.

## Where it stops — and what I could not explain

**The broker's CPU is not the ceiling anywhere near here.** Holding the worker at
4 cores and squeezing the broker in steps:

| broker cap | broker used | worker % of cap | records/s | src idle |
|---:|---:|---:|---:|---:|
| 2.5 cores | 0.40 (16%) | 100.1% | 552,950 | 5.1% |
| 1.0 core | 0.35 (36%) | 99.6% | 539,985 | 5.8% |
| 0.5 core | 0.36 (73%) | 99.8% | 569,284 | 5.1% |

The handover never happened: the broker never pinned and the worker never fell
off its cap; the three throughputs sit inside the 6.1% spread of the 4-core case.
So the ceiling was **not located**, and I am not going to name one.

Two things I measured and did not explain:

- **1→2 came out 9% above linear.** Ruled out with evidence: the job graph is
  identical on all ten cases (read off the running plan, guard-enforced); the
  1-core case was not under-using its cap (99.7–100.4%, throttled in 100% of
  periods); the broker used ≤ 0.11 of 2.5 cores at the baseline; the backlog had
  1342–1486 s of headroom; the order effect is 0.94–1.05 and the sentinel drifted
  −2.5%. What is left is per-thread behaviour, and there the only number I have
  is that garbage collection took **9.6%** of the 1-core case's capacity against
  5.9% at 2 cores and 5.5% at 4. That is a correlation across three cases, not a
  controlled comparison — I did not run one rig, one build, one variable — so it
  is a hypothesis and not the cause. **I do not know yet.**
- **Source idle rises 0.0% → 1.3% → 5.4% with core count**, alongside the 92% of
  linear at 2→4. Broker CPU is ruled out by the ceiling table above. Cause
  unknown.

There is no dashboard. The rig has 8 CPUs and the measurement allocates 7 of them
(worker 4 + broker 2.5 + job manager 0.5); a Prometheus/Grafana pair inside the
stack would have competed for the last one, and the 4-core case already measured
95.1% of cap on one pass against a 95% floor. A panel that costs you the thing
under test is worse than no panel.

## Wall clock

| phase | span | duration |
|---|---|---|
| write the pipeline, generator, verifier; build the jar | 17:41 → 17:53 | 12 min |
| stack up + preflight (calibration) | 17:53 → 17:55 | 2 min |
| completeness, first run (correctness gate + 1-core rate for sizing) | 17:55 → 18:03 | 8 min |
| tiny proof arm A, broker at 2 GB — REFUSED at 87% of cap | 18:05 → 18:13 | 8 min |
| diagnose the broker's memory pressure, change one variable, restack | 18:13 → 18:18 | 5 min |
| tiny proof arm B, broker at 4 GB — REFUSED on a split commit tick | 18:18 → 18:24 | 6 min |
| analyse the tick series, size the backlogs from the measured rate | 18:24 → 18:29 | 5 min |
| **the published chain — `prove.py all`** | **18:28:39 → 19:26:20** | **57.7 min** |
| &nbsp;&nbsp;└ up | | 2 s |
| &nbsp;&nbsp;└ preflight | | 67 s |
| &nbsp;&nbsp;└ completeness (clean drain + worker killed) | | 464 s |
| &nbsp;&nbsp;└ tiny proof + 29-guard self-test | | 403 s |
| &nbsp;&nbsp;└ fill, 230M records at 2.2M rec/s | | 106 s |
| &nbsp;&nbsp;└ suite, 10 cases | | 2419 s |
| &nbsp;&nbsp;└ report | | 0 s |
| ceiling probe, three broker caps | 19:30 → 19:43 | 13 min |
| teardown, asserted, `fstrim` returned 22.4 GiB | 19:45:54 → 19:45:59 | 5 s |
| **total** | **17:41 → 19:46** | **2 h 05 min** |

Nineteen of the 36 minutes between the stack coming up and the published chain
starting went on the two tiny-proof refusals and the measurement that resolved them. Both would have voided the whole 40-minute suite had they landed inside
it. That is what the tiny proof is for.

## Teardown

`prove.py down` removed every container, volume and network prefixed `st13`,
reaped two anonymous volumes, asserted nothing survives, and ran `fstrim`, which
returned 22.4 GiB to the host. Re-checked afterwards by hand: no `st13` container,
volume or network survives, and no host process this run started is alive. Host
free disk is back to 126 GB.

## The interview that could not happen

No human was available. The questions I would have asked, one at a time, and the
assumptions I took in their place are in `JOURNAL.md`. The ones that most affect
how you should read the number:

- **Symbols are drawn uniformly at random.** Real block-trade flow is skewed, and
  a skewed key distribution would put a partitioning story on top of the scaling
  story. This claim is scoped to uniform keys.
- **Fan-out is exactly 5×** and cardinality is fixed (200 symbols, 2 000
  accounts), which is what makes the completeness assertions exact rather than
  tolerances.
- **The claim includes the 1-core case**, so it was run — the skill would
  otherwise leave out the structurally weakest and noisiest case. It behaved:
  8.8% spread across four passes, at 100% of its cap in every one.

## Raw results

Everything is under `results/`: `suite.json` / `suite.md` / `suite.txt` (the
table and per-pass records, including the sampler tick series per case),
`completeness.json`, `tinyproof.json` and the two refused arms
(`tinyproof-armA-kafka2g.json`, `tinyproof-armB-kafka4g.json`), `selftest.json`,
`preflight.json`, `ceiling.json`, `phases.log`, `all.json`, `harness.log`, and
the generator manifests. Every number in this report is generated from those
files; none was retyped from a screen.
