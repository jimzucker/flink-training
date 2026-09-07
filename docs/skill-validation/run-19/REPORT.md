# Block trades → allocations → running positions: a quick look at how it scales

> **QUICK LOOK — not a result.** The harness stamped this table
> `quickLook: true`, `publishable: false` and printed this banner on it:
>
> *"one pass per case. No spread, so no table: these numbers say the rig ran
> clean and roughly how fast, and nothing about how repeatable the ratio is.
> The record's own passes read 2.04-2.27x where a suite reported 2.15x.
> Do not publish or quote."*
>
> `prove.py all --quick` runs two passes per case instead of the configured
> three. Every per-case guard stays live, so the table says the rig ran clean
> and roughly how fast. It says nothing dependable about the ratios, and the
> two ratios below are **not** presented as a proven result.

## Headline

| step | ratio | efficiency | spread across passes |
|---|---:|---:|---|
| **1 → 2 cores** | **2.19×** | 110% of linear | not measured — one pass per case in quick mode |
| **2 → 4 cores** | **1.78×** | 89% of linear | not measured — one pass per case in quick mode |

The 1→2 figure is superlinear, and I do not believe it. The 1-core baseline
moved 11.2% across the suite (see the sentinel, below), which is larger than
the amount by which 2.19× exceeds 2.00×. Computed against the baseline's last
measurement instead of its mean, the step is ≈2.09×. Take both ratios as
"roughly two, and roughly one-and-three-quarters".

## Header fields

| field | value |
|---|---|
| axis | one worker growing: one task-manager container capped at N cores, parallelism N, N slots (all three read back from the engine per case) |
| API level | Flink 1.20 DataStream API, hand-written operators — no SQL, no Table API |
| guarantee | state: **exactly-once checkpointing**; sink: **at-least-once, idempotent** by emitting the absolute position per key |
| checkpoint interval | 10 000 ms |
| build hash | `248e47691f1d1056` (completeness passed for the same build) |
| passes per case | 2 (quick mode; 3 configured), plus the sentinel |
| rate source | committed broker offsets on `block-trades` — never the engine's meter |
| CPU source | cgroup `cpu.stat usage_usec` — never `docker stats` |
| held still | broker cap 2.5 cores, job-manager cap 0.5, 8 partitions, 240 M-record backlog, 2 GiB/partition sink retention, worker process memory 2048m |

## The table

| cores | pass | records/s | tm cores | % of cap | throttled | broker cores | src idle | src BP | headroom | vantage |
|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | p1-asc | 172,397 | 1.00 | 100.3% | 100% | 0.16 / 2.5 | 0.2% | 37.9% | 1253 s | 0.24% |
| 2 | p1-asc | 405,858 | 1.99 | 99.6% | 98% | 0.38 / 2.5 | 1.8% | 33.6% | 450 s | 0.24% |
| 4 | p1-asc | REFUSED (case) — worker used 94.1% of its 4-core cap (floor 95%) | | | | | | | | |
| 4 | p2-desc | 718,138 | 3.86 | 96.4% | 90% | 0.74 / 2.5 | 3.5% | 29.3% | 192 s | 0.15% |
| 2 | p2-desc | 399,564 | 1.93 | 96.6% | 99% | 0.36 / 2.5 | 1.6% | 35.6% | 460 s | 0.27% |
| 1 | p2-desc | 186,080 | 1.00 | 100.2% | 100% | 0.16 / 2.5 | 0.1% | 41.6% | 1147 s | 0.30% |
| 1 | sentinel | 192,899 | 0.99 | 99.5% | 100% | 0.16 / 2.5 | 0.1% | 40.8% | 1100 s | 0.26% |

| cores | passes kept | mean records/s | spread | reportable |
|---:|---:|---:|---:|---|
| 1 | 3 | 183,792 | 11.2% | yes |
| 2 | 2 | 402,711 | 1.6% | yes |
| 4 | 1 | 718,138 | not measured (one pass survived) | yes, in quick mode only |

Output records are 5× these numbers: at 4 cores, 718 k trades/s in and
3.59 M position updates/s out.

**Sentinel** (the baseline measured first and last, so a rig that drifts
across the suite shows up rather than hiding): 1 core read 172,397 rec/s at
the start and 192,899 rec/s at the end — **+11.2% drift**, counted inside that
case's 11.2% spread. That drift is the reason the 1→2 ratio is not to be
trusted at two decimal places.

**Order effect** (descending / ascending): 1 core 1.079, 2 cores 0.985.

## What was refused, and why

| what | where | why |
|---|---|---|
| the 4-core case, pass p1-asc | suite | worker used **94.1%** of its 4-core cap against a 95% floor — "it is not the constraint". A refusal about one case's data, so the suite marked it and carried on; the 4-core case therefore rests on a single surviving pass. |
| the whole chain, first attempt | tiny proof, 22:33 | the 4-core case read **91.6%** of cap. `all` stops at the first failing step, so nothing downstream ran. This is the same guard firing on a rig that had been under load for half an hour. |
| nothing else | — | preflight 14/14 PASS; the guard self-test broke **35 of 35** guards on purpose and every one refused as expected; completeness passed twice for this build. |

Completeness, with no tolerances, on a 10 M-record backlog drained to the last
record — once clean and once **with the task-manager container killed at 35%
of the drain**: 64 symbol keys and 512 account keys exactly as predicted in the
interview, every per-key position equal to the generator manifest, both paths
summing to the same total over 10 M trades and 40 M allocation legs. The
guarantee is the one configured, tested by killing something.

## What I did not explain

**Why the worker sits just under its cap at 4 cores, sometimes.** At 4 cores it
read 98.5% of cap in the tiny proof, 96.4% in one suite pass and 94.1% in the
other — straddling the floor. The broker was at 0.74 of its 2.5-core cap and
the source idled 3.5%, so it was **not** starved from outside; the worker is
blocking on something internal. I do not know what.

I tried three mechanisms and **all three are void**, because the control
proved the rig itself was moving faster than the effects I was measuring:

| arm | change | records/s | % of 4-core cap |
|---|---|---:|---:|
| A | baseline | 603,768 | 88.0% |
| B | bigger producer batches (linger 50 ms, 512 KB) | 500,497 | 72.4% |
| C | smaller producer batches (linger 0, 64 KB) | 530,695 | 67.4% |
| D | worker memory 2048m → 3584m | 375,990 | 52.4% |
| **A′** | **baseline again, nothing changed** | **535,148** | **59.5%** |

A′ is the same build and the same configuration as A and read 59.5% where A
read 88.0%. So B, C and D measure the drift, not their own variables, and I am
withdrawing all three. What *is* measured is the drift: **this rig loses
throughput under sustained load and recovers when idle** — after a ten-minute
idle the same build read 718 k rec/s at 98.5% of cap. Thermal throttling is
the obvious guess and I have not measured it, so it stays a guess.

Two hypotheses I did test and can rule out for the 4-core shortfall:
checkpoint barrier alignment (checkpoints complete in 48–125 ms on a 10 s
interval) and broker page-cache starvation (zero container memory-limit hits,
3 file-page refaults inside the window).

**Where it stops** is also unexplained: no `ceiling` run was made. At the
4-core case the broker used 0.74 of 2.5 cores and the source idled 3.5%, so
the broker was not yet the constraint — the ceiling is above 4 cores and its
location is unknown.

## Wall clock

The accepted chain, from `results/phases.log`:

| phase | duration |
|---|---:|
| up | 0.03 min |
| preflight | 1.1 min |
| completeness | 8.1 min |
| tiny proof (incl. 35-guard self-test) | 9.0 min |
| fill (240 M records, 32 GB) | 5.3 min |
| suite (7 cases) | 27.6 min |
| report | 0.0 min |
| **total (`results/DONE`)** | **51.1 min** |

The whole session, including the work the chain does not see:

| stage | duration |
|---|---:|
| build the job, generator and verifier; end-to-end correctness check | ~30 min |
| calibration drains at 1 and 4 cores, to size the backlogs from measured rates | (included above) |
| first chain attempt, stopped by the tiny proof at 4 cores | 18 min |
| four experiment arms plus the control that voided three of them | 21 min |
| idle, to let the rig recover | 10 min |
| **accepted chain** | **51.1 min** |
| teardown, asserted | 1 min |
| **total** | **≈ 2 h 20 min** |

## Scope

This is one pipeline on one laptop. It supports "**this** pipeline scaled this
way on this rig", not "Flink scales". And because it is a quick look, it does
not support even that at two decimal places — the harness says so itself, at
the top of this page.

## What was built

`job/` — one jar, three entry points:

- `blocktrades.PositionsJob` — the DataStream job. Kafka source → parse →
  allocate each block across 4 accounts, then two hash edges into two keyed
  aggregations (running position by symbol; running position by account),
  each chained to its own Kafka sink. Three vertices at every parallelism,
  because a `keyBy` is a hash edge at parallelism 1 as much as at 4 — so the
  baseline runs the same graph as every other case, which is the difference
  the skill measured as 2.16× against 3.26×. The harness read the shape off
  the running plan and compared it across every case.
- `blocktrades.GenerateBacklog` — the deterministic generator. Record *i* is a
  pure function of (seed, *i*); two manifests from one seed are byte-identical
  (preflight checked it).
- `blocktrades.VerifyCompleteness` — reads both sinks and compares to the
  manifest with no tolerances, exiting non-zero on any miss.

Fan-out is exactly 5 output records per input (1 by-symbol + 4 by-account), so
the harness's two-vantage check divides by an exact constant; transport and
sink agreed to within 0.15–0.30% on every case.

The harness was used **verbatim** — nothing in `lib.py` or `prove.py` was
edited and no threshold was touched. Raw results are under `results/`
(`suite.json`, `suite.md`, `tinyproof.json`, `selftest.json`,
`completeness.json`, `preflight.json`, `phases.log`, `all.json`, `DONE`).
The interview I could not hold, and the assumptions taken in its place, are in
`JOURNAL.md`.

Teardown asserted: no container, volume or network with the `bt20` prefix
survives; `fstrim` returned 48.6 GiB to the host.
