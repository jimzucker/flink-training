# Clean-room validation, run 27 — the demo's whole job, not half of it

Every run since 17 asked for positions only. The demo does more: positions
**and** market value at close on a ten-second tumbling window, with the price
taken at the window boundary, and correctness proved exactly. That is the
workload the skill is supposed to be able to produce, and comparing a
positions-only pipeline against the demo's numbers was comparing two different
jobs — the demo spends ~24 µs of CPU per record, the positions-only runs ~7 µs.

This run asks for the whole thing, on the harness at #83.

## What the demo does, measured on `main` today

| units | orders/sec | vs previous | Flink cores |
|---:|---:|---|---:|
| 1 | 35,399 | — | 1.00 |
| 2 | 80,974 | 2.29× | 2.00 |
| 4 | 168,200 | 2.08× | 3.98 |

One pass per unit, its own script, no spread — the harness's stricter view of
the same machine is that a two-pass ratio carries about ±4%.

## The criteria, written before launch

| criterion | note |
|---|---|
| the job does both halves: positions by symbol and account, **and** market value at close on a 10 s tumbling window, price at the boundary | the demo's requirement |
| completeness passes with no tolerances, clean and with a worker killed mid-drain | including the windowed outputs |
| chain passes on the first attempt | run 26 managed it |
| chain ≤ 60 min | 46.0 min (26) |
| every case at 95–101% of cap, GC under the 11% ceiling | the guards, unchanged |
| **2→4 meets the claim** — lower bound ≥ 95% of linear | run 17, the last full-workload run, read 1.946× point estimate on the old harness |
| 1→2 meets the claim | |
| CPU µs per record reported per case | the number that separates this from the positions-only runs |

## Response

**The skill built the whole project.** One chain, 47.1 min, `FAIL at report` on
the claim — not on the measurement.

| step | ratio | interval | of linear (low) | claim |
|---|---:|---|---:|---|
| 1→2 | 2.426 | [2.402, 2.451] | 120% | met |
| 2→4 | 1.853 | [1.824, 1.881] | 91.2% | **missed** |

| cores | rec/s | spread | % of cap | GC | µs CPU per record |
|---:|---:|---:|---:|---:|---:|
| 1 | 108,886 | 7.8% | 99.8% | 5.8% | 9.18 |
| 2 | 264,182 | 6.8% | 99.6% | 5.9% | 7.53 |
| 4 | 489,595 | 8.3% | 100.2% | 2.9% | 8.19 |

Six outputs per input — positions by symbol and by account, plus market value
by symbol and by account, once per key per ten-second window.

## Correctness, which is the part that matters here

16 assertions, no tolerances, passing on a clean drain **and** on a drain with
the task manager killed at 40%:

- exact final position for all 32 symbols and all 64 accounts
- the two aggregation paths summing to the blocks' signed quantity
- **exact market value for all 384 (symbol, window) and all 763 (account,
  window) pairs**, with the window *set* predicted from the input's watermark
  arithmetic, and every replayed window record byte-identical to the one it
  repeats

Expected values come from a single-threaded reference over the generator seed,
never from the pipeline.

## Two real defects in the windowed half, each pinned by experiment

| defect | how it was found |
|---|---|
| an 8 MB per-partition fetch made the watermark lag most of the backlog | one-variable experiment, not argument |
| a source watermark generator is not checkpointed, so a fully-consumed price topic pinned the watermark at `MIN_VALUE` after a restart — the killed arm produced **zero** windowed outputs | completeness refused the killed arm; the fix moves "all prices at or before this boundary have arrived" out of the source and into keyed state |

Neither is visible without a correctness check that covers the windowed
outputs under failure. This is what the positions-only runs could not have
found.

## Against the demo, same machine, same day

| | 1 | 2 | 4 | 1→2 | 2→4 |
|---|---:|---:|---:|---:|---:|
| demo (`scale-units.sh`, one pass) | 35,399 | 80,974 | 168,200 | 2.29× | 2.08× |
| run 27 (two passes, intervals) | 108,886 | 264,182 | 489,595 | 2.426× | 1.853× |

The skill's pipeline is ~3× faster per core at the same job description, and
its second step is 0.23 short of the demo's. The demo's figure carries no
spread; the harness's own view of this machine is that a two-pass ratio wanders
about ±4%, which covers most of that gap but not all of it.

## What was refused

The job's own startup assertion refused a defective copy of Flink's key-group
hash; completeness refused the killed arm before the watermark fix; the tiny
proof refused the 1-core case at GC 11.8% against the 11% ceiling; the suite
classified one 1-core pass as a **CEILING** (broker hit its memory limit 323
times) and excluded it; 38/38 self-test guards fired on purpose; `report`
refused the claim.

## Measured, not explained

Why 2→4 gives 92.7% — cap ownership, broker, memory, GC, key skew, graph shape
and backlog headroom all ruled out, with the host probe bounding it between 98%
register-only and 75% memory-bound. Why 1→2 is superlinear. And why the rig ran
~8% faster later in the suite: sentinel drift **+7.8%**, order effect 1.07–1.09.

## Cost

2 h 08 m: 47.1 min accepted chain, 52 min rework (three completeness runs, two
tiny proofs, three diagnostic probes), 24 min first-time build. The agent also
capped worker memory itself (`2048m + 256m` per core) although the default is
now uncapped — the shipped example still carries those keys, which is what it
copied.

