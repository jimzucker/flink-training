# Clean-room validation, run 28 — the full project again, to see if it repeats

[Run 27](clean-room-run-27.md) built the whole job — positions *and* market
value at close on a ten-second window — proved it exactly under a killed
worker, and read 2→4 = **1.853×** [1.824, 1.881], missing the claim at 91.2%
of linear on the lower bound. That is one measurement of the real workload.

This run repeats it, on the harness at #85, with the example now shipping no
worker memory keys so an uncapped default is what a new pipeline inherits.

## The criteria, written before launch

| criterion | run 27 |
|---|---|
| the job does both halves, with the windowed outputs proved exactly under a killed worker | 16 assertions, no tolerances |
| chain passes on the first attempt | yes, 47.1 min |
| chain ≤ 60 min | 47.1 min |
| every case at 95–101% of cap, GC under 11% | 99.6–100.2%, GC 2.9–5.9% |
| **2→4 meets the claim** — lower bound ≥ 95% of linear | missed at 91.2% |
| 1→2 meets the claim | met at 120% |
| **the two runs agree inside their intervals** | 2→4 interval was [1.824, 1.881] |

**The question**: is 1.853× a property of this workload on this machine, or one
run's luck? Run 27's own sentinel drifted +7.8% and its order effect ran
1.07–1.09, so the interval may be narrower than the truth.

## Response

**It repeats, and it still misses.** One accepted chain, 52.1 min.

| run | 2→4 | interval | of linear (low) | claim |
|---|---:|---|---:|---|
| 27 | 1.853 | [1.824, 1.881] | 91.2% | missed |
| **28** | **1.909** | [1.779, 2.038] | 89.0% | missed |

Run 28's point estimate falls **outside** run 27's interval, which answers the
other question the run was launched with: a two-pass interval is narrower than
the truth. Taken together the workload's 2→4 is about **1.88** on this machine,
and 95% of linear is not reached.

| cores | rec/s | passes | spread | % of cap | µs CPU per record |
|---:|---:|---:|---:|---:|---:|
| 1 | 186,252 | 3 | 7.7% | 98.6% | 5.29 |
| 2 | 396,981 | 2 | 0.6% | 99.7% | 5.02 |
| 4 | 757,704 | 2 | 7.5% | 99.8% | 5.27 |

1→2 = 2.131 [1.975, 2.329], met. No ceilings, sentinel drift −1.5%, vantage
disagreement ≤0.48%.

## Correctness

No tolerances, clean drain and worker killed at 35%: exact key sets, a per-key
update counter walked 1..K with no gap (replay rewinds allowed and counted),
every final position exact, **every one of 3,600 (key, window) market values
present as the exact expected set and equal to the manifest**, market value =
quantity × price on the record, and two independent paths agreeing exactly.

The agent's own note on why that is assertable at all: the window is
event-time, the backlog span lands mid-window, and the close is a real
snapshot — quantities accumulate as a per-window delta and prices are held by
their close instant, neither applied on arrival.

## Four defects, each found by measuring

| defect | how it surfaced |
|---|---|
| the price stream racing the trade stream | every market value wrong while every position was right |
| watermark alignment costing half the throughput | an 88% idle source |
| an exhausted price source stalling the watermark forever after a worker kill | **only** the kill arm caught it — the same class run 27 found independently |
| a silent 8.8M-record short fill from an `OutOfMemoryError` that `catch (Exception)` missed | the generator's own read-back |

GC and checkpointing were both *ruled out* as causes of a 4-core shortfall that
turned out to be a probe backlog draining inside its own window.

## And a defect in our measurement

The agent reported that the harness double-counts garbage collection. It was
right: Flink 1.20 emits `GarbageCollector.All.Time` alongside each real
collector, and the harness summed every `.Time`. Confirmed on a running task
manager — `All.Time 15`, `G1 Young Generation.Time 15`, `G1 Old Generation.Time
0`. Every GC figure recorded here before 2026-09-09 is double, and the 11%
ceiling was derived from doubled numbers; both are fixed in
[#86](https://github.com/jimzucker/flink-training/pull/86), with the ceiling
re-derived at 5.5%.

## Measured, not explained

Why 1→2 is superlinear; why the descending pass read 7.8% higher at 4 cores;
where the remaining 4.6% of 2→4 goes, with the host probe putting this
machine's memory-bound 2→4 at 76%; and where the ceiling is — four cores is
not it.

## Cost

2 h 39 m: 52.1 min accepted chain, 23.1 min in discarded chain attempts, ~76
min of build and diagnosis before the first chain, ~8 min teardown and report.

