# Clean-room validation, run 24 — the first run judged on the claim

The harness now gates the claim as well as the measurement: each step ratio
carries an interval from its adjacent pairs, and `meetsClaim` is true only when
the **lower** bound clears 95% of linear. Replayed across the twelve recorded
runs, one clears it — run 12, the heaviest job per record.

This run is the first measured under that gate, and its prompt tells the agent
the claim must pass, so a shortfall is the agent's to fix in its own pipeline
rather than a number to report and move on from.

## What the record predicts

| | |
|---|---|
| true 2→4 on the last build measured | **1.849 ± 2.8%** (13 adjacent pairs, one hour, nothing changed) |
| what two passes resolve | ±3.9% at 95% |
| recorded runs whose interval clears 95% of linear | 1 of 12 |

So the expected outcome is a **fail on 2→4** unless the agent's pipeline is
materially different from the eight that came before it. What the run tests is
whether the diagnosis the harness prints — per-core rate, cap, source idle, GC
and back-pressure for both cases — is enough for an agent to act on.

## The criteria, written before launch

| criterion | note |
|---|---|
| chain passes on the first attempt | run 23 managed it |
| harness verbatim, one suite, 0 forbidden-path reads | every run |
| chain ≤ 60 min | 49.0 min (23), 44.1 (21) |
| every case carries a spread | two-pass quick mode |
| **2→4 meets the claim** — lower bound ≥ 95% of linear | 1 of 12 recorded runs would |
| 1→2 meets the claim | most recorded runs would |
| if the claim fails, the agent says what it changed and what that did | the point of the diagnosis block |

## Response

**The gate worked, the diagnosis was actionable, and the run found the
mechanism nobody here had.** Chain 49 min 31 s, zero refusals, zero ceilings,
`FAIL at report` on the claim. Raw results in [run-24/](run-24/).

| step | ratio | interval | of linear (low) | claim |
|---|---:|---|---:|---|
| 1→2 | 2.040 | [2.016, 2.094] | 100.8% | **met** |
| 2→4 | 1.906 | [1.822, 1.991] | 91.1% | **missed** |

| cores | records/s | passes | spread | % of cap | GC |
|---:|---:|---:|---:|---:|---:|
| 1 | 130,179 | 3 | 2.3% | 100.1% | 5.8% |
| 2 | 265,573 | 2 | 1.8% | 97.3% | 1.3% |
| 4 | 506,286 | 2 | 2.7% | 98.2% | 0.7% |

## The agent moved its own pipeline

Given the failing diagnosis, it changed two things, one at a time, and
measured each:

| change | effect |
|---|---|
| `tmMemoryBase` 768m → 2048m | baseline GC 10.6% → 5.0%, baseline rate **+1.8%** — so the 1-core case was *not* heap-starved. Kept because it makes the baseline faster, which is the harder direction |
| cut ~2.4 KB/record of allocation on the hot path (byte-buffer encoder for identical bytes out) | 4-core throughput **+6.9%**, spreads roughly halved, 2→4 from 1.861 → **1.906**, and 1→2 from missing to meeting |

It declined two changes it judged self-serving: rebalancing memory to hand the
broker's page cache back only in the 4-core case, and re-rolling the same build
for a friendlier pair of passes.

## What it found that we had not

Before offering any mechanism it probed the host itself — one binary, one
variable, two arms, repeated:

```
        per-core ops/s              of linear
cores   alu           mem           alu 2→4   mem 2→4
1       624,104,821   522,968,792
2       612,242,598   526,617,068
4       599,613,923   356,597,412     0.980     0.690
```

**Register-only work doubles at 98% of linear on this host; memory-bound work
returns 69% on the second doubling.** That bounds every 2→4 number in this
record: a pipeline that touches memory cannot reach 95% here, and the days
spent on broker caps, checkpoint intervals, partition counts, network buffers,
compression and fetch sizes were spent inside a range the hardware had already
fixed. It does not attribute a percentage to this pipeline, and the agent said
so.

## Measured, not explained

The residual 4.7% per-core loss at 4 cores; a 4.5% disagreement between the two
2→4 passes on one unchanged build; cap use wandering 95.3–100.4%; and where the
ceiling is — the broker sits at 20% of its CPU cap, so `prove.py ceiling`,
which starves broker CPU, would not find it.

