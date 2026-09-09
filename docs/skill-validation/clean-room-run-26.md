# Clean-room validation, run 26 — memory uncapped, and the guards used as design tools

First run with worker memory uncapped by default and the GC ceiling in its
place ([#82](https://github.com/jimzucker/flink-training/pull/82)), and the
first with ceilings classified rather than deleted ([#76](https://github.com/jimzucker/flink-training/pull/76)).

**One chain, start to finish, no attempt lost or repeated** — 46.0 min
(preflight 1.3 with 18/18, completeness 7.6, tiny proof 7.9, fill 1.8, suite
27.4). Whole run 1 h 40 m.

## The measurement

| cores | passes | rec/s | spread | % of cap | GC | src idle |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 2 | 139,255 | 4.6% | 100.1% | 7.5% | 0.0% |
| 2 | 2 | 316,042 | 0.8% | 97.5% | 4.7% | 0.0% |
| 4 | 2 | 540,772 | **14.6%** | 100.2% | 2.2% | 0.0% |

| step | ratio | interval | of linear (low) | claim |
|---|---:|---|---:|---|
| 1→2 | 2.270 | [2.152, 2.390] | 108% | met |
| 2→4 | 1.711 | [1.479, 1.943] | 74% | **missed** |

`FAIL at report` is the claim gate, not a broken measurement: every case at
97.5–100.2% of cap, broker at 0.36 of 2.5 cores, source never idle, vantage
agreement 0.51%, completeness exact for the same build including a killed
worker that wrote 68,262 duplicates and still landed on every one of 64 symbol
and 8,192 account-symbol positions.

## The guards were used as design tools, which is the point

The agent **rejected three configurations before the chain**, each on a
harness verdict rather than a guess:

| configuration | verdict |
|---|---|
| uncompressed sink | worker only 85.4% of cap — the broker was the constraint |
| 3 GiB broker heap inside a 4 GiB cgroup | no page cache left |
| 1,792m worker at 1 core | GC exactly at the 11% ceiling |

That last one is the new guard doing precisely what it replaced a memory cap
to do: the agent found its own memory floor by measurement instead of being
handed a number that starved something.

One case was classified **CEILING** rather than deleted — the 1-core p2-desc
pass, where the broker hit its 4 GiB limit 3,146 times while the worker held
95.6% of cap. Measured, kept, excluded from the ratios.

## Host probe

```
        1→2     2→4
alu     95.8%   98.3%
mem     90.6%   75.9%
```

86% of linear sits between those bounds. Context, not cause — and the agent
said so rather than claiming it.

## Measured, not explained

- **The 4-core case's 14.6% spread is entirely an order effect**: 501k
  ascending against 580k descending. That is the widest order effect on record
  and it is what puts the 2→4 lower bound at 74%.
- Why 2→4 is short at all. Ruled out with evidence: broker CPU, starved input,
  memory and GC (which *falls* with cores), graph shape read off the running
  plan, uneven partitions, drift.
- The superlinear 1→2 = 2.270.
