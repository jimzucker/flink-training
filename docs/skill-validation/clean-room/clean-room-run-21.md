# Clean-room validation, run 21 — the chain is clean, the setup is not

First run with all three fixes in place: memory per subtask (#62, #63), two
passes in `--quick` (#61), and the tiny proof sizing the backlog from the rate
it measures (#66).

## Result

**The chain passed on its first attempt, in 44.1 min, with every case carrying
a spread.** 1→2 = **1.959×** (1.919–1.999), 2→4 = **1.865×** (1.816–1.916).

| cores | records/s | passes | spread | % of cap | GC |
|---:|---:|---:|---:|---:|---:|
| 1 | 198,906 | 2 | 1.1% | 99.5% | **26.4%** |
| 2 | 389,593 | 2 | 2.9% | 100.1% | 13.7% |
| 4 | 726,636 | 2 | 2.4% | 98.1% | 8.5% |

The backlog sizing check did its job silently — no attempt was lost to it, the
first time since run 14.

## Where the time went

| | |
|---:|---|
| **44.1 min** | the accepted chain (preflight 45 s, completeness 440, tiny proof 431, fill 108, suite 1,623) |
| 24.3 min | **three tiny proofs refused by the broker-memory guard** |
| 21.9 min | the agent's own controlled memory experiments |
| 6.8 min | build |
| ~4 min | write-up and teardown |
| **1 h 49 m** | total |

## The remaining blocker, and it is computable

Broker memory. The agent raised `kafkaMemory` three times — 10,648 → 1,136 →
995 limit hits — before landing on 6,144 MiB. Run 20 lost two attempts the same
way. Neither was a hard problem: the working set is a function of partitions,
retention and the backlog, all of which the harness knows before anything runs.

The agent's own controlled comparison is worth keeping: the guard was firing on
the **1-core** case, not the largest one, because on an idle VM the broker's
page cache grows into its cgroup limit — raising the limit 3,840 → 6,144 MiB
took limit hits 995 → 0. Its first explanation predicted the opposite and the
measurement killed it.

## Measured, not explained

- **GC at 26.4% on the 1-core case.** The 8.2 GB VM forces a split: the agent
  paid for the broker's cache out of the worker, choosing `1216m + 448m` per
  subtask, so the 1-core case ran on 1,664m. The ratios still came in with
  spreads under 3%, but the smallest case is uncomfortable and that is not
  a tuning question the run answered.
- **2→4 falls further short than 1→2** (1.865× against 1.959×). The agent
  ruled out the broker (0.29 of 2.5 cores), the partition split, graph shape,
  case order and cap consumption, and notes GC favours the *larger* cases. No
  cause offered.
- One in-chain refusal: the 1-core sentinel at 94.5% of cap, case-scope, so the
  case still reported on two passes — but the drift check is unavailable.
