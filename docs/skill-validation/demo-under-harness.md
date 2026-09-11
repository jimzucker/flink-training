# The demo's own job, measured by the skill's harness

Twenty-eight clean-room runs compared the skill's pipelines against the demo's
published table, and the comparison was never like for like: different job,
different input, and a looser instrument. The demo measures a 60 s window with
no warm-up requirement, one pass per unit, and publishes a case at 62% of its
CPU cap (its 8-unit row). The harness demands warm-up to a flat rate, headroom
at close, two or three passes with an interval, and refuses a case below 95% of
cap.

This is the demo's own `PositionsJob` run through that harness, so the only
thing left varying is the code and the key cardinality.

## What it took

| piece | why |
|---|---|
| `HarnessBacklog` | the demo's generator paces against a clock and asserts nothing; the harness needs a bounded fill and a manifest of expected per-key totals computed from the input alone |
| `HarnessVerify` | drains both position topics and asserts every key exactly, no tolerances |
| `JobConfig.fromArgs` | the harness submits with `--key=value`; the demo reads the environment. With no arguments the result is identical, so the demo path is unchanged |
| `SYMBOL_COUNT` / `ACCOUNT_COUNT` | the demo's universe is four hardcoded symbols; the reverse check needs the same code at a large key space. Unset, it is exactly the four names it always was |

All 48 existing tests pass with the environment unset.

## The results

| arm | worker memory | 1→2 | 2→4 | both claims |
|---|---|---|---|---|
| 4 symbol keys | uncapped (Flink's 1728m default) | 1.986 [1.848, 2.033] | **2.012** [1.954, 2.112] | no — 1→2 missed |
| 4 symbol keys | 1024m + 1024m per subtask | **2.080** [2.036, 2.200] | **1.936** [1.903, 1.969] | **yes** |
| 4,096 symbol keys | 1024m + 1024m per subtask | **2.059** [1.915, 2.179] | **1.988** [1.945, 2.032] | **yes** |
| 32,768 symbol keys | uncapped, then 1024m + 1024m | — | — | **ceiling**: GC 10.2%, then 9.0%, against a 5.5% ceiling |

Rates at 4,096 keys: 58,326 / 120,115 / 238,804 rec/s, GC 3.8 / 0.8 / 0.3%,
every case 96.3–98.1% of cap, no ceilings.

## What it says

**The demo's code scales better, and not because its workload is easier.** At
1,024× the key cardinality its 2→4 did not degrade — 1.988 against 1.936 — and
it is the only pipeline in this record to pass both claims, twice.

Against the skill's pipelines at the same 4,096 keys and the same instrument:

| | 2→4 |
|---|---:|
| demo code | **1.988** |
| [run 24](clean-room-run-24.md) | 1.906 |
| [run 23](clean-room-run-23.md) | 1.793 |

The skill's pipelines are roughly **3× faster in absolute throughput** — they
do less per record — and lose more when the cores double. Both facts are
measured on the same rig with the same guards.

**Where the demo's code fails**: at 32,768 symbol keys it is garbage-collection
bound on this host and cannot be measured at all — one subtask holds every key,
and four cores would need about 10 GB on a 7.8 GB VM. The skill's
[run 21](clean-room-run-21.md) ran at that cardinality and reported 1.865×, but
its GC was 13.2% of capacity, so under today's ceiling it would be classified
the same way. **Neither pipeline can be measured at 32,768 keys on this
machine.**

**And the demo's published 2.08× survives the stricter instrument**: 2.012 with
an interval, three passes, every case at its cap. That number was real, not an
artefact of the looser measurement.
