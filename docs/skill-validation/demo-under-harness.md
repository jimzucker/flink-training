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

Measured 2026-09-10. Two of the three arms were quick looks — two passes per
case, which the harness marks not publishable — and none recorded which garbage
collector each case ran.

| arm | worker memory | passes per case | 1→2 | 2→4 |
|---|---|---:|---|---|
| 4 symbol keys | uncapped (Flink's 1728m default) | 3 | 1.986 [1.848, 2.033] | **2.012** [1.954, 2.112] |
| 4 symbol keys | 1024m + 1024m per subtask | 2 | 2.080 [2.036, 2.200] | **1.936** [1.903, 1.969] |
| 4,096 symbol keys | 1024m + 1024m per subtask | 2 | 2.059 [1.915, 2.179] | **1.988** [1.945, 2.032] |
| 32,768 symbol keys | uncapped, then 1024m + 1024m | — | — | ceiling: GC 10.2%, then 9.0%, against a 5.5% limit |

Rates at 4,096 keys: 58,326 / 120,115 / 238,804 rec/s, GC 3.8 / 0.8 / 0.3%,
every case 96.3–98.1% of cap.

The 4,096-key arm was measured again on 2026-09-24 with the same jars and
configuration and three passes per case
([article-gap.md](article-gap.md)):

| | 1-core collector | 1→2 | 2→4 |
|---|---|---:|---:|
| as configured | Serial (Java's own choice in a one-CPU container; G1 above it) | 2.01 [1.92, 2.06] | 1.96 [1.89, 2.07] |
| G1 on every case, nothing else changed | G1 | **1.91** [1.85, 1.97] | **1.99** [1.95, 2.10] |

The Serial baseline is slower and collects more, so the step off it reads high.
The harness now runs G1 on every case, so like for like this build reads
**1.91× / 1.99×**.

## What it says

**The demo's code holds its 2→4 as the key count grows.** At 1,024× the key
cardinality its 2→4 did not degrade — 1.988 against 1.936 — and with G1 on
every case it reads 1.99×.

Against the skill's pipelines at the same 4,096 keys and the same instrument:

| | 2→4 |
|---|---:|
| demo code | **1.988** |
| [run 24](clean-room/clean-room-run-24.md) | 1.906 |
| [run 23](clean-room/clean-room-run-23.md) | 1.793 |

The skill's pipelines are roughly **3× faster in absolute throughput** — they
do less per record — and lose more when the cores double. Both facts are
measured on the same rig with the same guards.

**1→2 above 2× is not a faster pipeline.** The 1→2 figures in the first table
come from a 1-core case running a different garbage collector from the cases
above it; with G1 everywhere the same build reads 1.91×.

**Where the demo's code stops**: at 32,768 symbol keys it spent 9–10% of its time
on garbage collection on this host — one subtask holds every key, and four cores
would need about 10 GB on a 7.8 GB VM. The skill's
[run 21](clean-room/clean-room-run-21.md) ran at that cardinality and reported
1.865×, with GC at 13.2% of capacity. Neither has been measured at 32,768 keys
under the harness's current rules.
