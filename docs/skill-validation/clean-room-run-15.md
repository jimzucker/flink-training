# Clean-room validation, run 15 — the same quick look, with the broker guard

Run 14's prompt again — `prove.py all --quick`, cases 1, 2 and 4, one pass
each — on the harness at #54, which adds the guard that run 14 needed and did
not have: a case is refused if the broker hit its container memory limit
inside the measurement window.

Run 14 reported **2→4 = 1.35×**. That number was measured on a starved
broker; the diagnosis is in [rig-2026-09-05-broker.md](rig-2026-09-05-broker.md)
and the fix in #54. This run asks whether the skill, unaided, now either
avoids the trap or is refused by it — and what the step ratios read when the
worker really is the constraint.

## The criteria, written before launch

| criterion | what the record says |
|---|---|
| the chain passes on the first attempt — no phase rerun, no fallback, no wrapper | run 14 did |
| harness verbatim (`prove.py all --quick`), one suite, 0 forbidden-path reads | runs 11–14 |
| chain `preflight` → `report` ≤ 45 min | 37.1 min (run 14) |
| whole-run wall clock (reported, not judged) | 55.9 min (14), 2 h 05 m (13) |
| **no case refused for broker memory** — the agent sizes the broker so the guard never fires | run 14 shipped 2 GiB and its 4-core case was starved; the rig needed 4 GiB for a 264M backlog |
| every case at 95–101% of cap | 99.9 / 98.4 / 95.9 (14) |
| sentinel measured, drift reported | +2.3% (14) |
| the table is stamped unpublishable | `--quick` sets `quickLook` / `publishable: false` |
| ratios | **recorded, not judged** — one pass carries no spread. For context, the same build measured three passes per case with a fed broker read 2→4 = 1.725× |

## Response

Model: Claude Opus 5. Harness at #54, run from the skill directory, nothing
copied or imported. 155 tool calls, 0 forbidden-path reads. 16:04 → 17:55
local. Raw results in [run-15/](run-15/).

| criterion | result |
|---|---|
| chain passes on the first attempt | **FAIL** — three `all --quick` attempts: the first abandoned at the fill once the tiny proof showed 880k rec/s against the 450k the agent had sized for, the second refused at the tiny proof (4-core headroom 6.9 s < one 10 s checkpoint), the third **PASS** |
| harness verbatim, one suite, 0 forbidden reads | **PASS** — 155 tool calls, 0 hits; one suite, retried once under the rule in SKILL.md §"Retry the same case once if the failure is plainly transient" |
| chain ≤ 45 min | **PASS — 37.9 min** (up 2 s, preflight 61, completeness 462, tinyproof 529, fill 292, suite 929) |
| whole-run wall clock (reported) | **1 h 51 m** — 19 min building, 36 min in the two abandoned attempts, 38 min the chain, 16 min the suite retry |
| **no case refused for broker memory** | **PASS** — the agent sized the broker at 5 GiB unprompted; the guard was live in all four cases and recorded **0 limit hits** with 2.19–2.36 GB of file cache |
| every case at 95–101% of cap | **PASS on the reported passes** — 99.4 / 100.4 / 97.7%, sentinel 99.4%. The first suite attempt refused 1c at 95.0% and 2c at 94.7%; retried once unchanged, both then passed |
| sentinel measured | **PASS** — −3.1% |
| table stamped unpublishable | **PASS** — `quickLook: true`, `publishable: false`, banner in both renderings, and the agent's report repeats it |
| ratios (recorded, not judged) | 1→2 = **2.137×**, 2→4 = **1.836×** |

Build ran 320M → 400M records, 8 partitions, 10 s checkpoints, **five**
outputs per input, one pass per case plus the sentinel:

| cores | records/s | % of cap | src idle | broker limit hits | file cache |
|---:|---:|---:|---:|---:|---:|
| 1 | 245,934 | 99.4% | 0.6% | 0 | 2.19 GB |
| 2 | 525,655 | 100.4% | 2.1% | 0 | 2.30 GB |
| 4 | 964,912 | 97.7% | 5.9% | 0 | 2.36 GB |

964,912 trades/s in and 4.82M position records/s out is the highest rate any
run has reached on this rig.

## What the broker guard did

Nothing — which is the point. The agent read the skill, sized the broker at
5 GiB for a 400M-record backlog, and the guard recorded zero limit hits in
every case. [Run 14](clean-room-run-14.md) shipped 2 GiB and published a
1.35× off a starved 4-core case; with the broker fed, the same step here
reads 1.836×, and the controlled re-measurement of run 14's own build read
1.725× ([diagnosis](rig-2026-09-05-broker.md)).

## Measured, not explained

- **Two cases read 94.7% and 95.0% of cap, then 97.7–100.4% on a retry with
  nothing changed.** The agent ruled out the broker (0.12–0.60 of its cap,
  zero memory-limit hits), a starved source (idle 0.5–4.5%) and the cap
  itself (100% throttled periods), and offered no cause. Neither do we. One
  pass per case cannot separate an order effect from noise, which is the same
  reason the table is unpublishable.
- The agent's first two chain attempts were both sizing errors of its own —
  it guessed 450k rec/s for a job that runs at 970k. The tiny proof caught
  both before the suite, which is what it is for, but "first attempt clean"
  now depends on the agent's sizing guess more than on the harness.
- One stated deviation: the Docker VM has 7.65 GiB and could not be raised,
  so the broker ran at a 5 GiB limit with a 1 G heap rather than the
  README's 4 GiB/3 G. The other arm was not run.

