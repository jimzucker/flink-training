# Clean-room validation, run 19 — the two-pass quick mode

Run 18's prompt again on the harness at #61, where `--quick` now measures each
case **twice** instead of once. Run 18 reported 2→4 = 1.539× from a single
pass; three passes of its own build read 1.678×, so the one-pass ratio was
8.3% low. This run tests whether two passes make the reported ratio stable.

## The criteria, written before launch

| criterion | the record |
|---|---|
| chain passes on the first attempt | 1 of the last 4 |
| harness verbatim, one suite, 0 forbidden-path reads | every run |
| chain ≤ **60 min** (raised from 45: two passes add ~12 min) | 36.1 min at one pass (18) |
| whole-run wall clock (reported) | 1 h 25 m (18), 1 h 37 m (16) |
| **every case carries a spread** | new — one pass could not |
| every case at 95–101% of cap | 96.0–99.8% (18) |
| no case refused for broker memory | 0 hits (16, 18); the guard caught the rig's 3 GiB config today |
| sentinel drift reported | +7.5% (18), −1.5% (17) |
| ratios (recorded, not judged) | 2→4 has read 1.54–1.95× at one pass; 1.678× at three passes on run 18's build |

**The consistency test**: this run's 2→4 and its pass-to-pass spread, against a
second run of the same prompt. Two runs whose ratios agree inside their spreads
is the bar; anything else is still inconsistent.

## Response

Opus 5, harness at #61, `prove.py all --quick`, 107 tool calls, 0
forbidden-path reads, 21:45 → 00:05. Raw results in [run-19/](run-19/).

| criterion | result |
|---|---|
| chain passes on the first attempt | **FAIL** — the first attempt died at the tiny proof (4 cores at 91.6% of cap after half an hour under load); it passed after ten minutes idle, at 98.5% |
| harness verbatim, 0 forbidden reads | PASS |
| chain ≤ 60 min | PASS — 51.1 min |
| whole-run wall clock | 2 h 20 m |
| **every case carries a spread** | **partly** — 1c three passes, 2c two, **4c one**: its other pass was refused at 94.1% of cap |
| every case at 95–101% of cap | one refusal at 94.1% |
| no broker-memory refusal | PASS |
| sentinel drift | **+11.2%** |
| ratios | 1→2 = 2.191×, 2→4 = **1.783×** |

| cores | records/s | passes | spread | % of cap |
|---:|---:|---:|---:|---:|
| 1 | 183,791 | 3 | 11.2% | 100.0% |
| 2 | 402,711 | 2 | 1.6% | 98.1% |
| 4 | 718,137 | 1 | — | 96.4% |

## What this run contributed

Two passes were not enough while the 4-core case could still be refused, so
its 2→4 still rested on a single 4-core measurement. The agent also measured
something we had missed: **re-running its unchanged baseline read 59.5% of cap
where it had read 88.0%**, and its tiny proof went 91.6% → 98.5% after ten
minutes idle. It tested three mechanisms — bigger producer batches, smaller
batches, more worker memory — and **withdrew all three**, because the rig was
moving faster than the effects.

That last observation is what sent the next day's work at the memory contract:
the "more worker memory" arm was pointing at the right thing and could not be
resolved against the drift. The controlled version, interleaved so drift
cancels, is in [memory-per-subtask](memory-per-subtask.md), and it found the
4-core case was starved of heap — flat 2048m read 2→4 = 1.645× against 1.910×
with the memory it needed.

The agent's own honest note stands: the superlinear 1→2 = 2.19× is inside the
baseline's 11.2% sentinel drift, and against the baseline's last measurement
the step is 2.09×.

