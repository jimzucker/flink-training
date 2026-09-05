# Clean-room validation, run 14 — one clean run, 1/2/4, one pass each

**The objective is a first-attempt clean run and the time it takes.** Every
stop-and-restart before this one was thrown away, so what is being measured
here is the skill's best time when nothing has to be re-run:
`prove.py all --quick` — cases 1, 2 and 4, one pass each, the sentinel
repeating the baseline at the end — on the harness at #50.

Same conditions as [runs 1–13](README.md): fresh agent, empty directory
(`flink-skill-test-15`; the 14 directory was consumed by two abandoned
attempts), barred from this repository and every other test directory, one
prompt, no human input.

Two earlier attempts at run 14 were abandoned and nothing from either is
kept: 2026-09-04 19:57/20:05 (stopped for a laptop restart) and 2026-09-05
05:39 (the agent hit the `--quick` defect fixed in #50, worked around it with
its own launcher, hit the second half of the same defect, and fell back to a
three-pass run — killed at 06:48 during its suite).

Before this launch the fixed chain was proved end to end on the author's rig,
so that a defect would cost 40 minutes of rig time and not a two-hour agent
run.

## The criteria, written before launch

| criterion | what the record says |
|---|---|
| **the chain passes on the first attempt** — no rerun of a phase, no fallback, no wrapper around `prove.py` | run 13 needed 3 tiny-proof attempts of its own before the chain; the 2026-09-05 attempt needed a wrapper and then failed |
| uses the harness verbatim (`prove.py all --quick`, nothing imported or copied); one suite; 0 forbidden-path reads | runs 11–13 |
| chain `preflight` → `report` ≤ 45 min | derived: the rig's gates cost ~25 min and a 4-case quick suite ~16 min at 242 s per case |
| whole-run wall clock (reported, not judged) | 2 h 05 m (13), 1 h 58 m (11); 1 h 07 m of run 13's was the agent on its own pipeline |
| the stack holds: no Flink restart, no rig-scoped refusal, no rebuild once the chain starts | run 13 had none |
| every case at 95–101% of its CPU cap | 95.1–100.4% (13); the rig refused one at 92.8% |
| sentinel measured, drift reported | −2.5% (13), −7.0% (rig, 3 passes) |
| the table is stamped unpublishable and the report says so | `--quick` sets `quickLook` / `publishable: false` |
| ratios | **recorded, not judged** — one pass cannot carry a spread |

## Response

**PASS on every criterion, and the fastest run on record.** Model: Claude
Opus 5. Harness `lib.py fb4feb70845e8ace`, `prove.py 0b9b3cbc0cd0b06b`
(main at #52), run from the skill directory, nothing copied or imported.
54 tool calls, 0 forbidden-path reads. 08:28 → 09:30 local. Raw results in
[run-14/](run-14/).

| criterion | result |
|---|---|
| the chain passes on the first attempt | **PASS** — one `phase=all start`, every phase rc=0 |
| harness verbatim, one suite, 0 forbidden reads | **PASS** — `prove.py all --quick` directly, 0 of 54 tool inputs touched a forbidden path |
| chain ≤ 45 min | **PASS — 37.1 min** (up 2 s, preflight 74, completeness 422, tinyproof 498, fill 282, suite 949) |
| whole-run wall clock (reported) | **55.9 min** — the fastest clean-room run on record; 8.3 min of it was the agent building its job |
| the stack holds | **PASS** — no Flink restart, no rig refusal, no rebuild once the chain started; the suite refused nothing |
| every case at 95–101% of cap | **PASS** — 99.9 / 98.4 / 95.9%, sentinel 96.3% |
| sentinel measured | **PASS** — +2.3% |
| table stamped unpublishable | **PASS** — `quickLook: true`, `publishable: false`, banner in `suite.md` and `suite.txt`, and the agent's report repeats it |
| ratios (recorded, not judged) | 1→2 = **2.11×**, 2→4 = **1.35×** |

Build `01cf956ac64d455e`, 264M-record backlog, 8 partitions, 10 s
checkpoints, four outputs per input, one pass per case plus the sentinel:

| cores | records/s | % of cap | throttled | src idle | vantage |
|---:|---:|---:|---:|---:|---:|
| 1 | 186,468 | 99.9% | 100% | 0.1% | 0.21% |
| 2 | 398,686 | 98.4% | 100% | 0.2% | 0.18% |
| 4 | 539,902 | 95.9% | **87%** | 2.6% | 0.04% |
| 1 (sentinel) | 190,876 | 96.3% | 100% | 0.0% | 0.16% |

Before the chain the agent's own calibration tiny proof was refused once —
its 24M backlog drained before the window closed — and it re-sized from the
measured 1-core rate rather than guessing again. That is the tiny proof doing
its job; the published chain then ran once, clean.

## Measured, not explained

- **2→4 read 1.35× (68% of linear)**, against 1.85–2.15× in every earlier
  run. One pass, so there is no spread and nothing here is publishable. What
  is visible: at 4 cores the worker was throttled in **87%** of cgroup
  periods against 100% at 1 and 2 cores, so the CPU cap stopped being the
  only binding constraint — and what replaced it was not measured. The agent
  ruled out the broker (8% of its cap), a starved source (2.6% idle), the
  measurement (0.04% vantage, 7 boundaries, 326 s headroom), the graph shape
  and rig drift, and named no cause. Neither do we.
- The 1-core case's own two measurements differ by 2.3% with cap use of
  99.9% and 96.3% — the sentinel is inside the noise the record already
  carries, but it is the same direction as the 4-core throttling gap.

## What this run cost to get

Four harness defects were found and fixed between 05:39 and 08:28, each
proved at the cheapest level that reproduced it before any rerun: the
`--quick` flag reaching `replay` (the record would have been re-derived with
`minPasses` bypassed), the same leak switching off a live guard, the guard's
own `finally` clearing the flag mid-chain (a suite voided itself as "1 pass
< 2" while running one pass), and — not a `--quick` defect at all — the
window closing on the first piece of a split commit, which had refused three
runs on record at 20.2%, 33.9% and 33.0% vantage. See #50, #51, #52.

