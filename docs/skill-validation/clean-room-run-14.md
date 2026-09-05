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

*(written after the run)*
