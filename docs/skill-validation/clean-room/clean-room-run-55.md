# Clean-room run 55 — the defaults, on the skill after #158

Every interview default (DataStream, Apache Kafka), run 54's prompt with three
wording changes: the macOS-upgrade line taken out, "decision" for "claim", and
feedback asked for on waiting for the chain. The first clean-room run on
scalable-flink-skill `e0e13f7`, whose one change is #158: an agent waits for
`results/DONE` inside its turn, re-running `watch.sh` with a time limit. 2.57 h
from launch to the agent's report (19:26 to 21:59), 106 tool calls, 301k
tokens. Files: [run-55/](run-55/), including the job,
[PositionsJob.java](run-55/job/src/main/java/st55/PositionsJob.java).

**#158 did what it was for.** The agent re-ran the watcher 12 times
(`watch.log`, not kept), reported after `results/DONE`, opened its report with
the scorecard, and ran the probe and the ceiling. Run 54's agent had reported
71 minutes before its chain finished.

## What it produced

[The suite](run-55/results/suite.txt), build `55e693b915fbe2a6`, 300,000,000
orders:

| cores | records/s | passes marked OK | how far apart | counted |
|---:|---:|---:|---:|---|
| 1 | 128,653 | 7 | 10.5% | yes |
| 2 | 235,385 | 6 | 6.7% | yes |
| 4 | 438,463 | 2 | 7.7% | yes |

1 → 2 **1.830×, not met**: it could be as low as 1.763×, 2.1% under the 1.80×
target, after the six extra settling passes. 2 → 4 **1.863×, met**: at its
lowest, 1.784×, it is 0.9% under, inside the 1% margin. The chain STOPPED at
report after 100.0 min. Completeness passed on both arms, clean and with the
worker killed. One 4-core pass (p2-desc) was thrown out as a ceiling: it used
94.5% of its cores. The 1-core rate read 8.1% lower at the end of the suite
than at the start.

The two measurements the skill asks for when a step is not met, both kept:

| measurement | what it found |
|---|---|
| [probe](run-55/results/hostprobe.json), 9 repeats | the Mac's own cores on memory-heavy work give 1→2 at 90.1% of linear (about 1.80×) and 2→4 at 75.5% (about 1.51×); simple arithmetic 97.0% and 98.6% |
| [ceiling](run-55/results/ceiling.json), 4 cores | 447,262 / 437,915 / 394,602 orders/s with Kafka capped at 2.5 / 1.0 / 0.5 cores; Kafka used about 0.49 cores and was the limit only at half a core |

**Not explained:** why 1→2 reads short. The probe's spread is wider than the
2.1% shortfall, so the Mac is neither ruled in nor out. Cause not measured.

## What went wrong on the way

- **The tiny proof passed 1→2 at 1.778×**, under the target, without a word,
  and the suite then spent about 25 minutes of settling passes on the same
  step. scalable-flink-skill #159 adds a warning.
- **The tiny proof kept its 4-core case as a ceiling** (93.3% of its cores)
  and passed; the suite then ran 4 cores at 99.4% and 96.9%.
- **The Mac went to sleep from 20:01 to 20:06**, on battery, during
  completeness (the agent's report; the gap sits inside completeness's 1,099
  s). It fell between the drain and its verifier, so no reading moved. Nothing
  in the harness notices sleep.
- **Before the chain, the agent's own hand-run completeness stopped** on the
  killed arm: its verifier counted a repeated last record after the restart as
  a gap. Fixed in the verifier and proved on the recorded outputs before the
  chain.

## What the agent reported

Ten findings in [SKILL-FEEDBACK.md](run-55/SKILL-FEEDBACK.md). F1–F4, F9 and
F10 are fixed in scalable-flink-skill #159: how `keySets` names label the job
graph, the `keySets` error's field list, where to start `all` from, a second
input written once, the tiny-proof warning, and `WATCH_MINUTES=8`. F5 (sleep),
F6 (the tiny proof's advice after a ceiling), F7 (the start sweep and `tail
-F`) and F8 (the sink-name rule only in SKILL.md §4) are not fixed.

Reading the run's output found one more: the verdict sentence "the low end of
its range, 1.763x, is 2.1% under the 1.80x target" printed beside 1.83×, with
nothing joining the two. The author: "this does not make sense". #159 reads
"measured 1.830x, but it could be as low as 1.763x, 2.1% under the 1.80x
target; a step may fall at most 1% under".
