# Clean-room run 53 — the defaults, on the skill after #148–#153

Every interview default (DataStream, Apache Kafka), run 52's prompt with three
changes: the agent also writes the pipeline's own README; its feedback must
cover that README, keeping the evidence in git, and the job diagram; and it is
told the Mac may be busy after the day's macOS upgrade. The first clean-room
run on scalable-flink-skill `ada7890`: the busy-machine wait (#148), the
largest-first short proof (#149), the README check (#150), evidence kept in git
(#151, #152) and the every-operator diagram (#153). 2.63 h, 93 tool calls, 318k
tokens. Files: [run-53/](run-53/), including the job,
[PositionsJob.java](run-53/job/src/st53/PositionsJob.java).

## What it produced

[The suite](run-53/results/suite.txt), build `b0d1448e7640f0c3`:

| cores | records/s | passes marked OK | how far apart |
|---:|---:|---:|---:|
| 1 | 130,810 | 3 | 5.7% |
| 2 | 245,664 | 6 | 14.4% |
| 4 | 441,859 | 6 | 18.6% |

1 → 2 **1.88×, met** within the 1% margin (the low end of its range, 1.786×,
is 0.8% under 1.80×). 2 → 4 **1.80×, not met**: the low end of its range,
1.725×, is 4.2% under the target, after five of the six extra settling cases
went to it. Verdict: `STOPPED at report: not met … More passes could change
that; change nothing in the pipeline`. One 1-core pass was set aside as a
ceiling (94.7% of its core against a 95% floor); every other pass ran at
95–101% of its CPU cap. Completeness passed on both arms, clean and with the
worker killed. Preflight, the README check, `records.md`, `headline.md`, the
[diagram](run-53/results/graph.png) (4 tasks, 14 operators) and the evidence in
git all worked.

After the stop the agent ran the two measurements section 6 asks for:

- `prove.py probe --repeats 9`: this Mac's own cores give 1.98× on simple
  arithmetic for 2→4 but 1.65× on memory-heavy work (middle half 7%), so the
  machine cannot be ruled out as the limit. [hostprobe.json](run-53/results/hostprobe.json)
- `prove.py ceiling`: cutting Kafka from 2.5 cores to 0.5 moved the 4-core rate
  3% (504,732 to 487,985/s). Kafka is not the limit.

**Not explained:** the 4-core passes rose late in the suite on an unchanged
build — 422,338, 428,256, 410,837 and 429,480/s, then 467,289 and 492,953/s.
The 2-core passes show no such trend. Other programs were under 2 cores at
every window. Cause not measured.

## What went wrong on the way

- **The README check's own self-test missed** (1 of 321): the operator
  `orders-source` counted as naming the topic `orders`, because the check was a
  text search. A rename and a rebuild, about 5 minutes. Fixed in #154.
- **A self-test "STOPPED:" line in `harness.log`** ended the agent's first
  wait loop early. Fixed in #154.
- **The agent's own wait loop** named `prove.py` and so matched itself; about
  15 minutes of wall clock, no measurement lost.
- Flink's metrics reporter writes `position-mv-by-symbol` as
  `position_mv_by_symbol`; the agent caught it by querying Prometheus before
  the chain. Fixed in #155.

## What the agent reported

13 findings in [SKILL-FEEDBACK.md](run-53/SKILL-FEEDBACK.md). Twelve led to
changes: #154 (README names matched whole, self-test messages in their own
file, one rule in section 6 for a step not met, `headline.md` in the
scorecard's words) and #155 (the reporter's spelling, quoted compose values,
`{arm}` in the examples, completeness never stopped by a busy Mac, the probe's
file kept, and three wording fixes). The thirteenth, the wait loop, was the
agent's own; section 2 already says to wait on `results/DONE`. Both were tested
on the reference pipeline on 2026-10-12, which passed
([reference-pipeline/run-2026-10-12/](../reference-pipeline/run-2026-10-12/)).
