# Clean-room validation, run 10 — the harness refused, and the harness was the problem

The second run against the short skill, with the spread guard re-scoped after
[run 9](clean-room-run-9.md) — a data refusal now voids the case, not the
suite. Same conditions as [runs 1–9](README.md): fresh agent, empty directory,
barred from this repository, every earlier test directory and the preserved
results, only the skill, one prompt, no human input. DataStream pinned; the
prompt asked for the 2→4 step ratio with its spread as the headline.

**Model: Claude Opus 5.**

## What it cost

| | |
|---|---:|
| Human time | **0 h** |
| Human prompts | **1** |
| Wall clock | 2.32 h (139 min) |
| Suites run | 4 |
| Final report | written; **no 2→4 ratio reported** |

## The answer it gave: none

Four suites, one build (`57be5902a9228120`), 160M-trade backlog, 10 s
checkpoints, worker at 99.3–100.2% of cap and source idle ≤0.1% on every row —
and no reportable step, because its spread guard was written at 10%:

| suite | change | 2c trades/s | spread | 4c trades/s | spread | 2→4 | reported |
|---|---|---:|---:|---:|---:|---:|---|
| A | sink retention 400 MB | 108k | 86% | — | — | — | no |
| B | retention 2 GiB | 190,439 | 11.2% | 394,315 | 4.6% | **2.07×** | **no — 11.2% > 10%** |
| C | warm-up scatter gate 10% | 1 pass each | — | — | — | — | no |
| D | scatter 20%, final | 202,742 | 17.8% | 377,826 | 26.8% | 1.86× | no |

Suite B is a valid table. Two- and four-core cases at cap, six passes
alternating asc/desc, vantage points within 0.6%, and a step of 2.07× — the
same answer run 9 gave (2.10×) and the demo pipeline gave (2.11×). The harness
voided it for 1.2 points of spread over a round number, then spent the next
hour on suites C and D looking for a rig that would come in under it. Suite D
found something real instead: its third passes fell 13% at two cores and 25%
at four while broker CPU per record rose. **The cause is unmeasured.** The
agent said so.

What it asserted from a controlled comparison, correctly: sink
`retention.bytes` 400 MB → 2 GiB moved the 2-core mean 108k → 190k and its
spread 86% → 11%, because the segment-delete sweep was landing inside the
window. That is suite A against suite B, one variable.

## What the record says about 10%

Replaying a 10% spread ceiling against every suite on disk from runs 9 and 10
refuses eight step ratios the record marks valid; 30% reports suite D, which
it marks invalid. Valid 2- and 4-core cases span 0.1–15.8%, the outliers sit at
26.8% and 86%. A 20% ceiling agrees with all eighteen recorded verdicts. The
threshold was a round number written before anyone looked; the record that
would have corrected it already existed when it was written.

## The finding that outranks the table

Run 9 wrote 1,115 lines of harness. Run 10 wrote 1,652 different lines to the
same prose, and re-decided things the prose had already decided: what a
refusal does (run 9 stopped the suite; run 10 marked the case), what counts as
flat (run 10 added a scatter gate, then found its own 10% unsatisfiable and
relaxed it mid-run), what a record is (legs in run 9, trades in run 10). Each
agent did the skill's job well and then rewrote its instruments from memory —
which is why the two runs cannot be compared and why neither could be trusted
to be stable on the next.

So the harness now ships with the skill —
[`harness/`](../../.claude/skills/prove-it-scales/harness/README.md) — as
code: the sampler, the preflight, the tiny proof, the completeness run, the
suite, every guard with a self-test, and the thresholds with the measurement
each was set from. `prove.py replay` re-derives the recorded suites against
the current thresholds before any command touches a stack. An agent supplies
the pipeline and a `pipeline.json`; it does not write a harness.

Its first live test, against this run's own pipeline and build, found **six
defects in the harness's own sizing** before the suite ran — a window shorter
than the reporter interval, a tiny backlog that drained during warm-up, a 2 s
checkpoint override that read 83–94% of cap where the pipeline's 10 s read
100% (A/B, one rig, one build, one variable), a warm-up ceiling one interval
too short, a self-test aimed at a topic that did not exist, and a
completeness backlog one checkpoint interval long so the kill could not land
where it was asked. Every one of those would have been a refusal a fresh
agent went off to explain. Then, with nothing else changed, the suite
([`harness-live-1/`](harness-live-1/suite.md)):

| cores | p1-asc | p2-desc | p3-asc | mean | spread | % of cap |
|---:|---:|---:|---:|---:|---:|---:|
| 2 | 213,073 | 221,023 | 217,746 | 217,281 | 3.7% | 100.1% |
| 4 | 438,039 | 437,344 | 440,984 | 438,789 | 0.8% | 98.4% |

**2→4 = 2.019×**, range 1.98–2.07× across passes, source idle ≤0.1%, vantage
points within 0.4%, order effect 1.03 / 1.00 — the same pipeline this run
could not report on. Preflight 13/13, tiny proof 1.86× with 21/21 guards
fired live, completeness clean and with the worker killed at 37% of a 20M
drain, 23 minutes for the suite and about 50 for the whole command chain.
One number is noted and not explained: the last 4-core pass read 95.7% of
its cap where the other two read 99.6–99.7%.

## What it sent back into the skill

- **replay before run**: a new guard or threshold is checked against every
  recorded result before it costs a run; thresholds come from measured spread
- **the harness is an artifact of the skill, not of the run**
- spread ceiling 20%, with its provenance in the code
- warm-up: slope *and* scatter, four commit intervals, ≥90 s — the ramp ran
  46–136 s here and slope alone admitted it
- the REST back-pressure path is deprecated on Flink 1.20; busy/idle come
  from the worker's slf4j reporter, and a window must hold ≥3 of its samples
