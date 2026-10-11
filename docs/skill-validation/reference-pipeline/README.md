# The reference pipeline

A pipeline we already trust, kept so that **a harness change can be measured
without an agent building a new one first**.

## Why

Testing a change to the harness used to mean a clean-room run: an agent reads
the skill, builds a whole Flink project from scratch, and then debugs what it
built. That is the right test for one question and the wrong test for another.

| run | tokens | wall clock | chain itself |
|---|---:|---:|---:|
| 44 | 363,000 | 3h 10m | ~62 min |
| 45 | 559,000 | 2h 00m | never reached a suite |
| 46 | 294,000 | 1h 50m | 59 min |

Most of that is the agent writing and debugging its own pipeline. Run 46's
single largest cost was 25 minutes on a watermark problem in its own job; run
45's was 25 minutes on a direct-memory setting in its own job. Neither taught
us anything about the harness, and nearly every fix made on 2026-09-23 was a
harness fix.

So separate the two questions:

| question | what answers it | cost |
|---|---|---|
| **does the harness work?** | this fixture | about an hour, no building, no debugging |
| **can an agent build a correct pipeline from the skill's prose?** | a clean-room run | two to three hours, and worth it only when what the skill *teaches* has changed |

## What it is

Clean-room run 44's build, unchanged: temperature readings in, one average per
location per hour out, bucketed on the timestamp inside the reading. A
`KeyedProcessFunction` per location, an at-least-once sink made idempotent by
publishing the absolute total and count, exactly-once checkpointing at 10 s.

It was chosen because it is the only recorded run whose **ten cases all
measured** — no ceilings, nothing thrown out — and whose completeness passed
both arms, clean and killed, with no tolerances.

```
job/           the Flink job, generator, verifier and progress command (1,170 lines of Java)
dashboard/     Prometheus, Grafana and the container CPU exporter; the dashboard is
               built by dashboard/build_dashboard.py in the skill's four rows
pipeline.json  run 44's own, with {RUNDIR} where the absolute paths were
ANSWERS.md     the six interview answers it was built from
ASSUMPTIONS.md run 44's own assumptions and PLAN.md its plan, word for word; preflight
PLAN.md        stops the chain without them
run-reference.sh
```

## Use

```
source scripts/env.sh          # Java 17; the build needs it
sh docs/skill-validation/reference-pipeline/run-reference.sh
```

Every container, volume and network is named with `PREFIX` (default `refpipe`),
which the script substitutes into `pipeline.json` and both Prometheus files as it
copies them. Run 44 hardcoded its own `st44-` prefix there, and the first re-run
started a Prometheus scraping two hosts that did not exist.

It copies itself to a scratch directory, builds the jar, and runs
`prove.py all` against whatever harness is installed at
`~/.claude/skills/scalable-flink-skill`. Nothing here is modified.

## What it produced, and what a re-run is compared against

Run 44, build `0c8c347d45877a11`, 900,000,000 readings, 8 partitions, 3 passes
per case plus the sentinel:

| cores | rate | spread | worker CPU |
|---:|---:|---:|---:|
| 1 | 794,036/s | 7.4% | 98.8% of cap |
| 2 | 1,409,446/s | 8.7% | 99.6% of cap |
| 4 | 2,567,941/s | 10.4% | 99.8% of cap |

| step | ratio | reportable | met the target in force then (1.90×) |
|---|---:|---|---|
| 1→2 | 1.775× | yes | no |
| 2→4 | 1.822× | yes | no |

Sentinel drift +2.2% across 42 minutes. Ten of ten cases `OK`.

**Read a re-run against those two step ratios and the ten-of-ten.** The rates
themselves move with the machine and the hour — run 44 measured on a host that
reached load 15.4 — so a rate 10% away is not news. These are:

- **a case count below ten is news.** A harness change that starts throwing out
  cases this pipeline used to pass is a regression, and it is exactly what run
  46 hit.
- **a step that stops being reportable is news**, for the same reason.
- **the target has changed since run 44.** It was 1.90× on the mean. Since
  scalable-flink-skill #145 a step is met when the low end of its range is above
  1.782× (1.80× less 1%). Run 44's per-pass rates are not in this repository, so
  its verdict under today's rule cannot be recomputed, and a re-run that says
  "met" for 2→4 is not news by itself. Compare ratios and case counts, not
  verdicts judged by different rules.
- **a re-run on a busy or memory-short Mac is not a test of the harness.** The
  2026-10-11 re-run ([`run-2026-10-11/`](run-2026-10-11/), findings §9) stopped
  with the 2-core readings 23% apart while the Mac had 7–9 GB swapped out and
  other programs used up to 4.46 of 8 cores at window open. Check both in the
  case records (`otherCoresOpen`, `hostSwapOpenMB`) before reading a stop as a
  regression.

## Re-runs

| run | harness | result | 1→2 | 2→4 | cases measured | kept results |
|---|---|---|---:|---:|---|---|
| 2026-10-11, first | scalable-flink-skill `ada7890` | stopped at the report: the 2-core readings were 23% apart | — | — | 8 of 10 | [`run-2026-10-11/`](run-2026-10-11/) |
| 2026-10-11, third | scalable-flink-skill `ada7890`, macOS 27.0.1 | passed, 82.9 min | 1.940× (1.845–2.035×) | 1.837× (1.756–1.948×) | 10 of 10, plus 4 settling passes | [`run-2026-10-11c/`](run-2026-10-11c/) |

The second run that day (`refpipe-20261011b`) passed its short proof and then
stopped at the suite: Photos analysis and Spotlight kept 3–5 of 8 cores busy
after the macOS upgrade, and the busy-machine guard waited its full 10 minutes
before the first suite case. Its results were not kept.

The third run's rates were 823,178 / 1,596,840 / 2,933,062 records per second
at 1 / 2 / 4 cores, each case's passes 4.7–5.2% apart, sentinel drift +2.9%.
The busy-machine guard held the 2-core and 4-core cases while Photos analysis
and file sync were busy, and each wait cleared before its limit. Both steps
meet today's rule; 2→4's lowest pair is 0.5% under 1.80×, inside the 1% margin.

## Do not let a clean-room agent read this directory

It is a worked answer to the interview a clean-room run exists to answer for
itself — the assumptions, the record shape, the arrival-lag trick, the
`max-parallelism` value, all of it. Clean-room briefs already forbid reading
anything under `flink-training`; this is one of the reasons.
