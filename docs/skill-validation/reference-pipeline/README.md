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
dashboard/     Prometheus, Grafana and the container CPU exporter
pipeline.json  run 44's own, with {RUNDIR} where the absolute paths were
ANSWERS.md     the six interview answers it was built from
run-reference.sh
```

## Use

```
source scripts/env.sh          # Java 17; the build needs it
sh docs/skill-validation/reference-pipeline/run-reference.sh
```

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

| step | ratio | reportable | meets the 1.90× target |
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
- **a verdict other than "the pipeline did not meet the target" is news.** This
  pipeline does not meet 1.90× on this machine. If a re-run says it does, or
  says `PASS`, something is wrong with the harness rather than right with the
  pipeline.

## Do not let a clean-room agent read this directory

It is a worked answer to the interview a clean-room run exists to answer for
itself — the assumptions, the record shape, the arrival-lag trick, the
`max-parallelism` value, all of it. Clean-room briefs already forbid reading
anything under `flink-training`; this is one of the reasons.
