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
pipeline.json  run 44's own, with {RUNDIR} where the absolute paths were; its dashboard
               is built by the harness, in the skill's seven rows, and
               `STORE=redis sh run-reference.sh` adds the Redis store
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
which the script substitutes into `pipeline.json` as it copies it. Run 44 hardcoded its own `st44-` prefix there, and the first re-run
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
| 2026-10-12 | scalable-flink-skill `df12dcf` (#154, #155), macOS 27.0.1 | passed, 83.1 min | 1.980× (1.819–2.115×) | 1.819× (1.711–1.940×) | 10 of 10, plus 5 settling passes | [`run-2026-10-12/`](run-2026-10-12/) |
| 2026-10-13 | scalable-flink-skill `28ce501` (#156, #157), macOS 27.0.1 | passed, 66.4 min | 1.868× (1.813–1.926×) | 1.899× (1.877–1.899×) | 10 of 10, plus 1 settling pass | [`run-2026-10-13/`](run-2026-10-13/) |
| 2026-10-17, dashboard built by the harness | scalable-flink-skill `461649a` (branch of #161), macOS 27.0.1 | stopped at the report: the 2-core readings were 29.4% apart | — | — | 10 of 10, plus 1 settling pass; the 2-core case not counted | [`run-2026-10-17/`](run-2026-10-17/) |
| 2026-10-17, the same with the Redis store | scalable-flink-skill `6162f55` (#161, merged as `4ffa99a`), macOS 27.0.1 | passed, 66.6 min | 1.842× (1.830–1.879×) | 1.907× (1.807–1.941×) | 10 of 10 | [`run-2026-10-17-store/`](run-2026-10-17-store/) |
| 2026-10-18, from an empty broker | scalable-flink-skill `9e6846f` (#162, #163, #164), macOS 27.0.1 | passed, 67.5 min | 1.870× (1.795–1.906×) | 1.864× (1.813–1.936×) | 10 of 10, one 4-core pass a ceiling, plus 1 settling pass | [`run-2026-10-18/`](run-2026-10-18/) |
| 2026-10-18, largest case first | scalable-flink-skill `cf7627c` (#166, #167, #168), macOS 27.0.1 | stopped at the report: 1→2 could be as low as 1.759×, 2.3% under the target | 1.786× (1.759–1.849×) | 1.882× (1.857–2.108×) | 10 of 10, plus 6 settling passes | [`run-2026-10-18-order/`](run-2026-10-18-order/) |

The second run that day (`refpipe-20261011b`) passed its short proof and then
stopped at the suite: Photos analysis and Spotlight kept 3–5 of 8 cores busy
after the macOS upgrade, and the busy-machine guard waited its full 10 minutes
before the first suite case. Its results were not kept.

The third run's rates were 823,178 / 1,596,840 / 2,933,062 records per second
at 1 / 2 / 4 cores, each case's passes 4.7–5.2% apart, sentinel drift +2.9%.
The busy-machine guard held the 2-core and 4-core cases while Photos analysis
and file sync were busy, and each wait cleared before its limit. Both steps
meet today's rule; 2→4's lowest pair is 0.5% under 1.80×, inside the 1% margin.

The 2026-10-13 run's rates were 838,207 / 1,565,813 / 2,973,908 records per
second at 1 / 2 / 4 cores, each case's passes 7.0–7.9% apart, sentinel drift
−5.9%. Other programs used at most 1.66 of the 8 cores at any window open,
with 727 MB swapped out. Its output was the first with #156's wording: no
"claim", the step table's "range it is judged on" column, and the preflight
row "other programs' use of the cores (reported)".

The two 2026-10-17 runs were the first with the dashboard the harness builds
(scalable-flink-skill #161): seven rows, consumer lag on every topic, and the
broker's network as a share of its link. In both, every panel showed data over
the suite and all 31 metric names in the skill's section 7 returned data.

The first, with the store off, stopped at the report. Its 2-core settling pass
read 1,128,874 records per second against 1,490,756–1,548,117 for the other
three, and the 1-core sentinel read 16.4% under the first 1-core pass. The Mac
had 4.1–6.0 GB of memory moved out to disk at window open throughout, against
1.5–2.0 GB in the store-on run. What slowed those two passes was not measured.

The store-on run passed. Its rates were 822,298 / 1,514,724 / 2,888,082 records
per second at 1 / 2 / 4 cores, each case's passes 6.1%, 5.2% and 0.9% apart,
sentinel drift −1.3%. Other programs used at most 2.36 of the 8 cores at any
window open. One 1-core pass was measured again because its two ways of
counting were 5.2% apart, over the 5% limit; the repeat agreed. Redis held 100
keys, the output's 100 distinct keys. An earlier store-on attempt that day
stopped at completeness with the four store panels empty: the store was
switched on for a stack already up, and the running Prometheus kept its old
configuration. #161 fixed that before merging; its results were not kept.

The 2026-10-18 run tested three skill changes: #162 (a case whose window
spans a sleep is thrown out, and preflight reports mains or battery), #163
(wording only) and #164. Every case recorded 0.0 s asleep inside its window,
11 in the suite and 3 in the tiny proof, and every step of the chain
recorded its sleep. Preflight said "on mains power, battery 95%".

It is also the first run here from an empty broker since the disk row
guessed at record size. A first attempt that morning stopped at preflight:
the row guessed 120 bytes a record and projected 121 GB against 114 GB free,
for a run that needs about 29 GB. #164 makes a guess that does not fit a
report, leaving the decision to the tiny proof, which measures the size
before the fill. The attempt's results were not kept.

Rates were 832,922 / 1,557,917 / 2,904,055 records per second at 1 / 2 / 4
cores, each case's passes 5.8%, 6.1% and 4.4% apart, sentinel drift −1.4%.
The descending 4-core pass was a ceiling: 94.7% of its cores, with Kafka at
its memory limit 1,835 times in the window. It is kept in the table and left
out of the ratios. The 1→2 range reached under the target, so one settling
pass ran; the step was then met inside the 1% margin. Other programs used at
most 1.23 of the 8 cores at any window open, with 1.8–3.4 GB of memory moved
out to disk.

The second 2026-10-18 run tested three more skill changes: #166 (a self-test
that works at any backlog size), #167 (the largest case first) and #168 (a
case waits while 4 GB or more of memory is moved out to disk, and the tiny
proof stops at the first problem with the machine). The order held: the tiny
proof measured 4, 2, then 1 cores; the suite ran 4, 2, 1, then 1, 2, 4, then
4, 2, 1, and the sentinel was a 4-core pass. The memory wait held no case:
1.0–3.4 GB was moved out to disk before each one. Every case recorded 0.0 s
asleep.

It stopped at the report after 84.9 minutes. 1→2 measured 1.786×, and its
lowest reading, 1.759×, is 2.3% under the 1.80× target, more than the 1%
allowed; six settling passes at 1 and 2 cores did not bring it inside. 2→4
was met at 1.882×. Rates were 844,045 / 1,507,136 / 2,836,615 records per
second at 1 / 2 / 4 cores, each case's passes 3.8%, 10.2% and 18.6% apart.
The 4-core sentinel read 19.4% under the first 4-core pass, with the
machine's load at 21.1 on 8 cores while it was measured; other programs used
0.30 of the 8 cores at its window open and 1.18 at its close. Other programs
used at most 1.6 of the 8 cores at any window open. What slowed the sentinel
and the slowest 2-core pass was not measured, and one run cannot say whether
the new order changed the result.

## Do not let a clean-room agent read this directory

It is a worked answer to the interview a clean-room run exists to answer for
itself — the assumptions, the record shape, the arrival-lag trick, the
`max-parallelism` value, all of it. Clean-room briefs already forbid reading
anything under `flink-training`; this is one of the reasons.
