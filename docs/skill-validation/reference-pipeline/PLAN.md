# The plan — clean-room run 44

> Clean-room run 44's own file, kept word for word. Where it names the
> prefix `st44-`, the fixture now substitutes whatever `PREFIX` is set to
> when `run-reference.sh` runs, which is `refpipe` by default.

**No one approved this plan.** There was no human to show it to. §1a says to
write it anyway, before building, so that a reader can see what the run
committed to before it had any numbers. This is that record.

## What is being built

The pipeline of `ANSWERS.md`: a stream of temperature readings — sensor id,
location, timestamp, temperature in Celsius, one reading per sensor per second,
1,000 sensors across 100 locations — aggregated to **one row per location per
one-hour bucket**, carrying the total and the count. The bucket comes from the
timestamp inside the reading; the machine clock is never used. Flink
DataStream, Apache Kafka.

Alongside it: a deterministic generator, a verifier that exits non-zero on any
loss, a progress command that reports how much input the pipeline's own output
accounts for, and a Grafana dashboard.

## The unconditionals — declared, not asked

| | |
|---|---|
| the objective | **near-linear scaling across 1, 2 and 4 cores. Each step must return 1.90× or better on a doubling.** Two steps are reported, 1→2 and 2→4, and the one that is led with is 2→4 |
| where it runs | this laptop, in Docker. 8 CPUs, a 9,937 MiB Docker VM |
| the stack | `flink:1.20.1-scala_2.12-java17` and `apache/kafka:3.9.0`, both pinned, both checked native to arm64 before anything starts |
| what is measured | the drain rate of a fixed backlog at each core count, read from **committed broker offsets**, never from the engine's own meter, with per-component CPU, garbage collection, source idle and back-pressure beside it |
| the second reading | this pipeline's output is **per window** — one row per location per hour however many readings arrived — so there is no constant fan-out to divide by. The second vantage is a command that sums the `count` field the pipeline already publishes, run at each end of every window |
| what is proved first | completeness with no tolerances, on a small data set processed to the last record, once cleanly and once with the pipeline **killed mid-run and restarted**. No throughput table is published for a build that has not passed |
| what is built | the job, a deterministic generator, a verifier, the progress command, and the dashboard |
| the shape of the suite | **3 passes per case**, cases 1, 2 and 4 cores, ascending then descending, then the baseline once more as a drift sentinel: **10 measured cases**. That is most of the wall clock |
| the guarantee | **exactly-once checkpointing** for state, and an at-least-once sink made idempotent by publishing the absolute total and count for each (location, hour). Checkpoint interval **10,000 ms** |
| how long it takes | **about two to three hours**, most of it silent. A run that is expected to take twenty minutes gets stopped halfway |
| what it writes | three backlogs of readings, as record counts: a suite backlog, a tiny-proof backlog and a completeness backlog — initially guessed at 450,000,000 / 320,000,000 / 25,000,000 records and re-sized from the rate the tiny proof measures. That is **tens of gigabytes of Kafka log**. Host free disk is checked before every case; it stood at 121 GiB before the run |
| what it occupies | ports **19092** (Kafka), **18081** (Flink REST), **19090** (Prometheus), **13000** (Grafana), **19110** (container CPU exporter). Anything already on those ports will not start. 8 partitions. Every container, volume and network is prefixed `st44-` / `st44_` |
| where to watch it | **`results/PROGRESS.txt`** — one sentence, overwritten: which step of seven, which case of ten, and roughly how long is left. `results/DONE` holds the verdict when it is over |

## How it will be run

`prove.py all` detached, in one stack session:
`up → preflight → completeness → tinyproof → fill → suite → report`, stopping
at the first step that does not pass. The harness is the measurement rig and is
used verbatim — no fork, no second sampler, no hand-written suite runner.

Two things happen before that chain, because they are cheap and because getting
them wrong costs a whole chain:

1. `up` and `preflight` on their own, to learn the `pipeline.max-parallelism`
   that makes 100 location keys land evenly on 1, 2 and 4 subtasks. Flink
   chooses a different key-group count per parallelism when nothing sets it,
   which would hand each case a different key layout.
2. `tinyproof` on its own, to measure the largest case's rate and re-size the
   three backlogs from it rather than from the guess above.

The dashboard's panels are built while the fill runs. Its compose service is in
`pipeline.json` before the first `up`, because that is when the stack is
generated.

## If a step misses 1.90×

§6/§6a: measure the machine first (`prove.py probe` — a pipeline cannot beat
its host), check whether the case is a ceiling rather than a result, then **one
change at a time**, each with its prediction written into `FIXES.md` *before*
it is measured, each measured with `prove.py tinyproof` and reverted if it made
the step worse. **Four changes at most.** Two of §6a's seven levers — reading
the input once, and compressing the sink writes — do not apply to a pipeline
with one aggregation and a per-window output, so this build arrives at the
tuning loop with five.

## What this run will and will not claim

One pipeline on one laptop. If it scales, the sentence is "this pipeline scaled
near-linearly from 1 to 4 cores on one machine", not "Flink scales". The
ceiling is named wherever the measurement finds one.
