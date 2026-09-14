# Prove it scales: a Claude skill that refuses to publish a bad benchmark

Draft article for engineers. Every figure below comes from the record in this
repository — `docs/skill-validation/` and the skill itself at
`.claude/skills/prove-it-scales/`.

---

Building a data pipeline is the easy half. Producing a scaling number that
survives a skeptical reader is the hard half.

Most scaling claims fail on the measurement, not the code. The container you
capped at four cores wasn't actually capped. The one-core baseline ran a
different job graph from the four-core case. The number came from one pass,
and the next pass would have read 15% differently. The engine's own throughput
metric starved at exactly the load you cared about.

**prove-it-scales** is a Claude Code skill that builds the pipeline and then
measures it under a discipline strict enough that those mistakes can't reach
the table. When the measurement is compromised, it refuses — and says why.

It currently targets **Apache Flink on Kafka**, running in Docker on one
machine.

## What it does

**It interviews before it builds.** One question at a time, each with a
default: what goes in and what comes out, whether one input becomes several
outputs, how many distinct keys there are, what has to be exactly right, who is
watching, and — written down verbatim — the claim you want to make. Every later
decision is judged against that sentence.

**It builds in reviewable steps**, each ending with the system running and
measured, not just compiling.

**It proves nothing was lost before it measures anything.** Completeness is a
separate run on a backlog small enough to drain to the last record. The
generator writes a manifest of the expected total per key, computed from the
input alone, and the verifier compares every key against it with no tolerance.
Then it kills a worker mid-drain and checks again, because a delivery guarantee
is untested until something has failed. No throughput table is published for a
build that hasn't passed.

**Then it measures scaling** — by capping one worker's CPU at 2 and 4 cores on
a laptop, with parallelism equal to the cap, and draining a fixed backlog. It
leads with the step ratio you would actually buy: two units to four.

## The harness refuses

The measurement harness ships with the skill, and the skill tells Claude to use
it rather than write its own. Ten early validation runs each rewrote the
harness from the written rules, and every one re-decided something already
decided.

The harness has 21 refusals. Each exists because a run paid for it. A few:

| it refuses when | why that rule exists |
|---|---|
| the worker isn't at ≥95% of its CPU cap | you can only show something scales when it is the thing constrained |
| the job graph differs between cases | a one-core baseline written to chain into a single vertex read **2.16×** from 1→4; the same job with every case's graph read **3.26×** |
| CPU was read from `docker stats` | sampled, it refused a valid case at 94.4%; the cumulative cgroup counter said 97.8% |
| throughput came from the engine | at full load the engine's metric service starves — one case under-reported itself by 3× — so rates come from the broker's committed offsets |
| the broker hit its memory limit inside the window | at 2 GiB the broker read **96.4% of cap** — above the floor, so nothing else would have caught it — while its starved page cache dragged the rate down |
| a case's passes spread more than 10% | every case runs at least twice; that case and every ratio it's in are marked unreportable |
| a step returns less than 95% of linear | a valid table that doesn't scale is a result about the pipeline, not a table to publish |

A refusal about the rig stops the suite. A refusal about one case's data marks
that case and moves on. And a case that misses the cap is recorded as a
**ceiling** — where scaling stops — not deleted.

## Validated on itself

The skill was run 29 times from a clean room: a fresh directory, one prompt, no
human help, and Claude not allowed to read the repository that wrote the rules.
Every rule that a run broke became a guard with a self-test, or was deleted.

The harness alone, on a pipeline that was already built, passed its whole chain
in 66.2 minutes on the first attempt: **1→2 = 1.99×, 2→4 = 2.00×**.

The most useful result came from turning it on the hand-written demo it was
meant to reproduce. At 4,096 keys, on the same rig with the same guards:

| | 2→4 |
|---|---:|
| hand-written demo | **1.988** |
| Claude-built pipeline, run 24 | 1.906 |
| Claude-built pipeline, run 23 | 1.793 |

The Claude-built pipelines were **two to three times faster** in absolute
throughput — 506,286 and 668,254 records/s at four cores against the demo's
238,804. They do less work per record, and they lost more when the cores
doubled. The harness reported both. Neither pipeline could be measured at
32,768 keys on a 7.8 GB VM; both hit the garbage-collection ceiling, and
the harness said so instead of printing a number.

## Using it

**You need:** Docker, Python 3 (the harness is standard library only), and
JDK 17. The stack runs `flink:1.20.1-scala_2.12-java17` and
`apache/kafka:3.9.0`.

**Install** by copying the skill into your Claude Code skills directory:

```
git clone https://github.com/jimzucker/flink-training
cp -r flink-training/.claude/skills/prove-it-scales ~/.claude/skills/
```

**Then ask Claude** to build a pipeline and prove it scales. The skill starts
the interview.

**What you supply,** or what Claude builds with you:

| piece | contract |
|---|---|
| a job jar | the Flink job, accepting bootstrap, topics, parallelism and checkpoint interval as arguments |
| a generator | deterministic — two fills with one seed are byte-identical — and writes a manifest of expected totals |
| a verifier | drains the outputs and exits non-zero on any loss |
| `pipeline.json` | describes the three, the cases, passes, backlog and caps; start from `harness/pipeline.example.json` |

**Run the whole chain** with `prove.py all`:
`up → preflight → completeness → tinyproof → fill → suite → report`, stopping
at the first step that doesn't pass. It writes `results/DONE` with the verdict.
A full run takes about an hour on a laptop. `--quick` takes about 48 minutes
and stamps its table as not publishable — it tells you the rig runs clean,
not what the ratio is.

## What a result looks like

Lead with the 2→4 step ratio, its interval and its spread. Put the resource
columns beside the throughput — what the worker used, what the broker used,
back-pressure — so waiting is distinguishable from working. Record the build
hash beside every number. And say where it stops: naming the ceiling is what
makes the rest of the table credible.

## Limits

- **Flink on Kafka, today.** The interview and the measurement rules are
  general; the harness is not.
- **One machine.** The axis it measures is one worker growing, not workers
  multiplying across a network — a different measurement, with fixed costs
  paid again per worker.
- **About an hour per full run.** The completeness gate, the tiny proof and the
  fill don't shrink with `--quick`.

The skill, the harness and all 29 validation runs are public at
https://github.com/jimzucker/flink-training.
