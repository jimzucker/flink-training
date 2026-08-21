# flink-training

[![CI](https://github.com/jimzucker/flink-training/actions/workflows/ci.yml/badge.svg)](https://github.com/jimzucker/flink-training/actions/workflows/ci.yml)

Final project: calculate **positions** and **market value** from a stream of block
trades, using Apache Flink and Kafka.

Requirements come from [`assignment.pptx`](assignment.pptx).

---

## Problem statement

Process a stream of **block trades** and publish **positions** aggregated two ways
in parallel — by symbol, and by account/sub-account/symbol. Then join **prices**
to those positions and publish **market value** the same two ways, emitted once
per minute.

A block trade is one order split across several accounts: a trade of 400 shares
allocated to 4 accounts becomes 4 allocations of 100. That split is why the two
aggregations differ — one key per symbol, versus one key per account and symbol.

## Pipeline design

![Pipeline](docs/design/pipeline.svg)

Numbered left to right, in the order the demo talks through them. Full walkthrough,
object model and design rationale: [`docs/design/pipeline-design.md`](docs/design/pipeline-design.md).

## Expected inputs and outputs

The numbers the demo is verified against — each output ties back to a numbered
element in the diagram.

| Parameter | Input |
|---|---|
| Trades | 10 / sec |
| Prices | 1000 / sec |
| Symbols | 4 unique |
| Accounts | 4 unique |
| Allocations per trade | 4 (one per account) |

| # | Sink | Rate | Unique keys |
|---|---|---|---|
| 3 | `positions-by-symbol` | 10 / sec | **4** |
| 4 | `positions-by-account` | 40 / sec | **16** |
| 5 | `mv-by-symbol` | 4 / min | **4** |
| 6 | `mv-by-account` | 16 / min | **16** |

Verified against the real topics by `scripts/verify-topics.sh`. Sinks 3–6 are
produced by the jobs in later steps; what step 02 proves is the input side.

## Running it locally

Full one-command start arrives in step 08. Today the stack is Kafka, and the
generators run on the host.

```bash
source scripts/env.sh                          # Java 17
docker compose -f docker/compose.yml up -d     # Kafka + the six topics
mvn package -DskipTests
./scripts/verify-run.sh                        # exact run, then check the numbers
```

`verify-run.sh` resets the input topics, emits an exact number of records in
replay mode, and asserts the expected-output table against what landed. Every
check is exact — there are no tolerances, because the run is bounded by record
count rather than by elapsed time. Two invocations produce byte-identical topics.

To watch it run continuously instead:

```bash
java -jar generators/target/generators.jar     # wall clock, until stopped
```

Rates and seed come from the environment, which is how the scale cases turn each
knob independently:

| Variable | Default | What it does |
|---|---|---|
| `BOOTSTRAP_SERVERS` | `localhost:9092` | broker |
| `TRADES_PER_SECOND` | `10` | order rate — scale case 1 raises this to 1000 |
| `PRICES_PER_SECOND` | `1000` | price rate — scale case 2 raises this |
| `SEED` | `42` | fixes the content of the sequence |
| `START_EPOCH_MILLIS` | unset | unset uses the wall clock, so latency is measurable; setting it replays from a fixed origin with byte-identical output |
| `MAX_TRADES` | `0` | stop after N trades; `0` is unbounded |
| `MAX_PRICES` | `0` | stop after N prices; `0` is unbounded |
| `DURATION_SECONDS` | `0` | `0` runs until stopped |

Event times are wall clock by default, which is what makes end-to-end latency —
the age of a record when it reaches a sink — measurable at all.

To demonstrate reproducibility, replay from a fixed origin and bound the run by
**record count** rather than by time. Two such runs produce byte-identical
topics; bounding by duration does not, because a run ends mid-cycle and lands on
100 or 101 records depending on where the clock falls.

```bash
START_EPOCH_MILLIS=1700000000000 MAX_TRADES=100 MAX_PRICES=400 \
  java -jar generators/target/generators.jar
```

## Verifying the numbers

_Added in step 5._

## Scale results

_Added in step 8._

---

## Prerequisites

| Tool | Version | Notes |
|---|---|---|
| Java | **17** | Flink 1.20 does not run on newer JDKs |
| Maven | 3.8+ | |
| Docker | with Compose v2 | for Kafka, Flink, Prometheus, Grafana |

This machine's default JDK is newer than Flink supports, so the build pins Java 17:

```bash
source scripts/env.sh    # exports JAVA_HOME for Java 17
mvn validate
```

The build fails fast with a clear message if the JDK is wrong, rather than
surfacing as a stack trace deep inside Flink.

## Versions

| Component | Version | Why |
|---|---|---|
| Flink | 1.20.4 | Last 1.x LTS, and the newest line AWS Managed Service for Apache Flink runs (step 9) |
| Flink Kafka connector | 3.4.0-1.20 | Matching connector release for Flink 1.20 |
| Java | 17 | Required by Flink 1.20 |
| Docker image | `flink:1.20.4-java17` | Pinned to the same patch as the jars |

Flink 1.20.5 exists as a Docker image but is not yet published to Maven Central,
so both are held at 1.20.4 to keep the cluster and the job jars identical.

## Continuous integration

Every push runs three jobs:

| Job | What it proves |
|---|---|
| **Build and test** | compiles on Java 17 and runs all 23 tests, including the Testcontainers integration test that starts its own broker |
| **Verify the expected numbers** | brings up Kafka, runs the generators bounded by record count, and asserts the expected-output table exactly |
| **Shell scripts** | ShellCheck over the verification scripts, since they are part of how correctness is demonstrated |

The second job is the one that matters: it is the same `verify-run.sh` used
locally, so a change that quietly breaks the numbers fails the build rather than
surfacing during a demo.

`main` is protected and requires all three checks to pass, so work reaches it
through a pull request:

```bash
git switch -c step-NN-name
# ... work, commit ...
git push -u origin step-NN-name
gh pr create --fill
gh pr merge --squash          # once CI is green
```

Squash-merging keeps `main` linear with one commit per step, and the step branch
stays for inspection. Direct pushes to `main` are rejected — verified, not
assumed.

## Repository layout

```
.github/workflows/     CI
assignment.pptx        the requirements
pom.xml                parent POM -- versions, dependency and plugin management
common/                domain model and JSON encoding
generators/            block trade and price generators
docker/                local stack: Kafka, Flink, Prometheus, Grafana
scripts/               helper scripts
docs/design/           pipeline diagram and design notes
docs/steps/            per-step evidence: logs, metrics, screenshots
docs/reviews/          review exchanges for each step
JOURNAL.md             what was built at each step, and how it was verified
```

## How this project is built

Delivered against the order the assignment lists its deliverables. Each step is
developed on its own branch, reviewed, then squash-merged to `main` so that
`main` carries exactly one commit per completed step. Step branches are kept
for inspection.

The local Docker stack is stood up early and grown one service at a time, so
every step runs against real Kafka and a real Flink cluster rather than against
scaffolding that is later replaced.

See [`JOURNAL.md`](JOURNAL.md) for the step-by-step record.
