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
| 3 | `positions-by-symbol` | 10 / sec — one per trade | **4** |
| 4 | `positions-by-account` | 40 / sec — one per allocation | **16** |
| 5 | `mv-by-symbol` | 4 / min — one per key per window | **4** |
| 6 | `mv-by-account` | 16 / min — one per key per window | **16** |

Verified against the real topics by `scripts/verify-topics.sh`. Sinks 3–6 are
produced by the jobs in later steps; what step 02 proves is the input side.

## Running it locally

Full one-command start arrives in step 08. Today the stack is Kafka, and the
generators run on the host.

```bash
docker compose -f docker/compose.yml up -d --build
```

That is the whole thing: the jars are built inside Docker, Kafka and Flink start,
the topics are created, both jobs are submitted and the generators begin
producing. Nothing needs to be built or launched by hand, and no Java is required
on the host. About half a minute from nothing to a running pipeline.

```bash
./scripts/demo.sh              # the same, and tells you what to open
./scripts/verify-cold-start.sh # tears it all down and checks it comes back green
```

### When the data starts

`GENERATOR_START` decides when the generators begin producing. Auto is
convenient, but it gives away the most persuasive moment in the demo — graphs
going from flat to flowing while people watch.

| Value | Behaviour |
|---|---|
| `auto` (default) | starts with the stack, so the dashboard is alive when you open it |
| `manual` | the container stays up and idle until you start it |
| a number | starts that many minutes after the stack comes up |

```bash
GENERATOR_START=manual docker compose -f docker/compose.yml up -d
./scripts/start-generators.sh          # on cue

GENERATOR_START=2 docker compose -f docker/compose.yml up -d   # data arrives in 2 minutes
```

Anything else fails at start rather than being guessed at.

To work on the code rather than run it, build on the host and verify:

```bash
source scripts/env.sh                                     # Java 17
mvn package -DskipTests
./scripts/verify-run.sh                                   # run and verify
```

`verify-run.sh` resets the topics, submits both jobs, emits an exact number of
records, waits for the sinks, and asserts the expected numbers across all six
topics — including that the two position aggregations reconcile, and that the
price each market-value window closed against matches the last price on the
price topic before that boundary. The Flink UI is at <http://localhost:8081>.

The generator runs exactly as it does for the demo: wall-clock event times, paced
in real time, at the demo rates. Nothing about the clock is simulated.

Only the **window** is shortened, and it is a runtime parameter rather than a
change to the calculation.

| | Window | Set by | Why |
|---|---|---|---|
| Specification, and the job default | **60s** | `WINDOW_MS` default | what the requirements state; this is what the design diagram shows |
| Live demo | **10s** | `docker/compose.yml` | a minute of dead air before the market value sinks say anything is a long time in front of an audience |
| Verification | **10s** | `scripts/verify-run.sh` | closing three one-minute windows would need three and a half minutes per run, and a check that slow stops being run |

Event time is the wall clock and pacing is real time in all three — only the
window length differs. The demo runbook says to volunteer that when reaching
sinks 5 and 6, since it is the one deviation from the specification visible on
screen.

How many windows close depends on where the run starts relative to a boundary,
which is a property of a real clock, so the count is read from the data rather
than predicted. Everything inside a window stays exact: a key never skips a
window once it starts, never stops early, and there is exactly one record per key
per window.

To skip the job and check the inputs only:

```bash
WITH_PIPELINE=0 ./scripts/verify-run.sh
```

The position sinks are written **exactly-once**, so anything reading them must
use `read_committed` — an uncommitted read shows records belonging to
transactions that may still abort. A record also becomes readable only when its
checkpoint commits, so the sinks advance once every five seconds rather than
continuously.

Parallelism is a runtime setting, because the demo raises it from 2 to 4 to show
throughput scaling with it:

```bash
PARALLELISM=4 docker compose -f docker/compose.yml --profile submit run --rm submit
```

To prove exactly-once actually holds, kill a task manager mid-run and check the
sinks are still exact:

```bash
./scripts/chaos-exactly-once.sh
```

Market value is emitted once per key per window — a minute as specified, ten
seconds in the demo and the verification — at the **price at close**. A
join advances at its slower input, so an input with no traffic would stall the
watermark and the windows would stop firing while every record that did arrive
was in order. `IDLENESS_MS` prevents that, and setting it to `0` disables it —
which is how the setting is shown to matter: with it off and any partition empty,
sinks 5 and 6 emit nothing at all while Part 1 is unaffected.

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

## Watching it run

```bash
docker compose -f docker/compose.yml up -d --wait kafka jobmanager taskmanager prometheus grafana
```

| | |
|---|---|
| Dashboard | <http://localhost:3000/d/flink-training/block-trade-pipeline> |
| Flink UI | <http://localhost:8081> |
| Prometheus | <http://localhost:9090> |

Sources on the left, sinks on the right, numbered as on the pipeline diagram. The
key counts are the expected-output table made visible: sinks 3 and 5 hold 4 keys,
sinks 4 and 6 hold 16.

Flink has no metric for distinct keys, so the jobs publish one. Keys are
partitioned across subtasks, which is what makes summing the gauge give the total
without double counting. It counts from when a subtask started, so a restart
resets it until every key has been seen again.

To capture the dashboard as an image — this is how the pictures for the deck are
made:

```bash
./scripts/capture-dashboard.sh
```

Grafana renders it itself rather than relying on a browser. That is not
fastidiousness: a browser extension on the presenting machine was enough to stop
panels drawing entirely, while the dashboard was perfectly correct.

## Verifying the numbers

Every figure in the expected-output table is asserted on each run, with no
tolerances anywhere:

```bash
./scripts/verify-run.sh
```

It recomputes both halves of every market value independently from the topics
that fed them — the closing price from the price topic and the closing quantity
from the position topic — so neither rests on the job agreeing with itself.

### Explaining any number

Any figure on the dashboard can be walked back to the block trade that caused it:

```bash
./scripts/trace-trade.sh T000000098
```

```
① orders                T000000098  BUY 400 AAPL, 4 allocations of 100
③ positions-by-symbol   AAPL            -2800
④ positions-by-account  ACC1..4/SUB1/AAPL  -700 each
                        the four accounts sum to -2800, which is what sink 3 reports  OK
⑤ mv-by-symbol          -2800 x 74.25 = -207900.00
⑥ mv-by-account         -700 x 74.25 =  -51975.00 each
                        the account market values sum to -207900.00, which is what sink 5 reports  OK
```

The requirements ask for the numbers to be explainable and for logging to be
changed until they are. Every record carries the trade that last moved it, which
is what makes the walk back possible.

Full walkthrough for presenting it: [`docs/demo-runbook.md`](docs/demo-runbook.md).

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
jobs/                  Flink jobs
docker/                local stack: Kafka, Flink, Prometheus, Grafana
docker/grafana/        dashboard and provisioning
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
