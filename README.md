# flink-training

Final project: calculate **positions** and **market value** from a stream of block
trades, using Apache Flink and Kafka.

Requirements come from [`assignment.pptx`](assignment.pptx).

---

## Problem statement

_Added in step 1._

## Pipeline design

_Added in step 1 — `docs/design/pipeline.svg`._

## Expected inputs and outputs

_Added in step 2._

## Quick start

_Added in step 6 — a single `docker compose up`._

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

## Repository layout

```
assignment.pptx        the requirements
docker/                local stack: Kafka, Flink, Prometheus, Grafana
pom.xml                parent POM — versions, dependency and plugin management
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
