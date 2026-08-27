# Step 12 — a scaling demo that fits on a laptop

Steps 10 and 11 answered the question and produced a bad demo. Making Flink the
bottleneck by overwhelming everything else took 88 million records, 32 vCPUs and
a three-broker MSK cluster — which teaches the wrong lesson before a word is
spoken, that Flink needs a cluster to be interesting.

**Shrinking Flink is far cheaper than overwhelming Kafka.** Same physics, a
laptop instead of a cluster.

## The unit

One unit is **one core and one degree of parallelism, bought together** — which
is exactly what a KPU is in Managed Service for Apache Flink. Parallelism without
cores only spreads the same work over more threads; that is precisely why step 10
could never show scaling, since Flink already had every core it could use and the
broker was the ceiling.

## The result, on one laptop with one broker

| units | orders/sec | vs previous |
|---|---|---|
| 1 | 31,109 | — |
| 2 | 82,760 | **2.66×** |
| 4 | 149,164 | **1.80×** |
| 8 | 154,543 | **1.04×** |

Four beats, each with a reason you can point at on the dashboard.

**One core is mostly overhead.** The JVM's garbage collector, the Netty stack and
four Kafka clients all want that core, so the first unit spends much of itself on
housekeeping. This is why the first doubling returns 2.66× rather than 2× — the
second core is nearly all pipeline. It is also worth saying out loud when someone
is sizing KPUs: **your first vCPU does not do a full vCPU of work.**

**Then it scales.** Two to four returns 1.80×, near enough linear, and this is the
part people expect.

**Then it stops.** Four to eight returns 1.04% — three and a half percent for
double the cores. The broker is full: 149,164 orders/sec is 745,820 records/sec,
against the ~750,000 a single broker was independently measured to accept in step
10. Each order becomes five records, so the pipeline reaches the broker's ceiling
at about 150,000 orders/sec no matter how much CPU it is given.

That last beat is the one worth having. Most scaling demos stop after the second
and leave the audience believing throughput is bought with parallelism alone.

## What it costs to run

A laptop, one broker, `docker compose up`, and about four minutes per case. No
cluster, no cloud account, nothing that suggests this needs to be expensive.

`scripts/scale-units.sh` runs it, and refuses to report a number it cannot stand
behind: it checks that the CPU limit was actually applied to the container, that
a job is actually running, and that the backlog outlasted the measurement window.
All three of those were real failures during steps 10 and 11, and each one
produced a confident wrong answer.

## The same demo on AWS

The script takes `COMPOSE=docker/compose.aws.yml` and a bootstrap string, so the
identical experiment runs against MSK. The shape should be the same and the
ceiling should sit further right, because step 11 measured three brokers
accepting 1,300,265 records/sec where one accepted 711,700.

That makes the AWS run a one-slide appendix rather than the main event: *the
ceiling is the broker, and here is what it costs to move it.*
