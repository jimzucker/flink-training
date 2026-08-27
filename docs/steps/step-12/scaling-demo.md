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

Eight partitions throughout, 50,000,000 orders queued, a 60-second window after a
240-second warm-up:

| units | orders/sec | vs previous | Flink cores | broker cores | back-pressure |
|---|---|---|---|---|---|
| 1 | 30,505 | — | 1.00 | 0.10 | 24.1% |
| 2 | 65,721 | **2.15×** | 2.00 | 0.25 | 28.5% |
| 4 | 129,056 | **1.96×** | 3.94 | 0.39 | 51.6% |
| 8 | 151,969 | **1.18×** | 4.98 | 0.48 | 72.7% |

It doubles, doubles again, then stops. The resource columns are there so the last
step can be attributed rather than argued about, and they are what make this
demo honest.

**Up to four units, Flink is the constraint and parallelism converts directly
into throughput.** Flink uses every core it is given -- 1.00, 2.00, 3.94 of 1, 2
and 4 -- and the broker sits under half a core throughout.

**At eight units Flink cannot use what it was given.** It reaches 4.98 of 8 cores
while back-pressure climbs to 72.7%: the subtasks are not computing, they are
waiting. Adding cores past this point buys nothing because cores were never what
was short.

**And it is not the broker's CPU either**, which is the part worth pausing on.
The broker is at 0.48 cores -- nearly idle -- while being the thing in the way.
What it has run out of is *write throughput*: 151,969 orders/sec is **759,845
records/sec**, against the ~750,000 a single broker was independently measured to
accept in step 10. Each order becomes five records. The limit is disk and page
cache, not computation, so a broker that looks asleep can still be the ceiling.

"The broker is full" would have been the wrong way to say it, and was how this
was first written: it implies a busy broker. The two cases are only
distinguishable with both CPU columns beside the throughput, which is why they
are in the table.

Partitions are ruled out by construction. They are held at eight for every case,
so even the eight-unit run had a partition per subtask -- varying them with the
units would have made this step unattributable.

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
