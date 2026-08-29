# Step 12 — a scaling demo that fits on a laptop

**Bottom line:** one core and one degree of parallelism, bought together, is a
unit. Buy units and throughput follows — until something else becomes the
constraint. On a laptop with one broker that happens at 8 units, and the resource
columns say exactly why. The whole demo is four minutes a case on hardware you
already own.

## Why this step exists

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

Partitions are held at **eight for every case**, so even the eight-unit run had a
partition per subtask. Varying them with the units would have made the step
unattributable.

## The result

One laptop, one broker, eight partitions throughout, 50,000,000 orders queued,
a 60-second window after warm-up, producer stopped so nothing varies but units:

| units | orders/sec | vs previous | Flink cores | broker cores | back-pressure |
|---|---|---|---|---|---|
| 1 | 30,505 | — | 1.00 | 0.10 | 24.1% |
| 2 | 65,721 | **2.15×** | 2.00 | 0.25 | 28.5% |
| 4 | 129,056 | **1.96×** | 3.94 | 0.39 | 51.6% |
| 8 | 151,969 | **1.18×** | 4.98 | 0.48 | 72.7% |

```bash
scripts/scale-units.sh
```

**It doubles, doubles again, then stops.** Three things follow, in order.

**Up to four units Flink is the constraint, and parallelism converts directly
into throughput.** Flink uses every core it is given — 1.00, 2.00, 3.94 of 1, 2
and 4 — and the broker sits under half a core throughout.

**At eight units Flink cannot use what it was given.** It reaches 4.98 of 8 cores
while back-pressure climbs to 72.7%: the subtasks are not computing, they are
waiting. Adding cores past this point buys nothing, because cores were never what
was short.

**And it is not the broker's CPU either**, which is the part worth pausing on.
The broker is at 0.48 cores — nearly idle — while being the thing in the way.
What it has run out of is *write throughput*: 151,969 orders/sec is **759,845
records/sec**, against the ~750,000 a single broker was independently measured to
accept in step 10. The limit is disk and page cache, not computation, so a broker
that looks asleep can still be the ceiling.

"The broker is full" would have been the wrong way to say it, and was how this
was first written: it implies a busy broker. The two cases are only
distinguishable with both CPU columns beside the throughput, which is why they
are in the table.

## What the numbers count

The reported figure is **orders/sec** — block trades per second, the input rate,
and the most conservative of the three ways to count this.

One order becomes **five records**: four account-side allocations
(`SplitByAllocation`, one per account, and `ReferenceData.ACCOUNTS` has exactly
four) plus one symbol-side position (`ToSymbolUpdate`, fed from the trade rather
than the split, deliberately).

| units | orders/sec | allocations/sec | records written/sec |
|---|---|---|---|
| 1 | 30,505 | 122,020 | 152,525 |
| 2 | 65,721 | 262,884 | 328,605 |
| 4 | 129,056 | 516,224 | 645,280 |
| 8 | 151,969 | 607,876 | **759,845** |

That last figure is what makes the ceiling legible: it sits right on the measured
single-broker limit, and it is why the curve flattens where it does.

## Is that Flink's ceiling, or the laptop's?

The laptop's. The same script, unchanged, was run against a two-broker MSK
cluster on a `c7i.4xlarge` client — one question, one answer:

| | laptop | AWS |
|---|---|---|
| 4 → 8 units | **1.18×** | **1.73×** |
| Flink cores at 8 units | 4.98 of 8 | **7.99 of 8** |
| broker cores at 8 units | 0.48 | 0.62 |

With the broker no longer in the way, Flink uses everything it is given and the
flattening disappears. Nothing about the job changed.

That is the entire role of the AWS run — a confirmation, not a second demo. Its
numbers are noisier than the laptop's and carry caveats that the laptop's do not:
the AWS step ratios *rise* rather than decay, which is backwards and unexplained,
and the 4- and 8-unit points were each measured once. Full data and caveats:
[`units-aws.txt`](units-aws.txt).

**Quote the laptop table. Keep the AWS row in reserve for the question above.**

## What it costs to run

A laptop, one broker, `docker compose up`, and about four minutes per case. No
cluster, no cloud account, nothing that suggests this needs to be expensive.

`scripts/scale-units.sh` refuses to report a number it cannot stand behind: it
checks that the CPU limit was actually applied to the container, that a job is
actually running, that the backlog outlasted the measurement window, and that no
previous run still owns the cluster. **Every one of those was a real failure
during steps 10, 11 and 12, and each produced a confident wrong answer** — see
the review for the full list.
