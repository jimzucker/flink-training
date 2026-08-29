# Step 12 — a scaling demo that fits on a laptop

**Bottom line:** Flink converts cores into throughput almost perfectly until
something else becomes the constraint. On a laptop that something is one Kafka
broker, and the curve flattens at 8 units. On AWS, with the broker out of the
way, the same job keeps scaling — 8 units returns 1.73× and Flink uses 7.99 of
the 8 cores it was given. Same code, same script, one variable changed.

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

Eight partitions throughout, backlog drained with the producer stopped, a
60-second window after warm-up.

**Laptop** — one broker, 50,000,000 orders queued:

| units | orders/sec | vs previous | Flink cores | broker cores | back-pressure |
|---|---|---|---|---|---|
| 1 | 30,505 | — | 1.00 | 0.10 | 24.1% |
| 2 | 65,721 | **2.15×** | 2.00 | 0.25 | 28.5% |
| 4 | 129,056 | **1.96×** | 3.94 | 0.39 | 51.6% |
| 8 | 151,969 | **1.18×** | 4.98 | 0.48 | 72.7% |

**AWS** — `c7i.4xlarge` client, 2 × `kafka.m5.large` MSK at RF=1, 120,000,000
orders queued:

| units | orders/sec | vs previous | Flink cores | broker cores | back-pressure | runs |
|---|---|---|---|---|---|---|
| 1 | 43,538 | — | 1.00 | 0.30 | 20.4% | 2 |
| 2 | 64,106 | 1.47× | 2.00 | 0.39 | 20.6% | 2 |
| 4 | 104,912 | 1.64× | 3.99 | 0.45 | 34.9% | 1 |
| 8 | 181,133 | **1.73×** | 7.99 | 0.62 | 65.4% | 1 |

## Reading it

**The laptop doubles, doubles again, then stops.** Up to four units Flink is the
constraint and parallelism converts directly into throughput — it uses every core
it is given, 1.00, 2.00, 3.94 of 1, 2 and 4.

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

**On AWS the flattening disappears.** The broker never exceeds 0.62 cores and is
never the constraint, and Flink takes exactly what it is given at every rung —
1.00, 2.00, 3.99, 7.99. Where the laptop returned 1.18× for its last doubling,
AWS returns 1.73×. That is the whole point of the step in one comparison: the
laptop's ceiling is not Flink's.

## What the numbers count

The reported figure is **orders/sec** — block trades per second, the input rate,
and the most conservative of the three ways to count this.

One order becomes **five records**: four account-side allocations
(`SplitByAllocation`, one per account, and `ReferenceData.ACCOUNTS` has exactly
four) plus one symbol-side position (`ToSymbolUpdate`, fed from the trade rather
than the split, deliberately). So:

| units | orders/sec | allocations/sec | records written/sec |
|---|---|---|---|
| 1 | 43,538 | 174,152 | 217,690 |
| 2 | 64,106 | 256,424 | 320,530 |
| 4 | 104,912 | 419,648 | 524,560 |
| 8 | 181,133 | 724,532 | **905,665** |

That is also what makes the laptop's ceiling legible: 151,969 orders/sec is
759,845 records/sec, right at the measured single-broker limit.

## Honest limits of this data

**The AWS 1→2 step returns only 1.47×, and the ratios rise after it** — 1.47,
1.64, 1.73. Scaling efficiency improving as cores are added is backwards, and it
is not explained. It is not measurement noise: the 1- and 2-unit cases were each
run twice and agree within 2% (43,974/43,101 and 64,669/63,543). An early
hypothesis that parallelism 1 skips the network shuffle was **checked and
refuted** — the dataflow graph has the same three vertices at p=1 as at any other
parallelism.

**The 4- and 8-unit AWS cases were each measured once.** The interesting half of
the claim rests on them, and they are the two points not replicated.

Anchoring the claim at 2 units rather than 1 avoids the anomaly entirely:
**2 → 8 is 2.83× for 4× the cores on AWS, against the laptop's 2.31×.**

## What it costs to run

A laptop, one broker, `docker compose up`, and about four minutes per case. No
cluster, no cloud account, nothing that suggests this needs to be expensive.

```bash
scripts/scale-units.sh                                    # laptop
COMPOSE=docker/compose.aws.yml scripts/scale-units.sh     # against MSK
```

`scripts/scale-units.sh` refuses to report a number it cannot stand behind: it
checks that the CPU limit was actually applied to the container, that a job is
actually running, that the backlog outlasted the measurement window, and that no
previous run still owns the cluster. **Every one of those was a real failure
during steps 10, 11 and 12, and each produced a confident wrong answer** — see
the review for the full list.
