# Step 12 — Scaling demo: review

## Round 1

### Feedback

> the infra used is excessive does not make a good story will scare people. we
> have to come up with a better approach to show how flink pipelines scale

> locally and on aws

### The objection was right

Steps 10 and 11 answered the question and produced a bad demo. Making Flink the
bottleneck by overwhelming everything else took 88 million records, 32 vCPUs and
a three-broker MSK cluster — which teaches the wrong lesson before a word is
spoken: that Flink needs a cluster to be worth learning.

**Shrinking Flink is far cheaper than overwhelming Kafka.** Same physics, a
laptop instead of a cluster.

### The key fix

A **unit** is one core and one degree of parallelism, bought together — a KPU in
Managed Service for Apache Flink.

Step 10 varied parallelism alone while the task manager had all eight cores.
Flink already had every core it could use, so more parallelism only spread the
same work over more threads, and the result was flat: 137,931 / 142,857 /
131,147 at parallelism 1, 2 and 4. **You can only demonstrate that something
scales when it is the thing that is constrained.**

Worth recording that this was attempted in step 10 and appeared not to work. The
harness set `TASKMANAGER_CPUS` as a prefix on one `docker compose` command, and
the `run --rm submit` after it could not see the variable — so compose computed a
different desired config and silently recreated the container with no limit,
immediately before each measurement. Those runs reported "1 core sustains 148,148
orders/sec", which was really "unlimited cores". The idea was right; the tooling
fabricated the evidence against it.

## Round 2

### Feedback

> what is constrained that 4 to 8 is flat

> we should add flink CPU, kafka partitions and resources to chart ... or make
> partitions 8 for all runs?

### Actions

Partitions are now held at **8 for every case** rather than raised only for the
8-unit run. Varying them with the units would change two things at once and make
a flat step unattributable — which is precisely what made the first "the broker
is full" claim unsupportable, since those cases ran at 4 partitions and the
8-unit case therefore had half its source subtasks idle.

Flink cores, broker cores and back-pressure are now columns beside the
throughput, so a flat step is read rather than argued.

| units | orders/sec | vs prev | Flink cores | broker cores | back-pressure |
|---|---|---|---|---|---|
| 1 | 30,505 | — | 1.00 | 0.10 | 24.1% |
| 2 | 65,721 | 2.15× | 2.00 | 0.25 | 28.5% |
| 4 | 129,056 | 1.96× | 3.94 | 0.39 | 51.6% |
| 8 | 151,969 | 1.18× | 4.98 | 0.48 | 72.7% |

### What the columns settled

**Neither CPU is the constraint at 4 → 8.** Flink was given eight cores and could
only use 4.98; the broker sat at 0.48. Back-pressure at 72.7% says the subtasks
are waiting, not working.

What runs out is the broker's **write path**: 151,969 orders/sec is 759,845
records/sec against the ~750,000 measured independently in step 10, five records
to an order. The limit is disk and page cache, not computation — which is why a
broker at half a core is still the thing in the way.

**"The broker is full" was the wrong way to say it** and is corrected in the
write-up: it implies a busy broker. The two cases separate only with both CPU
columns beside the throughput, which is why they were asked for and why they
belong there.

### Two bugs, found by running the script rather than reading it

The script had been committed and described as working before it had ever been
executed. It used one bootstrap address for the Kafka CLI, which runs inside the
compose network, and for the generator, which runs on the host and cannot resolve
a service name: the fill wrote nothing and reported "0 orders queued" with no
error anywhere. And a window that found no progress reported 0 orders/sec as
though it were a measurement, rather than saying the warm-up was too short to
clear the cold start.

### Still open

**The cold start is unexplained.** At low unit counts the pipeline sits idle for
150 seconds or more before it begins, which is why the warm-up is 240s. It is
excluded from the measurement rather than understood, and it would show in a live
demo that was not pre-warmed.

**The AWS half has not been run.** The script takes
`COMPOSE=docker/compose.aws.yml` and a bootstrap string, but the claim that the
shape holds there with the ceiling further right is a prediction, not a
measurement.

**The broker has no volume.** It writes to the container's writable layer —
12GB of it — which on Docker Desktop means overlay2 copy-on-write inside a VM.
At roughly 50–60MB/s after compression, far below any SSD, the write path rather
than disk bandwidth is the suspect, and a volume or tmpfs is the cheapest test of
whether that is the ceiling.
