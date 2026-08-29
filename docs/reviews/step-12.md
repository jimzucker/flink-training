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

## Round 3 — the AWS half, and one open item closed

**Prompt:** run the AWS side so the comparison rests on measurement rather than
prediction, one case at a time, reporting each before proceeding.

Four cases ran: 1, 2, 4 and 8 units against a `c7i.4xlarge` client and a
two-broker MSK cluster at RF=1. The 1- and 2-unit cases were each run twice.

| units | orders/sec | vs previous | Flink cores | broker cores | back-pressure | runs |
|---|---|---|---|---|---|---|
| 1 | 43,538 | — | 1.00 | 0.30 | 20.4% | 2 |
| 2 | 64,106 | 1.47× | 2.00 | 0.39 | 20.6% | 2 |
| 4 | 104,912 | 1.64× | 3.99 | 0.45 | 34.9% | 1 |
| 8 | 181,133 | **1.73×** | 7.99 | 0.62 | 65.4% | 1 |

The prediction held on the part that mattered — the laptop's flattening is the
broker, and removing it removes the flattening — but **not** on the part that had
been asserted more confidently. The shape is not the same with the ceiling moved
right; the laptop's curve decays (2.15×, 1.96×, 1.18×) while the AWS curve
*improves* (1.47×, 1.64×, 1.73×).

**The cold start is explained, and was a bug.** Sink transactional-ID prefixes
were fixed strings, so across roughly ten runs Kafka accumulated 98,677
transactional IDs that every subsequent sink startup had to fence before emitting
a record. That is the 150s-plus idle at low unit counts. Scoping the prefix per
run (`TRANSACTIONAL_ID_SCOPE`) took first output from **470s to 31s** and
checkpoints in the window from **0 to 16**. The 240s warm-up exists to hide a
defect, and with the defect fixed the AWS runs used 120s.

**A hypothesis was raised and refuted rather than kept.** The rising ratios
looked like a one-time shuffle cost paid between parallelism 1 and 2. Snapshotting
the dataflow graph during a p=1 run showed the same three vertices as at any other
parallelism, so nothing collapses into a single chain and the explanation is
wrong. It is recorded as unexplained.

**Three process failures, all of them mine.** An entire AWS run was invalidated
because `compose.aws.yml` had no `deploy.resources.limits.cpus`, so it measured
parallelism alone with 16 vCPUs available — and the guard that should have caught
it exempted non-local compose files. An orphaned run held the cluster for 26
minutes because `exec bash` replaced the command line that `pkill` was matching,
starving every later submission of slots. And the cluster was left running
overnight, roughly nine and a half hours, because a question was asked instead of
the idle timer being started.

## Still open

**The AWS curve's rising ratios are unexplained**, and the two points that carry
the interesting half of the claim — 4 and 8 units — were each measured once.
Replicating them is the next thing worth doing.

**The broker has no volume.** It writes to the container's writable layer —
12GB of it — which on Docker Desktop means overlay2 copy-on-write inside a VM.
At roughly 50–60MB/s after compression, far below any SSD, the write path rather
than disk bandwidth is the suspect, and a volume or tmpfs is the cheapest test of
whether that is the ceiling.
