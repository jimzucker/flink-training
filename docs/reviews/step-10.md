# Step 10 — Scale: review

## Round 1

### Asked

Whether re-running the two required cases with the fixed generator was enough to
call the scaling claim proven.

### Feedback

> That's not a good approach you should force it fails and Eval it, let's run at
> 100k/sec and parallel 4, then decide based on that

> We should target a low latency at 4 parallel, then drop to 2 and see if your
> perf drops in half

### Actions

| Feedback | Action |
|---|---|
| Force it to fail and evaluate the failure | Overdrove to 315,000 orders/sec. Lag diverged by ~245k records/sec, latency went from 520 ms to 110 s, checkpoints from 13 ms to 700 ms — and nothing crashed |
| Run at parallelism 4, then 2, and compare | Both run, plus parallelism 1. The result is flat, and the reason is now measured rather than guessed |

### What forcing the failure turned up

The failure mode is worth more than the passing runs. Under saturation Flink
back-pressures the source and keeps producing **correct** output, just further
and further behind: no failed checkpoint, no restart, no exception. Latency rose
two hundredfold, but not because processing got slower — each record was waiting
behind a backlog of thirty million. Latency under saturation measures the queue,
not the work.

That is the shape of the problem to alert on: invisible in correctness, invisible
in throughput, obvious only in lag.

The run also showed **parallelism 2 beating parallelism 4** — three generator
JVMs and a Flink cluster were fighting over eight cores. Removing the producer
from the measurement entirely, by filling the topics first and timing a drain of
an identical 8,000,000-order backlog, gives the honest number:

| parallelism | time to drain | orders/sec |
|---|---|---|
| 1 | 58 s | 137,931/s |
| 2 | 56 s | 142,857/s |
| 4 | 61 s | 131,147/s |

Flat. My first explanation was cores — parallelism 1 already running eight task
threads at 385% CPU of the 800% available — and it was wrong. That reading has
~40% of the box idle while throughput is pinned, which is not what a CPU ceiling
looks like. Corrected in round 3 below.

Two corrections fell out of this. The latency queries named operator `by_symbol`
instead of `aggregate_by_symbol` and had been reading a flat 0 ms. And the
generator's old ceiling was the pacer, now gone: 20,000/sec asked returns
20,000/sec, 50,000 returns 50,000, verified directly.

## Round 2

### Feedback

> Are we tracking backpressure in grafana?

> Are we monitoring in grafana operator usage? To spot - Found it — one operator
> is pinned while everything else idles/ are we monitoring consumer lag?

> Also why r u not catching ci fails proactively

### Actions

| Feedback | Action |
|---|---|
| Track back-pressure | Added, as busiest-task and most-back-pressured-task lines with stat tiles |
| Track per-operator usage, to spot one operator pinned | The first attempt used `max()` across tasks, which shows that *something* is pinned but not *what* — the account-chain diagnosis had come from an ad-hoc query, so the dashboard could not have found it. Now broken out per task as sorted bar gauges |
| Track consumer lag | Added, per source. This was the real gap: lag is what *defines* saturation and there was no panel for it at all |
| Catch CI failures proactively | `scripts/precheck.sh` plus a `.githooks/pre-push` hook |

### On the metric choice

Lag uses Flink's `pendingRecords` rather than the Kafka consumer's
`records_lag_max`, which reports NaN for idle partitions and is a maximum rather
than a sum — adding it across subtasks would not mean anything.

The bar gauges needed one trick: Flink names a chained task after every operator
in it, so `aggregate_by_account____positions_by_account:_Writer____...` is the
label. `label_replace` trims it to the leading operator, applied twice so that a
task with no chain separator keeps its full name instead of losing its label.

### On the CI failures

Fair criticism, and the fix is process rather than apology. The failure was two
ShellCheck errors — `$@` interpolated inside a string, and an unused loop
counter — that take three seconds to catch locally. `scripts/precheck.sh` runs
the two CI jobs that need no Docker stack (ShellCheck and `mvn verify`) in about
fifteen seconds, and `.githooks/pre-push` runs it on every push once
`core.hooksPath` is set. **Every CI failure on this branch would have been caught
by it.** The two stack jobs still run on CI only; they need minutes and a quiet
machine.

Checking the workflow also turned up a stale claim: the README said CI runs three
jobs. It runs four — cold start was missing.

### Outcome

CI green on all four jobs. The scaling claim is not proven and is recorded as not
proven, with a specific, measured reason and a concrete proposal for what would
prove it: scale cores with parallelism, which is how Managed Service for Apache
Flink actually works — KPUs are one vCPU each, and parallelism and KPUs move
together. A demonstration that raises parallelism while holding cores fixed is
not testing the deployment model.

## Round 3

### Feedback

> Are we tracking cpu core usage/ network bandwidth? Anything else we should
> monitor to stop resource bottlenecks/ are charts grouped logically and in
> order of most used?

> I don't understand this are claims we don't have enough cpu for scale test
> false?

### Actions

| Feedback | Action |
|---|---|
| Track CPU and network | Added, along with heap, GC, network buffers and free task slots. CPU is derived from `rate(JVM_CPU_Time)/1e9` so it reads in cores rather than a fraction of an unstated whole |
| Group the charts, most-used first | Four rows: the pipeline (the demo), then latency, saturation, resources. Consumer lag moved to the head of the saturation section from the very bottom of the dashboard |
| Challenge the CPU claim | **The claim was wrong.** Retracted and replaced with a measured cause |

### The CPU claim was wrong

The challenge was right, and the arithmetic in my own message gave it away: a
TaskManager at 385% of 800% has ~40% of the machine idle. That is not what a CPU
ceiling looks like, and "the box is the constraint at every parallelism" did not
follow from it.

What the evidence actually supports, each candidate excluded by measurement:

| Candidate | Test | Result |
|---|---|---|
| TaskManager CPU | container CPU during a drain | 385% at parallelism 1, 475% at 4, of 800% — idle capacity throughout |
| A container CPU cap | `cpu.max` in the cgroup | `max`, quota `-1`, 8 processors — none |
| Kafka reads | `kafka-consumer-perf-test` | 1,908,570 records/sec — fourteen times the pipeline's rate |
| Partition count | drain with 16 partitions instead of 4 | 137,931/sec against 131,147 — unchanged |
| Checkpointing | drain at a 10s interval instead of 1s | no better |
| **Kafka writes** | **4 concurrent producers, `acks=all`** | **750,000 records/sec aggregate** |

Each order becomes five records — one symbol-side, four account-side — so at
137,000 orders/sec the pipeline issues **685,000 writes/sec, 91% of the broker's
aggregate capacity**. That single fact explains every observation: flat across
parallelism because the broker is shared and fixed, indifferent to partition
count because the limit is the broker process, TaskManager cores half idle
because its threads are blocked on acknowledgements rather than computing, and
the account chain pinned at 99% because it issues four of the five writes.

Low CPU together with high back-pressure was the signal I misread. A thread
waiting on a Kafka acknowledgement is neither busy nor idle for want of work, so
"there are spare cores" and "it cannot go faster" are both true and neither
explains the other.

The corrected conclusion is more useful than the one it replaces: **Flink
parallelism scales the work inside the job and cannot scale anything outside it.**

### A second measurement error, found on the way

Comparing generator compression appeared to show gzip and lz4 both at exactly
150,000 orders/sec — the same suspiciously round number, because `MAX_TRADES` was
3,000,000 and the window 20 seconds, so both had simply finished early. With a
cap that could not be reached: **gzip 386,892/sec, lz4 878,949/sec.**

gzip had been chosen to avoid the native-library warning lz4 triggers, so a demo
given from a console starts clean. That warning arrives on Java 24+; this project
targets 17, which is what the images and CI run, and under Java 17 lz4 produces
zero warning lines — verified. The generator now defaults to lz4.

### Outcome

CI green. The dashboard carries the diagnostic path that would have found this
without the ad-hoc queries, and the CPU panel now warns against the exact
misreading made here.

### A latent tie-break bug, surfaced by the compression change

Switching the generator to lz4 broke CI on a check that had passed for ten steps:
`price at close differs from the price topic`. The change of codec altered
producer batching enough to move a tie onto a window boundary.

The tie is real. In a 40,000-price run the topic holds **448 `(symbol,
eventTime)` pairs sharing a millisecond**, and for one of them:

| | value |
|---|---|
| what the job emitted | 317.75 |
| what the check expected | 318.50 |
| last for that symbol in topic order | 317.75 |

The job was right and the check was wrong. It recomputed the closing price with
`sort -n | tail -1`, and sort has no second key to order a tie by, so it picked
an arbitrary member. The job takes the last to arrive, which is well defined:
prices are keyed by symbol, so a symbol's ticks live in one partition and reach
the job in publication order. The check now reads the dump in topic order.

The same tie exists on the position side — 1 pair in `positions-by-symbol`, 4 in
`positions-by-account` — and there it was a genuine bug in the job, not just the
check. Positions are keyed by trade id and spread across partitions, so arrival
order varies between runs; taking whichever arrived last could close a window on
the running total from *before* the other trade and understate the position. It
had simply never landed on a boundary.

`MarketValueAtClose` now breaks ties on `updateCount`, the accumulation sequence
for the key, so the greater one is genuinely later — a tie-break on the data
rather than on timing, identical on every replay. The check breaks it the same
way, because a check that breaks it differently is checking a different rule.

Verified by running the full verification twice: all checks pass, with no
tolerances.

That the codec change surfaced this is worth keeping in mind about the whole
suite: these checks are exact, but exactness only pins down behaviour the inputs
actually exercise, and a timing change can move which cases those are.

## Round 4

### Feedback

> Can we solve the broker cap with more brokers? / Scale the Kafka so we achieve
> our flink scaling test goal

> Are we tracking disk usage and broker resource usage on grafana? / what do you
> suggest to tune, maybe 2 vs 3 brokers?

> Can we find a way to show scaling it could be 1-2 or 1-4 if 2-4 not
> demonstrable, we can change partitions in addition to parallelism if that make
> sense for a demo

### Actions

| Feedback | Action |
|---|---|
| Scale Kafka to lift the cap | Built a three-broker KRaft cluster. **It halved throughput.** Kept as an override, not a default |
| Track disk and broker resources | Attempted with cAdvisor; it reports only the root cgroup on Docker Desktop for Mac, so it would have shipped a page of empty panels. Removed. Still outstanding — see below |
| What to tune | Sink compression and batching, worth ~13% and a correctly-signed curve. Not 2 vs 3 brokers: the direction is measured and it is down |
| Show scaling, pairing partitions if needed | Paired cores, partitions and parallelism. 1 → 2 is real at ~25%; 2 → 4 is inside the noise |

### Three brokers were worse, and the reason generalises

| | 1 broker | 3 brokers |
|---|---|---|
| drain, parallelism 1 / 2 / 4 | 137,931 / 142,857 / 131,147 | 72,072 / 74,074 / 76,190 |
| write ceiling, 4 producers | 750,000 records/sec | 545,000 records/sec |

Three broker JVMs held 2.5GB of a 7.65GB VM where one held 1GB, and Kafka writes
are fast only while the page cache that leaves behind is large enough; three logs
then share one disk. **More brokers add capacity when they add machines; on one
machine they divide it.** Two brokers would sit between these, which is to say
still worse than one — the direction is what the measurement settles, so it was
not worth a run.

Correctness was re-verified on the three-broker cluster before it was rolled
back: all checks passed with no tolerances, so the cluster is sound and simply
slower here.

### Why no arrangement shows scaling

Every dial on one machine, and the answer barely moves:

| Configuration | orders/sec |
|---|---|
| 1 broker, 4 partitions, parallelism 1 / 2 / 4 | 137,931 / 142,857 / 131,147 |
| 1 broker, 12 partitions, parallelism 1 / 2 / 4 | 142,857 / 166,666 / 153,846 |
| 3 brokers, 12 partitions, parallelism 1 / 2 / 4 | 72,072 / 74,074 / 76,190 |
| tuned sinks, parallelism 1 / 2 / 4 | 153,846 / 166,666 / 173,913 |
| cores + partitions + parallelism paired, 1 / 2 / 4 | 148,148 / 190,476 / 173,913 |
| the same, repeated | 148,148 / 173,913 / 190,476 |

The clinching number: **one partition, parallelism 1, one core sustains 148,148
orders/sec** — 765,000 records/sec written against a broker measured to accept
750,000. There is no configuration in which Flink is the constraint, so there is
nothing for parallelism to relieve.

Repeating the paired run swapped 2 and 4, which is the honest reading of that
row: 1 → 2 is a real ~25% gain, 2 → 4 is noise.

### Still outstanding

Broker and disk metrics are not on the dashboard. cAdvisor was the wrong tool for
this host; the right one is a JMX exporter alongside the broker, which would give
bytes in and out, request handler idle time, log flush latency and log size on
disk — the last being the disk-usage question directly. Worth doing before the
AWS step, where the broker is the thing being sized.
