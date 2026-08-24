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
