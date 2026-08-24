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

Flat. And measurably so: the job graph is eight chained task groups, so
parallelism 1 is already eight task threads at **385% CPU of the 800% available**.
Parallelism 4 asks for thirty-two threads on the same cores and gets 475%. On a
fixed core count, parallelism alone does not raise throughput — which is a real
finding, not a failed measurement.

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
