# Step 10 — proving it scales, and what this machine can actually show

## The two cases the requirements specify: both pass

| | parallelism | orders/s asked | prices/s | orders through | order latency p50 | p99 | checkpoint |
|---|---|---|---|---|---|---|---|
| baseline | 2 | 10 | 1000 | 8/s | 519 ms | 1023 ms | 8 ms |
| **case 1** | 2 | 1000 | 1000 | **1000/s** | **522 ms** | 1011 ms | 14 ms |
| **case 2** | 2 | 10 | **20000** | 8/s | **513 ms** | 966 ms | 6 ms |

**Case 2** settles a question left open in step 05. Prices are broadcast to every
subtask, because the account side is keyed on account/sub-account/symbol and
cannot be joined to a symbol-keyed stream by key alone. The concern was that
broadcasting would put the price rate through the same threads doing order work.
At twenty times the price rate it does not: 513 ms against 519 ms. Measuring
rather than pre-reducing the price stream was the right call — pre-reducing would
have traded away the exact price-at-close for a problem that is not there.

## Forcing it to fail

Raising the rate until it breaks tells us more than any passing run. Three
generator JVMs offering 315,000 orders/sec into parallelism 4:

| | offered | sustained | lag drift | latency p50 | checkpoint | busy | back-pressure |
|---|---|---|---|---|---|---|---|
| parallelism 4 | 315,854/s | 37,419/s | **+242,861/s** | **110,088 ms** | 270–720 ms | 100% | 53% |
| parallelism 2 | 309,012/s | 43,300/s | **+248,766/s** | **107,080 ms** | 200–714 ms | 98% | 42% |

This is what saturation looks like, and it is worth reading in detail:

- **Lag diverges.** The gap between orders produced and orders processed grows by
  about a quarter of a million records every second and never recovers. That, not
  the throughput number, is the definition of saturated.
- **Latency degrades by a factor of two hundred**, from ~520 ms to ~110 seconds.
  It is not that processing got slower; each record now waits behind a backlog of
  thirty million. Latency under saturation measures the queue, not the work.
- **Checkpoints degrade with it**, from 13 ms to 270–720 ms against a 1 s
  interval — approaching the point where checkpointing itself would start to fail.
- **Nothing crashed.** No failed checkpoint, no restart, no exception. Flink
  back-pressures the source and keeps producing correct output, just further and
  further behind. That is the failure mode to expect and design alerts for: it is
  invisible in correctness and obvious in lag.

But the run also shows why it is not a scaling test: **parallelism 2 beat
parallelism 4** (43,300 against 37,419). Three generator JVMs and a Flink cluster
were competing for the same eight cores, so the extra parallelism bought
contention rather than capacity. That measures the laptop, not the pipeline.

## Measuring the pipeline instead: drain a fixed backlog

Removing the producer from the measurement means filling the topics first and
then starting the job, so what is timed is Flink draining a known backlog with
the cores to itself. The same 8,000,000-order backlog, drained three times:

| parallelism | time to drain | orders/sec | peak busy | peak back-pressure | checkpoint |
|---|---|---|---|---|---|
| 1 | 58 s | 137,931/s | 100% | 84% | 11 ms |
| 2 | 56 s | 142,857/s | 100% | 75% | 14 ms |
| 4 | 61 s | 131,147/s | 100% | 90% | 9 ms |

Flat. Within noise, and if anything falling. Parallelism does not move the
ceiling on this machine — and this time the generator is not in the room, so the
reason has to be found somewhere else.

## Why parallelism cannot move it here

Two measurements explain it.

**Where the work is.** At parallelism 4, one task chain is pinned while the rest
idle:

```
busy 99%   aggregate_by_account -> positions_by_account: Writer -> Committer
busy 57%   Source: positions_by_account -> parse_by_account
busy 48%   Source: orders -> by_symbol, split_by_allocation
busy 40%   aggregate_by_symbol -> positions_by_symbol: Writer -> Committer
busy  9%   Source: prices -> parse_prices

bp   51%   Source: orders -> by_symbol, split_by_allocation      <- the only one blocked
```

The account chain is the bottleneck, and it is the obvious candidate:
`split_by_allocation` fans every order into four allocations, so that chain
carries 432,000 records/sec against the orders' 108,000. The single
back-pressured task is the orders source, queued behind it. The pipeline is
correctly shaped; one branch simply does four times the work.

**Why more subtasks do not help.** The job graph is eight chained task groups, so
*parallelism 1 is already eight task threads* plus the Kafka producers' own
sender threads. Container CPU while draining, against 800% for eight cores:

| | TaskManager | Kafka broker | total |
|---|---|---|---|
| parallelism 1 | 385% | 98–123% | ~500% |
| parallelism 4 | 475% | 120–200% | ~650% |

Parallelism 1 already consumes nearly four cores of eight. Parallelism 4 asks for
thirty-two task threads on the same eight cores and gets 475% — it cannot get
four times the CPU, because there is not four times the CPU to get. The box is
the constraint at every setting, which is exactly why the drain rate is flat.

This is a real result, not a failed measurement: **on a fixed core count, raising
parallelism alone does not raise throughput.** Parallelism buys nothing without
cores to spend it on.

## What would demonstrate scaling

Scale the cores with the parallelism. That is not a workaround for the laptop; it
is how the managed service actually works — in Amazon Managed Service for Apache
Flink you buy KPUs, each one vCPU, and parallelism and KPUs move together. A
demonstration that raises parallelism while holding cores fixed is not testing
what the deployment model does.

Concretely, pin the TaskManager to a CPU quota proportional to parallelism —
1 core at parallelism 1, 2 at 2, 4 at 4 — and drain the same backlog in each. If
throughput tracks the quota the pipeline scales; if it flattens, the account
chain has a serial constraint worth finding. Either outcome is informative, and
both are measurable on this machine, because each case leaves the rest of the box
free instead of fighting for it.

## Watching for it

The dashboard now carries the saturation signal directly: busiest task and
most-back-pressured task, side by side. Busy near 100% with no back-pressure is a
pipeline working at its limit and keeping up. Sustained back-pressure is a
pipeline that is not — the offered load is above what the current parallelism and
cores can absorb, and lag is growing. That is the panel to alert on, and the one
number a throughput graph will never show you.

## Scripts

- `scripts/scale-test.sh` — the two required cases, live
- `scripts/scale-saturate.sh` — offer more than the job can take; reports the lag
  trend and a saturated / keeps-up verdict
- `scripts/scale-catchup.sh` — drain a fixed backlog at each parallelism with the
  producer stopped, timed to completion
