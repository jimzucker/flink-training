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
were competing for the same eight cores *and* the same broker, so the extra
parallelism bought contention rather than capacity. Both sustained rates are far
below the 137,000/sec the pipeline reaches when the producers are not running,
which is the clearest sign that this run measures the machine rather than the
pipeline.

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

The first answer I reached for was cores, and it was wrong. It is worth setting
out how it was excluded, because the wrong answer was the plausible one.

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

The account chain is the bottleneck, and the shape of the job says why:
`split_by_allocation` fans every order into four allocations, so that chain
carries 432,000 records/sec against the orders' 108,000. The pipeline is
correctly shaped; one branch simply does five times the work in total.

**What it is not.** Four candidates, each excluded by measurement:

| Candidate | Test | Result |
|---|---|---|
| TaskManager CPU | container CPU during a drain | 385% at parallelism 1, 475% at 4, of 800% available — **~40% of the box idle while throughput was pinned** |
| A container CPU cap | `cpu.max` in the cgroup | `max`, quota `-1`, 8 processors visible — no cap |
| Kafka reads | `kafka-consumer-perf-test` on the same topic | **1,908,570 records/sec** — fourteen times the pipeline's rate |
| Partition count | rerun the drain with 16 partitions instead of 4 | 137,931/sec against 131,147 — unchanged |
| Checkpointing | rerun the drain at a 10s interval instead of 1s | 115,942 and 126,984/sec — no better |

Low CPU alongside high back-pressure is the tell. A thread blocked waiting on a
Kafka acknowledgement is not busy, and it is not idle for want of work either, so
"the box has spare cores" and "the pipeline cannot go faster" are both true at
once and neither explains the other.

**What it is.** The single broker's write throughput. Four concurrent producers
against it, uncompressed with `acks=all`:

```
aggregate: 750,000 records/sec
```

And the pipeline's write volume is five records per order — one on the symbol
side, four on the account side. At the measured 137,000 orders/sec that is
**685,000 records/sec, or 91% of the broker's aggregate write capacity.**

That is the ceiling, and it explains every observation at once: it is flat across
parallelism because the broker is shared and fixed; more partitions do not help
because the limit is the broker process rather than the partition count; the
TaskManager's cores stay half idle because its threads are blocked on
acknowledgements; and the pinned chain is the account writer because that branch
issues four of the five writes.

Parallelism was never going to move it. **Flink parallelism scales the work
inside the job; it cannot scale a broker outside it.**

## What would demonstrate scaling

Raise the ceiling that is actually binding, then vary parallelism against it.
Since the ceiling is one broker's write throughput, that means more brokers: a
three-broker KRaft cluster in the same compose file roughly triples the write
capacity, and the drain can then be run at parallelism 1, 2 and 4 against a limit
that is no longer the first thing hit. If throughput tracks parallelism the
pipeline scales; if it flattens again, the next constraint is worth finding and
the same exclusion process applies.

Two cheaper observations point the same way and cost nothing to state. Four of
every five records written come from the account branch, so the write ceiling is
overwhelmingly an account-side cost — anything that reduces it (writing
allocations in batches, or not materialising every allocation to Kafka) moves the
ceiling further than any parallelism change. And the deployment model matters:
in Amazon Managed Service for Apache Flink parallelism is bought in KPUs of one
vCPU each, but MSK brokers are provisioned separately, so the same trap exists
there — scaling KPUs against an undersized cluster buys nothing.

The general lesson is the one worth taking to the deck. **Flink parallelism
scales the work inside the job. It cannot scale anything outside it**, and when
the constraint is outside, more parallelism costs threads and returns nothing.

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
