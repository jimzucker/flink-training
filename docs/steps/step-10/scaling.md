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

## Trying to demonstrate scaling anyway

Every dial available on one machine, measured. An 8,000,000-order backlog drained
with the producer stopped, except the paired cases, which use 4,000,000:

| Configuration | orders/sec |
|---|---|
| 1 broker, 4 partitions, parallelism 1 / 2 / 4 | 137,931 / 142,857 / 131,147 |
| 1 broker, 12 partitions, parallelism 1 / 2 / 4 | 142,857 / 166,666 / 153,846 |
| **3 brokers**, 12 partitions, parallelism 1 / 2 / 4 | **72,072 / 74,074 / 76,190** |
| 1 broker, tuned sinks, parallelism 1 / 2 / 4 | 153,846 / 166,666 / 173,913 |
| partitions + parallelism paired, 1 / 2 / 4 | 148,148 / 190,476 / 173,913 |
| the same, repeated | 148,148 / 173,913 / 190,476 |

Read down the last two rows first: repeating the paired run swaps 2 and 4, so
**the difference between parallelism 2 and 4 is inside the noise.** Only 1 → 2 is
a real gain, and it is about 25%, not 100%.

Read across and the reason is plain: every configuration lands between 130,000
and 190,000 orders/sec. Partitions from 1 to 12, parallelism from 1 to 4, a core
budget from 1 to 8, one broker or three — the answer barely moves. **A single
partition at parallelism 1 sustains 148,000 orders/sec**, which is
765,000 records/sec written, against the 750,000 the broker was measured to
accept. Everything saturates the same thing.

### Resolved on a backlog larger than memory

Every drain above used 4 to 12 million records, which fits in the VM's RAM. The
fill leaves the topic in page cache and the drain then reads from memory, so what
those runs measured was partly the cache. Repeated against **88,678,174 orders**,
far larger than RAM, with nothing varying but parallelism:

| same 88.7M-order backlog, 4 partitions | p=2 | p=4 |
|---|---|---|
| time to drain | 726 s | **623 s** |
| orders/sec | 122,146 | **142,340** |
| allocations/sec | 488,584 | **569,360** |
| records written/sec | 610,730 | **711,700** |
| back-pressure | 20–21% | 42–43% |

**Parallelism 4 is 16.5% faster than parallelism 2**, over ten minutes, on
identical data.

This supersedes the earlier reading from the 12-million-record runs, which had
2 → 4 as a 9% *regression*. Those drains lasted about a minute against a topic
the machine could hold entirely in cache; at 88 million the reads come off disk,
and more subtasks means more concurrent read streams.

The back-pressure column is the tell. **p=2 sat at 20% and p=4 at 42%**: two
subtasks were not pressing the broker hard enough to back-pressure themselves, so
the limit was their own capacity rather than the broker's. Four pushed until the
sinks pushed back, and moved 16.5% more in the process.

So the curve flattens rather than reversing, which is what approaching a fixed
external ceiling looks like: p=4's 711,700 records/sec is about 95% of the
750,000 the broker was measured to accept.

One caveat kept deliberately. The **1 → 2 figure of +23% is still from the small,
cache-warm runs** and is not comparable to the two above. Completing the curve
means draining this same 88-million backlog at parallelism 1, which has not been
done.

### What a live generator can and cannot show

Two three-minute runs with the job started first and the generator started after,
which is the arrangement a demo actually uses:

| | offered | Flink consumed | lag after the run |
|---|---|---|---|
| 1 thread, uncompressed | 85,132/s | 85,132/s | **0 — kept up** |
| 4 threads, lz4 | 224,501/s | **13,787/s** | **83,231,947 and growing** |

Neither measures Flink. In the first the generator was throttled by contention to
well under what Flink can take, so Flink absorbed everything with headroom to
spare. In the second the generator's four threads took the cores Flink needed,
and Flink ran at 13,787/sec against the 142,340 it manages with the machine to
itself -- starved, not outrun. Latency reached 374 seconds and back-pressure
pegged at 100%.

**On one machine a live producer either cannot outrun Flink or starves it, with
nothing in between.** That is the same conclusion the drains reach, from the
other direction, and it is why the backlog method is the one to trust.

The saturated run is still worth keeping as a demo exhibit -- lag climbing,
back-pressure red, latency in minutes is exactly what saturation looks like -- as
long as it is narrated as offered load exceeding what the machine can do, rather
than as Flink's ceiling.

### More brokers made it worse

Three brokers were the obvious response to a broker-bound pipeline, and they cut
throughput roughly in half: 137,931 orders/sec became 72,072, and the four-
producer write ceiling fell from 750,000 records/sec to 545,000.

The cause is the machine, not Kafka. Three broker JVMs held **2.5GB of a 7.65GB
VM** where one held 1GB, and Kafka writes are only fast while the page cache that
leaves behind is large enough. Sharing one disk between three logs does the rest.
**More brokers add capacity when they add machines; on one machine they divide
it.** Kept as `docker/compose.cluster.yml` so the measurement can be repeated,
not as a default.

### What tuning did buy

Sink producers were on Kafka's defaults: no compression, and `linger.ms=0`, which
sends a batch as soon as one record is ready. Since the TaskManager had cores to
spare and the broker did not have writes to spare, spending the former on the
latter is the trade the system was asking for. With lz4, `linger.ms=10` and 128KB
batches, parallelism 4 went from 153,846 to 173,913 orders/sec, and
back-pressure at parallelism 4 fell from 85% to 66%. It also turned the curve the
right way up: before tuning, parallelism 4 was *slower* than 2.

That is worth having, and it is not scaling.

## What would demonstrate scaling

Stop sharing one machine. Everything above is a variation on dividing eight cores
and one disk between a load generator, a broker and a Flink cluster, and the
result is the same wall each time because it is the same machine each time.

The AWS step is where this belongs, and now for a specific reason rather than a
vague preference for "dedicated resources": MSK brokers and Flink KPUs are
separate machines there, so raising KPUs raises Flink's capacity without taking
anything from Kafka's. The measurement to run is the same drain, and the
prediction is now falsifiable: throughput should track KPUs until the MSK cluster
becomes the ceiling, at which point adding KPUs will stop helping exactly as it
does here.

The cheaper lever, wherever it runs, is the write amplification. Four of every
five records written come from the account branch, so anything that reduces that
volume moves the ceiling further than any parallelism setting will.

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
