# Step 09 — latency

Two numbers, and they are not the same.

## What the pipeline takes

Published by the operators as `processingLatencyMillis`: how old a trade is
when its position is computed.

```
  p50   59 ms
  p99  110 ms
```

## What a consumer waits for

Measured from outside by `scripts/measure-latency.sh`, which reads the topic
and records how old each record was when it became readable.

```
orders                 n=299    p50=9      p95=14     p99=17     max=22      (ms)
positions-by-symbol    n=300    p50=2488   p95=4781   p99=4981   max=4988    (ms)
positions-by-account   n=1200   p50=2524   p95=4784   p99=4985   max=5024    (ms)
```

The input path is single-digit milliseconds. The positions are not, and the
reason is not the pipeline: under exactly-once a record is not readable until
the checkpoint that produced it commits, so a consumer waits a roughly uniform
interval on top of the processing time.

## The checkpoint interval is the floor

Same code, same rates, one setting changed:

| Checkpoint interval | p50 | max |
|---|---|---|
| 5s | 2488 ms | 4988 ms |
| 1s | **497 ms** | **1041 ms** |

Five times shorter, five times lower, and max lands within one interval each
time. That is what a uniform wait for the next commit looks like, and it is
the strongest evidence that the delay is the guarantee rather than the work.

## After moving to a one-second checkpoint

The interval is now one second by default, on review. Same code, same rates:

```
orders                 p50=8      p95=14     p99=20     max=27      (ms)
positions-by-symbol    p50=518    p95=927    p99=1020   max=1025    (ms)
positions-by-account   p50=515    p95=927    p99=1014   max=1036    (ms)
```

The maximum lands inside one interval, as it did at five seconds. A checkpoint
itself takes about **13ms**, so what costs is the interval between them and not
the checkpoint.

## Market value

Measured from the window close rather than from the trade, because the window is
the specification and not a delay to account for:

```
mv-by-symbol           n=12     p50=684    p95=1709   p99=1709   max=1709    (ms)
mv-by-account          n=48     p50=689    p95=1684   p99=1684   max=1684    (ms)
```

These step rather than spread, and the figures say so: p95, p99 and the maximum
are the same number. Every key in a window is emitted at the same boundary, so
they share an age — there is no tail to speak of, only a handful of distinct
values.

## An aborted checkpoint at startup

The dashboard shows one aborted checkpoint per job. It happens at startup: with a
one-second interval the first checkpoint triggers before every task is running
and is aborted, after which the count stays put while completed checkpoints climb
into the hundreds.

Harmless, but worth naming. The panel originally said any non-zero count meant
the guarantee was being retried, which would have raised a false alarm in the
middle of a demo — the panel now says what the number actually means, and the
runbook has an answer ready.

## The trade being made

A position is a running sum, so a record replayed after a failure is a wrong
number rather than a duplicate. Exactly-once is what prevents that, and this
wait is its price. The interval is a dial: shorter is lower latency and more
frequent checkpoints, and the demo can be run either way.

Market value latency is not reported here. Its delay is the window, which is
the specification rather than a cost.
