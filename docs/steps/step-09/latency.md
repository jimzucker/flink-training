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

## The trade being made

A position is a running sum, so a record replayed after a failure is a wrong
number rather than a duplicate. Exactly-once is what prevents that, and this
wait is its price. The interval is a dial: shorter is lower latency and more
frequent checkpoints, and the demo can be run either way.

Market value latency is not reported here. Its delay is the window, which is
the specification rather than a cost.
