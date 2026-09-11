# Clean-room validation, run 29 — the demo's input, exactly

Runs 27 and 28 built the demo's *job* but chose their own input: 160M and 300M
records, their own key cardinality, their own backlog sizing. This run is given
the demo's input verbatim, so the comparison is like for like.

## What the demo uses

| | |
|---|---|
| backlog | **50,000,000** block trades, filled with the job stopped, then drained |
| prices | backlog ÷ 100 = **500,000** |
| symbols | **4** — AAPL, MSFT, GOOG, AMZN, opening prices 100 / 200 / 300 / 400 |
| accounts | **4** — ACC1..ACC4, one sub-account each, so 16 account keys |
| allocation | every block split across all four accounts |
| partitions | 8 |
| checkpoint interval | 5,000 ms |
| window | 10 s tumbling, market value at close |

Its result on this machine today: 35,399 / 80,974 / 168,200 rec/s at 1 / 2 / 4
units — 2.29× and 2.08×, one pass per unit, no spread.

## The criteria, written before launch

| criterion | note |
|---|---|
| the input matches the demo: 50M trades, 500k prices, 4 symbols, 4 accounts, 8 partitions, 5 s checkpoints | the point of this run |
| both halves built, positions and windowed market value, proved exactly under a killed worker | as runs 27 and 28 |
| chain passes on the first attempt | run 27 did, run 28 needed three |
| every case at 95–101% of cap, GC under 5.5% | the corrected ceiling |
| 2→4 with its interval, against the demo's 2.08× and runs 27/28's ~1.88 | the comparison |
| rate per core against the demo's 35,399 / 40,487 / 42,050 | four keys is a much smaller state than these runs have used |

**Watch for**: four symbols across eight partitions and four subtasks is heavy
key skew — one key per subtask at four cores, and the demo lives with it. That
may be the whole difference between the demo's 2.08× and these runs' 1.88×.

## Response

**The harness refused the demo's input, and the refusal is the result.**

```
REFUSED (rig): backlog 50,000,000 is short of the 72,817,104 records the 4-core
case needs at its measured 746,842 rec/s (warm-up + window + headroom, x1.5);
set backlog.count to at least that
```

At the four-core rate the 50M backlog is **67 seconds of drain**, and one
measured case needs a ≥90 s warm-up to a flat trend plus a ≥60 s window plus
headroom. Nothing but the backlog can fix that, the backlog was fixed by the
prompt, and the agent stopped rather than substituting a size — which is what
it was asked to do.

This is an instrument difference, not a pipeline one: the demo drains the same
50M inside a 60 s window with no warm-up rule and no headroom requirement.

## What it measured before stopping

| cores | rec/s | % of cap | µs CPU per record | GC |
|---:|---:|---:|---:|---:|
| 1 | 181,115 | 100.4% | 5.54 | 3.8% |
| 4 | 746,842 | 98.7% | 5.29 | 0.8% |

1→4 = 4.124× from the tiny proof's single passes. The 2-core case never ran.

## Correctness passed, both arms

No tolerances, clean drain and worker killed at 733,463 of 1,500,000: the exact
set of (key, window-end) pairs — 149 windows × 4 symbol keys and × 16 account
keys — and every field of *every copy* of every record (position, price in
cents, market value in cents) equal to the generator manifest. Zero duplicates
clean; 44 and 176 after the kill, each carrying the identical exact triple.

## Key skew at four cores: none, because the agent removed it

Per-subtask rates agreed to four significant figures (117,439 / 117,432 /
117,436 / 117,435 on symbol positions). That is a chosen property: with Flink's
default maxParallelism of 128 the four symbols land 1/1/1/1 but the sixteen
account keys land **2/4/7/3**, so the job fixes maxParallelism at 1616 — the
smallest value that divides both key sets evenly across 1, 2 and 4. A pipeline
that did not do this would have measured skew, not scaling.

## Four defects found before any chain

Event-time Kafka timestamps let retention delete the backlog mid-run; windows
read the current position instead of the position at the boundary; 8 MB fetches
left the source 1.3M records ahead of its own watermark; and a restored split
parked at its end offset pins the watermark at `MIN_VALUE` after a kill — the
same class runs 27 and 28 found independently.

## Cost

2 h 53 m, of which the accepted chain is 17.0 min; 37 min in two re-run chain
attempts and 1 h 20 m of correctness work before any chain started.

