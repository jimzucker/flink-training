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

*(written after the run)*
