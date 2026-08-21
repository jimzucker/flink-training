# Step 02 evidence

## Two clock modes

Wall clock is the default, because latency has to be measurable from the instant
a record is created to the instant it reaches a sink. Replay mode, selected by
setting `START_EPOCH_MILLIS`, takes event times from a counter instead so two
runs are byte-identical.

| Mode | Event time | Reproducible | Used for |
|---|---|---|---|
| live (default) | wall clock | trade content only | the demo, and every latency measurement |
| replay (`START_EPOCH_MILLIS=...`) | counter from a fixed origin | byte-identical | proving reproducibility |

The seed fixes the *content* of the sequence in both modes: the same seed always
produces the same symbols, sides, quantities and allocations, in the same order.
Only the timestamp differs, asserted directly by
`DeterminismTest.wallClockLeavesContentUnchanged`.

## Wall-clock event times are current

```
eventTime = 1787186446417 ms  ->  2026-08-20T00:40:46Z
now                          ->  2026-08-20T00:43:16Z
```

## Reproducibility, measured through a real broker

A run has to be bounded by **record count**, not elapsed time. "Run for six
seconds" lands on 60 or 61 trades depending on where the clock falls, and two
such runs differ by that one record even though every record they share is
identical. `MAX_TRADES` and `MAX_PRICES` make the boundary exact.

Two separate runs, published to Kafka, consumed back and sorted:

```
orders  run 1  n=100  241903ac4ea9b746407516075275b694c42bb3a372c107df48ecd8b69d83fdae
orders  run 2  n=100  241903ac4ea9b746407516075275b694c42bb3a372c107df48ecd8b69d83fdae
prices  run 1  n=400  39dfc9c8097726746896a908046efa2e3030d0995579eefcfbe3d87b2efff109
prices  run 2  n=400  39dfc9c8097726746896a908046efa2e3030d0995579eefcfbe3d87b2efff109
```

The orders hash is the same value `DeterminismTest` pins in-process, so the
sequence the unit test guards and the bytes that reach the broker are provably
the same thing.

## Rates are approximate; invariants are not

A run ends mid-cycle, so counts land within a record or two of nominal and one
symbol can be one price ahead of the others. Those are properties of stopping a
paced stream, not defects. The verification script checks rates with a tolerance
and the invariants exactly:

| Checked exactly | Checked with tolerance |
|---|---|
| `allocations = 4 x orders` | record count vs nominal rate |
| allocations sum to block quantity | round-robin spread (<= 1) |
| 4 symbol keys, 16 account keys | |
| no duplicate trade ids, both sides present | |
| prices positive and on exact quarters | |
