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

## Removing the tolerance entirely

An earlier version of the verification checked counts with a 5% tolerance. The
tolerance was a symptom, not a requirement: it existed only because the run was
bounded by elapsed time while emission is paced, so a stop landing between pacer
ticks yielded 100 or 101 records, and a run ending mid-cycle left one symbol a
price ahead.

Three causes, all removed rather than tolerated:

| Slop | Cause | Fix |
|---|---|---|
| count 100 or 101 | stop falls between pacer ticks | bound the run by record count |
| one symbol one price ahead | run ends mid round-robin cycle | make the price count a multiple of the symbol count |
| per-symbol counts look uneven | `--max-messages` slices across partitions | drain the whole topic |

A fourth surfaced while testing the fix: **topic deletion in Kafka is
asynchronous**, so recreating too quickly leaves the previous run's records in
place. That turned an exact count check into a race, which is worse than a
tolerance because it fails for the wrong reason. `verify-run.sh` now waits for
deletion to complete and aborts rather than verifying stale data.

Every check is now exact, with no tolerances anywhere:

```
orders   record count 100 · symbols 4 · allocations 400 · account keys 16
         malformed keys 0 · sides 2 · duplicate ids 0 · missing ids 0
prices   record count 400 · symbols 4 · prices per symbol 100
         non-positive 0 · off-quarter 0
all checks passed, with no tolerances
```

`missing tradeIds` is worth noting: it checks the full expected id set
`T000000000`–`T000000099` is present, which a count alone would not catch if a
record were both dropped and duplicated.
