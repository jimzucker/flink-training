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
produces the same symbols, sides, quantities and sub-accounts, in the same order.
Only the timestamp differs, which is asserted directly by
`DeterminismTest.wallClockLeavesContentUnchanged`.

## Wall-clock event times are current

```
eventTime = 1787186446417 ms  ->  2026-08-20T00:40:46Z
now                          ->  2026-08-20T00:43:16Z
```

## Key coverage is a function of run length

With 4 accounts x 10 sub-accounts x 4 symbols there are 160 account keys, and
allocations land on them at random. Covering the whole space takes roughly
160 x ln(160) = 812 draws, which is about 203 trades, or 20 seconds at 10/sec.

| Run | Allocations | Account keys seen |
|---|---|---|
| 10s | 400 | 147 of 160 (91%) |
| 45s | 1804 | **160 of 160 (100%)** |

The 10-second figure is not a defect: 147 is what coupon-collector predicts for
400 draws over 160 keys. It does mean a short demo shows the key count climbing
rather than sitting at its final value, which was not true of the original
16-key design.
