# Step 03 evidence — CI run 32436156688

Branch: step-03-ci
Title: Step 03: CI
Conclusion: success
URL: https://github.com/jimzucker/flink-training/actions/runs/32436156688

## Jobs

- SUCCESS  Shell scripts
- SUCCESS  Build and test
- SUCCESS  Verify the expected numbers

## Verify-the-numbers job output

```
Verify the expected numbers	Run and verify	2026-08-21T01:25:29.7451730Z emitting exactly 100 trades and 400 prices (seed 42, replay)
Verify the expected numbers	Run and verify	2026-08-21T01:25:40.7735772Z 01:25:30.480 INFO  GeneratorMain - starting generators: bootstrap=localhost:9092 trades/sec=10 prices/sec=1000 seed=42 duration=forever time=replay from 1700000000000
Verify the expected numbers	Run and verify	2026-08-21T01:25:40.7737336Z 01:25:30.483 INFO  GeneratorMain - universe: 4 symbols, 4 accounts, 4 allocations per trade -> 4 symbol keys, 16 account keys
Verify the expected numbers	Run and verify	2026-08-21T01:25:40.7763182Z orders  (expecting exactly 100)
Verify the expected numbers	Run and verify	2026-08-21T01:26:02.7568674Z   record count                             100          OK
Verify the expected numbers	Run and verify	2026-08-21T01:26:02.7612546Z   unique symbols (sink 3 keys)             4            OK
Verify the expected numbers	Run and verify	2026-08-21T01:26:02.7771221Z   allocations (sink 4 rate)                400          OK
Verify the expected numbers	Run and verify	2026-08-21T01:26:02.7816170Z   allocations sum to block qty             0            OK
Verify the expected numbers	Run and verify	2026-08-21T01:26:02.7864930Z   unique account keys (sink 4)             16           OK
Verify the expected numbers	Run and verify	2026-08-21T01:26:02.7906924Z   malformed account keys                   0            OK
Verify the expected numbers	Run and verify	2026-08-21T01:26:02.7949566Z   sides present                            2            OK
Verify the expected numbers	Run and verify	2026-08-21T01:26:02.7997630Z   duplicate tradeIds                       0            OK
Verify the expected numbers	Run and verify	2026-08-21T01:26:02.8964445Z   missing tradeIds                         0            OK
Verify the expected numbers	Run and verify	2026-08-21T01:26:02.9008320Z   buy/sell mix                                  43 BUY      57 SELL 
Verify the expected numbers	Run and verify	2026-08-21T01:26:02.9008886Z prices  (expecting exactly 400)
Verify the expected numbers	Run and verify	2026-08-21T01:26:24.5707918Z   record count                             400          OK
Verify the expected numbers	Run and verify	2026-08-21T01:26:24.5751389Z   unique symbols                           4            OK
Verify the expected numbers	Run and verify	2026-08-21T01:26:24.5809601Z   prices per symbol                        100          OK
Verify the expected numbers	Run and verify	2026-08-21T01:26:24.5847754Z   non-positive prices                      0            OK
Verify the expected numbers	Run and verify	2026-08-21T01:26:24.5888160Z   prices off a quarter                     0            OK
Verify the expected numbers	Run and verify	2026-08-21T01:26:24.5888822Z all checks passed, with no tolerances
```
