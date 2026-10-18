# Headline numbers

Build `da70e36c1e600ecd`, suite saved 2026-10-18 12:47:52 EDT. Generated from `results/suite.json` by `prove.py report`; do not edit by hand.

| cores | mean input records per second | passes |
|---:|---:|---:|
| 1 | 832,922 | 4 |
| 2 | 1,557,917 | 4 |
| 4 | 2,904,055 | 2 |

| step | ratio | range it is judged on | target 1.80x |
|---|---:|---|---|
| 1->2 | 1.870x | 1.795x to 1.906x | met — measured 1.870x; at its lowest, 1.795x, it is 0.3% under the 1.80x target, inside the 1% margin |
| 2->4 | 1.864x | 1.813x to 1.936x | met — measured 1.864x; at its lowest, 1.813x, it is 0.7% over the 1.80x target |

The rates are records of the input topic drained per second. Full table: `suite.md`.
