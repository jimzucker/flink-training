# Headline numbers

Build `55e693b915fbe2a6`, suite saved 2026-10-14 21:32:06 EDT. Generated from `results/suite.json` by `prove.py report`; do not edit by hand.

| cores | mean input records per second | passes |
|---:|---:|---:|
| 1 | 128,653 | 7 |
| 2 | 235,385 | 6 |
| 4 | 438,463 | 2 |

| step | ratio | range it is judged on | target 1.80x |
|---|---:|---|---|
| 1->2 | 1.830x | 1.763x to 1.847x | not met — the low end of its range, 1.763x, is 2.1% under the 1.80x target; more passes could change that |
| 2->4 | 1.863x | 1.784x to 1.980x | met — the low end of its range, 1.784x, is 0.9% under the 1.80x target, within the 1% margin |

The rates are records of the input topic drained per second. Full table: `suite.md`.
