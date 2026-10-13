# Headline numbers

Build `d7e14e7505068416`, suite saved 2026-10-12 23:25:01 EDT. Generated from `results/suite.json` by `prove.py report`; do not edit by hand.

| cores | mean input records per second | passes |
|---:|---:|---:|
| 1 | 108,433 | 4 |
| 2 | 217,213 | 3 |
| 4 | 358,160 | 3 |

| step | ratio | lowest to highest pair | target 1.80x |
|---|---:|---|---|
| 1->2 | 2.003x | 1.786x to 2.179x | met — the low end of its range, 1.919x, is 6.6% over the 1.80x target |
| 2->4 | no number | | the step was thrown out: voided: 4c its readings are 20% apart and the limit is 20% |

The rates are records of the input topic drained per second. Full table: `suite.md`.
