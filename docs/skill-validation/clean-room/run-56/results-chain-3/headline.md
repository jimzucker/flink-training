# Headline numbers

Build `e35f29848e13cb4f`, suite saved 2026-10-18 15:29:36 EDT. Generated from `results/suite.json` by `prove.py report`; do not edit by hand.

| cores | mean input records per second | passes |
|---:|---:|---:|
| 1 | 120,080 | 4 |
| 2 | 214,065 | 3 |
| 4 | 375,851 | 3 |

| step | ratio | range it is judged on | target 1.80x |
|---|---:|---|---|
| 1->2 | no number | | the step was thrown out: the 2-core case does not count: its readings are 24.4% apart, over the 20% limit |
| 2->4 | no number | | the step was thrown out: the 2-core case does not count: its readings are 24.4% apart, over the 20% limit; the 4-core case does not count: its readings are 28.8% apart, over the 20% limit |

The rates are records of the input topic drained per second. Full table: `suite.md`.
