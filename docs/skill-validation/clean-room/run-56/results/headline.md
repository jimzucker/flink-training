# Headline numbers

Build `e35f29848e13cb4f`, suite saved 2026-10-18 16:13:27 EDT. Generated from `results/suite.json` by `prove.py report`; do not edit by hand.

| cores | mean input records per second | passes |
|---:|---:|---:|
| 1 | 116,161 | 3 |
| 2 | 200,254 | 3 |
| 4 | 341,760 | 3 |

| step | ratio | range it is judged on | target 1.80x |
|---|---:|---|---|
| 1->2 | no number | | the step was thrown out: the 1-core case does not count: its readings are 21.0% apart, over the 20% limit |
| 2->4 | no number | | the step was thrown out: the 4-core case does not count: its readings are 28.9% apart, over the 20% limit |

The rates are records of the input topic drained per second. Full table: `suite.md`.
