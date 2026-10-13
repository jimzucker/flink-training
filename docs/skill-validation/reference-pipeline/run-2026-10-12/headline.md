# Headline numbers

Build `2fc501e9bece7d23`, suite saved 2026-10-12 19:24:56 EDT. Generated from `results/suite.json` by `prove.py report`; do not edit by hand.

| cores | mean input records per second | passes |
|---:|---:|---:|
| 1 | 788,097 | 4 |
| 2 | 1,560,623 | 6 |
| 4 | 2,838,679 | 5 |

| step | ratio | lowest to highest pair | target 1.80x |
|---|---:|---|---|
| 1->2 | 1.980x | 1.819x to 2.115x | met — the low end of its range, 1.892x, is 5.1% over the 1.80x target |
| 2->4 | 1.819x | 1.711x to 1.940x | met — the low end of its range, 1.796x, is 0.2% under the 1.80x target, within the 1% margin |

The rates are records of the input topic drained per second. Full table: `suite.md`.
