# Headline numbers

Build `16a07060336b9172`, suite saved 2026-10-18 18:22:48 EDT. Generated from `results/suite.json` by `prove.py report`; do not edit by hand.

| cores | mean input records per second | passes |
|---:|---:|---:|
| 1 | 844,045 | 6 |
| 2 | 1,507,136 | 6 |
| 4 | 2,836,615 | 4 |

| step | ratio | range it is judged on | target 1.80x |
|---|---:|---|---|
| 1->2 | 1.786x | 1.759x to 1.849x | not met — measured 1.786x, but it could be as low as 1.759x, 2.3% under the 1.80x target; a step may fall at most 1% under; more passes could change that |
| 2->4 | 1.882x | 1.857x to 2.108x | met — measured 1.882x; at its lowest, 1.857x, it is 3.2% over the 1.80x target |

The rates are records of the input topic drained per second. Full table: `suite.md`.
