## Step ratios (quick look — not a result)

- **1→2 cores: 2.23×** (111% of linear), range across passes 2.17–2.31×
- **2→4 cores: 1.93×** (96% of linear), range across passes 1.90–1.96×

## Per case

| cores | passes | mean records/s | spread across passes | tm cores used | % of cap | throttled | GC fraction of capacity | broker cores | src idle | src back-pressure | vantage (max) | reportable |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 1 | 3 | 183,813 | 4.4% | 0.97 | 97.2% | 100% | 8.56% | 0.11 / 2.5 | 0.4% | 54.4% | 0.32% | yes |
| 2 | 2 | 409,534 | 1.9% | 2.00 | 100.0% | 100% | 3.69% | 0.25 / 2.5 | 4.0% | 39.7% | 0.42% | yes |
| 4 | 2 | 790,259 | 1.1% | 3.99 | 99.8% | 100% | 1.61% | 0.52 / 2.5 | 5.7% | 28.3% | 0.26% | yes |

## Every pass

| cores | pass | records/s | output records/s | tm cores | % of cap | throttled | GC fraction | broker cores | src idle | src BP | headroom | vantage | status |
|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 1 | p1-asc | 178,674 | 714,697 | 0.97 | 97.3% | 100% | 8.06% | 0.11 | 0.5% | 56.4% | 1535 s | 0.32% | OK |
| 2 | p1-asc | 405,685 | 1,622,741 | 2.00 | 100.0% | 100% | 3.76% | 0.25 | 4.0% | 37.6% | 586 s | 0.42% | OK |
| 4 | p1-asc | 794,487 | 3,177,946 | 3.99 | 99.7% | 100% | 1.88% | 0.53 | 5.6% | 28.7% | 226 s | 0.26% | OK |
| 4 | p2-desc | 786,032 | 3,144,127 | 4.00 | 99.9% | 100% | 1.33% | 0.51 | 5.9% | 28.0% | 231 s | 0.20% | OK |
| 2 | p2-desc | 413,383 | 1,653,533 | 2.00 | 99.9% | 100% | 3.63% | 0.24 | 4.1% | 41.9% | 574 s | 0.35% | OK |
| 1 | p2-desc | 186,806 | 747,225 | 0.99 | 98.6% | 100% | 10.57% | 0.11 | 0.2% | 55.6% | 1467 s | 0.26% | OK |
| 1 | sentinel | 185,958 | 743,830 | 0.96 | 95.7% | 100% | 7.06% | 0.11 | 0.4% | 51.2% | 1474 s | 0.23% | OK |

## Order effect and sentinel

| cores | ascending mean | descending mean | descending / ascending |
|---:|---:|---:|---:|
| 1 | 178,674 | 186,806 | 1.046 |
| 2 | 405,685 | 413,383 | 1.019 |
| 4 | 794,487 | 786,032 | 0.989 |

Sentinel: the 1-core case measured first (178,674 rec/s, pass `p1-asc`) and last (185,958 rec/s), drift +4.0% across the suite; counted inside that case's spread.

## What was refused

No case was refused in the suite. Every case passed every guard: cap read back from the container, parallelism = cap = slots read back from the engine, one graph shape across cases, the worker over the cap floor, the broker never at its memory limit inside a window, at least the minimum commit boundaries in the window, both vantage points in agreement, and backlog headroom left at close.

Guard self-test (every guard broken on purpose, live against this rig): 35/35 fired as expected — PASS.

## Wall clock by phase

| phase | seconds | minutes | result |
|---|---:|---:|---|
| up | 18 | 0.3 | ok |
| preflight | 63 | 1.1 | ok |
| completeness | 456 | 7.6 | ok |
| tinyproof | 504 | 8.4 | ok |
| fill | 383 | 6.4 | ok |
| suite | 1,543 | 25.7 | ok |
| report | 0 | 0.0 | ok |
| **whole chain (`prove.py all --quick`)** | **2,968** | **49.5** | **PASS** |

