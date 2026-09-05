> **QUICK LOOK — not a result.** one pass per case. No spread, so no table: these numbers say the rig ran clean and roughly how fast, and nothing about how repeatable the ratio is. The record's own passes read 2.04-2.27x where a suite reported 2.15x. Do not publish or quote.

| field | value |
|---|---|
| axis | one worker growing: one task manager container capped at N cores, parallelism N, N slots |
| API level | Flink DataStream API, hand-written operators (no SQL, no Table API) |
| guarantee | state: exactly-once checkpointing; sink: at-least-once, idempotent by emitting the absolute position per key |
| checkpoint interval | 10000 ms |
| build hash | `01cf956ac64d455e` (completeness passed for `01cf956ac64d455e`) |
| passes per case | 1 |
| rate source | committed broker offsets on `block-trades` |
| CPU source | cgroup `cpu.stat usage_usec` |

**1→2 cores: 2.11× (106% of linear) — one pass per case, no spread measured.**
**2→4 cores: 1.35× (68% of linear) — one pass per case, no spread measured.**

| cores | pass | records/s | tm cores | % of cap | throttled | broker cores | src idle | src BP | headroom | vantage |
|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | p1-asc | 186,468 | 1.00 | 99.9% | 100% | 0.06 / 3 | 0.1% | 32.0% | 1268 s | 0.21% |
| 2 | p1-asc | 398,686 | 1.97 | 98.4% | 100% | 0.13 / 3 | 0.2% | 37.7% | 516 s | 0.18% |
| 4 | p1-asc | 539,902 | 3.83 | 95.9% | 87% | 0.25 / 3 | 2.6% | 26.8% | 326 s | 0.04% |
| 1 | sentinel | 190,876 | 0.96 | 96.3% | 100% | 0.06 / 3 | 0.0% | 30.4% | 1243 s | 0.16% |

| cores | passes | mean records/s | spread | reportable |
|---:|---:|---:|---:|---|
| 1 | 2 | 188,672 | 2.3% | yes |
| 2 | 1 | 398,686 | 0.0% | yes |
| 4 | 1 | 539,902 | 0.0% | yes |

Sentinel: the 1-core case first (186,468 rec/s) and last (190,876 rec/s), drift +2.3% across the suite; counted in that case's spread.
