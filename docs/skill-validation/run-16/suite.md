> **QUICK LOOK — not a result.** one pass per case. No spread, so no table: these numbers say the rig ran clean and roughly how fast, and nothing about how repeatable the ratio is. The record's own passes read 2.04-2.27x where a suite reported 2.15x. Do not publish or quote.

| field | value |
|---|---|
| axis | one worker growing: one task manager container capped at N cores, parallelism N, N slots |
| API level | Flink DataStream API, hand-written operators (no SQL, no Table API) |
| guarantee | state: exactly-once checkpointing; sink: at-least-once, idempotent by emitting the absolute position per key |
| checkpoint interval | 10000 ms |
| build hash | `65146bb06a53b232` (completeness passed for `65146bb06a53b232`) |
| passes per case | 1 |
| rate source | committed broker offsets on `block-trades` |
| CPU source | cgroup `cpu.stat usage_usec` |

**1→2 cores: 2.06× (103% of linear) — one pass per case, no spread measured.**
**2→4 cores: 1.81× (90% of linear) — one pass per case, no spread measured.**

| cores | pass | records/s | tm cores | % of cap | throttled | broker cores | src idle | src BP | headroom | vantage |
|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | p1-asc | 153,526 | 1.00 | 99.6% | 100% | 0.16 / 2.5 | 0.1% | 37.0% | 1094 s | 0.35% |
| 2 | p1-asc | 322,915 | 1.93 | 96.3% | 100% | 0.28 / 2.5 | 0.7% | 26.8% | 448 s | 0.30% |
| 4 | p1-asc | 584,031 | 3.92 | 97.9% | 95% | 0.57 / 2.5 | 2.8% | 19.7% | 165 s | 0.10% |
| 1 | sentinel | 159,993 | 1.00 | 100.2% | 100% | 0.13 / 2.5 | 0.0% | 33.2% | 1045 s | 0.30% |

| cores | passes | mean records/s | spread | reportable |
|---:|---:|---:|---:|---|
| 1 | 2 | 156,759 | 4.1% | yes |
| 2 | 1 | 322,915 | 0.0% | yes |
| 4 | 1 | 584,031 | 0.0% | yes |

Sentinel: the 1-core case first (153,526 rec/s) and last (159,993 rec/s), drift +4.1% across the suite; counted in that case's spread.
