| field | value |
|---|---|
| axis | one worker growing: one task manager container capped at N cores, parallelism N, N slots |
| API level | Flink DataStream API, hand-written operators (no SQL, no Table API) |
| guarantee | state: exactly-once checkpointing; sink: at-least-once, idempotent by emitting the absolute position per key |
| checkpoint interval | 10000 ms |
| build hash | `6ee9600b17e9ba41` (completeness passed for `6ee9600b17e9ba41`) |
| passes per case | 3 |
| rate source | committed broker offsets on `block-trades` |
| CPU source | cgroup `cpu.stat usage_usec` |

**1→2 cores: 1.99× (100% of linear), range 1.75–2.29× across passes.**
**2→4 cores: 2.00× (100% of linear), range 1.81–2.23× across passes.**

| cores | pass | records/s | tm cores | % of cap | throttled | broker cores | src idle | src BP | headroom | vantage |
|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | p1-asc | 183,365 | 1.00 | 99.9% | 100% | 0.13 / 2.5 | 0.3% | 19.6% | 933 s | 0.01% |
| 2 | p1-asc | 327,499 | 2.01 | 100.4% | 100% | 0.41 / 2.5 | 10.7% | 13.7% | 349 s | 0.00% |
| 4 | p1-asc | 705,850 | 3.99 | 99.7% | 99% | 0.63 / 2.5 | 8.4% | 10.0% | 137 s | 0.21% |
| 4 | p2-desc | 731,807 | 3.90 | 97.4% | 94% | 0.68 / 2.5 | 9.3% | 8.3% | 121 s | 0.41% |
| 2 | p2-desc | 357,660 | 1.99 | 99.4% | 100% | 0.33 / 2.5 | 12.8% | 13.1% | 396 s | 0.40% |
| 1 | p2-desc | 179,771 | 1.00 | 99.9% | 100% | 0.15 / 2.5 | 0.2% | 21.8% | 970 s | 0.32% |
| 1 | p3-asc | 186,783 | 0.97 | 97.2% | 100% | 0.12 / 2.5 | 0.2% | 23.9% | 927 s | 0.50% |
| 2 | p3-asc | 390,769 | 2.00 | 100.1% | 100% | 0.32 / 2.5 | 16.5% | 12.2% | 367 s | 0.26% |
| 4 | p3-asc | REFUSED (case) — task manager used 92.8% of its 4-core cap (floor 95%) — it is not the constraint | | | | | | | | |
| 1 | sentinel | 170,982 | 1.00 | 99.8% | 100% | 0.15 / 2.5 | 0.2% | 24.1% | 1018 s | 0.43% |

| cores | passes | mean records/s | spread | reportable |
|---:|---:|---:|---:|---|
| 1 | 4 | 180,225 | 8.8% | yes |
| 2 | 3 | 358,642 | 17.6% | yes |
| 4 | 2 | 718,829 | 3.6% | yes |

Sentinel: the 1-core case first (183,365 rec/s) and last (170,982 rec/s), drift -7.0% across the suite; counted in that case's spread.
