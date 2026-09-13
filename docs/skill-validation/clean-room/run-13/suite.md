| field | value |
|---|---|
| axis | one worker growing: one task manager container capped at N cores, parallelism N, N slots |
| API level | Flink DataStream API, hand-written operators (no SQL, no Table API) |
| guarantee | state: exactly-once checkpointing; sink: at-least-once, idempotent by emitting the absolute position per key |
| checkpoint interval | 10000 ms |
| build hash | `6381961fd22c52ef` (completeness passed for `6381961fd22c52ef`) |
| passes per case | 3 |
| rate source | committed broker offsets on `block-trades` |
| CPU source | cgroup `cpu.stat usage_usec` |

**1→2 cores: 2.17× (109% of linear), range 2.02–2.33× across passes.**
**2→4 cores: 1.85× (92% of linear), range 1.75–1.97× across passes.**

| cores | pass | records/s | tm cores | % of cap | throttled | broker cores | src idle | src BP | headroom | vantage |
|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | p1-asc | 154,079 | 1.00 | 100.4% | 100% | 0.09 / 2.5 | 0.0% | 40.0% | 1342 s | 0.40% |
| 2 | p1-asc | 322,907 | 1.99 | 99.6% | 100% | 0.20 / 2.5 | 1.5% | 30.1% | 567 s | 0.31% |
| 4 | p1-asc | 593,313 | 3.80 | 95.1% | 100% | 0.32 / 2.5 | 5.3% | 16.6% | 248 s | 0.31% |
| 4 | p2-desc | 611,800 | 3.83 | 95.8% | 100% | 0.34 / 2.5 | 5.4% | 17.0% | 232 s | 0.35% |
| 2 | p2-desc | 329,033 | 2.00 | 100.0% | 100% | 0.19 / 2.5 | 1.3% | 33.2% | 532 s | 0.16% |
| 1 | p2-desc | 141,040 | 1.00 | 100.1% | 100% | 0.09 / 2.5 | 0.1% | 38.0% | 1486 s | 0.32% |
| 1 | p3-asc | 145,724 | 1.00 | 99.7% | 100% | 0.11 / 2.5 | 0.0% | 38.7% | 1392 s | 0.29% |
| 2 | p3-asc | 310,496 | 2.00 | 100.0% | 100% | 0.21 / 2.5 | 1.1% | 29.6% | 592 s | 0.02% |
| 4 | p3-asc | 575,479 | 4.00 | 99.9% | 100% | 0.40 / 2.5 | 5.6% | 16.8% | 246 s | 0.19% |
| 1 | sentinel | 150,336 | 1.00 | 100.3% | 100% | 0.09 / 2.5 | 0.0% | 38.5% | 1386 s | 0.29% |

| cores | passes | mean records/s | spread | reportable |
|---:|---:|---:|---:|---|
| 1 | 4 | 147,795 | 8.8% | yes |
| 2 | 3 | 320,812 | 5.8% | yes |
| 4 | 3 | 593,531 | 6.1% | yes |

Sentinel: the 1-core case first (154,079 rec/s) and last (150,336 rec/s), drift -2.5% across the suite; counted in that case's spread.
