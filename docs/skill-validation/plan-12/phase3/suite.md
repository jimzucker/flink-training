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

**2→4 cores: 1.93× (97% of linear), range 1.88–1.98× across passes.**

| cores | pass | records/s | tm cores | % of cap | throttled | broker cores | src idle | src BP | headroom | vantage |
|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2 | p1-asc | REFUSED (case) — task manager used 97.4% of its 2-core cap (floor 98%) — it is not the constraint | | | | | | | | |
| 4 | p1-asc | 742,967 | 3.87 | 96.8% | 95% | 0.61 / 2.5 | 10.1% | 7.5% | 113 s | 0.67% |
| 4 | p2-desc | 773,543 | 3.82 | 95.6% | 96% | 0.61 / 2.5 | 10.4% | 8.1% | 110 s | 0.06% |
| 2 | p2-desc | 390,456 | 2.00 | 99.8% | 100% | 0.29 / 2.5 | 15.6% | 13.0% | 357 s | 0.10% |
| 2 | p3-asc | 395,828 | 2.00 | 99.9% | 100% | 0.29 / 2.5 | 15.7% | 13.3% | 355 s | 0.53% |
| 4 | p3-asc | 768,095 | 3.85 | 96.2% | 94% | 0.61 / 2.5 | 10.1% | 7.8% | 107 s | 0.52% |
| 2 | sentinel | 394,752 | 2.00 | 100.2% | 100% | 0.29 / 2.5 | 15.9% | 12.6% | 361 s | 0.15% |

| cores | passes | mean records/s | spread | reportable |
|---:|---:|---:|---:|---|
| 2 | 3 | 393,679 | 1.4% | yes |
| 4 | 3 | 761,535 | 4.0% | yes |

Sentinel: the 2-core case first (390,456 rec/s) and last (394,752 rec/s), drift +1.1% across the suite; counted in that case's spread.
