| field | value |
|---|---|
| axis | one worker growing: one task manager container capped at N cores, parallelism N, N slots |
| API level | Flink DataStream API, hand-written operators (no SQL, no Table API) |
| guarantee | state: exactly-once checkpointing; sink: at-least-once, idempotent by emitting the absolute position per key |
| checkpoint interval | 10000 ms |
| build hash | `57be5902a9228120` (completeness passed for `57be5902a9228120`) |
| passes per case | 3 |
| rate source | committed broker offsets on `block-trades` |
| CPU source | cgroup `cpu.stat usage_usec` |

**2→4 cores: 2.02× (101% of linear), range 1.98–2.07× across passes.**

| cores | pass | records/s | tm cores | % of cap | throttled | broker cores | src idle | src BP | headroom | vantage |
|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2 | p1-asc | 213,073 | 2.01 | 100.4% | 100% | 0.32 / 2.5 | 0.0% | 25.9% | 601 s | 0.32% |
| 4 | p1-asc | 438,039 | 3.99 | 99.6% | 100% | 0.61 / 2.5 | 0.1% | 22.6% | 211 s | 0.32% |
| 4 | p2-desc | 437,344 | 3.99 | 99.7% | 100% | 0.61 / 2.5 | 0.0% | 21.4% | 215 s | 0.21% |
| 2 | p2-desc | 221,023 | 1.99 | 99.7% | 100% | 0.32 / 2.5 | 0.0% | 26.6% | 580 s | 0.24% |
| 2 | p3-asc | 217,746 | 2.00 | 100.2% | 100% | 0.31 / 2.5 | 0.0% | 23.8% | 575 s | 0.35% |
| 4 | p3-asc | 440,984 | 3.83 | 95.7% | 100% | 0.61 / 2.5 | 0.1% | 18.4% | 210 s | 0.43% |

| cores | passes | mean records/s | spread | reportable |
|---:|---:|---:|---:|---|
| 2 | 3 | 217,281 | 3.7% | yes |
| 4 | 3 | 438,789 | 0.8% | yes |
