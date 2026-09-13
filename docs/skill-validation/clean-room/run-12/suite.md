| field | value |
|---|---|
| axis | one worker growing: one task manager container capped at N cores, parallelism N, N slots |
| API level | Flink DataStream API, hand-written operators (no SQL, no Table API) |
| guarantee | state: exactly-once checkpointing; sink: at-least-once, idempotent by emitting the absolute position per key |
| checkpoint interval | 10000 ms |
| build hash | `d14782d8399359b0` (completeness passed for `d14782d8399359b0`) |
| passes per case | 3 |
| rate source | committed broker offsets on `block-trades` |
| CPU source | cgroup `cpu.stat usage_usec` |

**1→2 cores: 2.01× (101% of linear), range 1.94–2.08× across passes.**
**2→4 cores: 2.15× (108% of linear), range 2.04–2.27× across passes.**

| cores | pass | records/s | tm cores | % of cap | throttled | broker cores | src idle | src BP | headroom | vantage |
|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | p1-asc | 74,902 | 1.00 | 99.9% | 100% | 0.14 / 3 | 0.1% | 22.7% | 1599 s | 0.11% |
| 2 | p1-asc | 152,543 | 2.00 | 100.3% | 100% | 0.31 / 3 | 6.1% | 17.2% | 697 s | 0.28% |
| 4 | p1-asc | 311,070 | 4.01 | 100.2% | 100% | 0.78 / 3 | 1.9% | 9.7% | 266 s | 0.25% |
| 4 | p2-desc | 322,823 | 4.01 | 100.2% | 100% | 0.75 / 3 | 2.1% | 12.1% | 222 s | 0.48% |
| 2 | p2-desc | 149,812 | 2.00 | 99.9% | 100% | 0.30 / 3 | 5.5% | 14.8% | 721 s | 0.09% |
| 1 | p2-desc | 73,349 | 0.99 | 99.4% | 100% | 0.14 / 3 | 0.1% | 23.4% | 1630 s | 0.51% |
| 1 | p3-asc | REFUSED (case) — task manager used 95.9% of its 1-core cap (floor 98%) — it is not the constraint | | | | | | | | |
| 2 | p3-asc | 145,325 | 2.00 | 99.8% | 100% | 0.30 / 3 | 4.4% | 14.6% | 748 s | 0.26% |
| 4 | p3-asc | 330,331 | 4.01 | 100.3% | 99% | 0.75 / 3 | 2.6% | 9.1% | 235 s | 0.28% |
| 1 | sentinel | REFUSED (case) — task manager used 97.4% of its 1-core cap (floor 98%) — it is not the constraint | | | | | | | | |

| cores | passes | mean records/s | spread | reportable |
|---:|---:|---:|---:|---|
| 1 | 2 | 74,126 | 2.1% | yes |
| 2 | 3 | 149,227 | 4.8% | yes |
| 4 | 3 | 321,408 | 6.0% | yes |
