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

**2→4 cores: 1.87× (93% of linear), range 1.75–2.06× across passes.**

| cores | pass | records/s | tm cores | % of cap | throttled | broker cores | src idle | src BP | headroom | vantage |
|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2 | p1-asc | REFUSED (case) — task manager used 96.4% of its 2-core cap (floor 98%) — it is not the constraint | | | | | | | | |
| 4 | p1-asc | 687,827 | 3.87 | 96.7% | 93% | 0.53 / 2.5 | 9.2% | 10.6% | 126 s | 0.35% |
| 4 | p2-desc | 693,104 | 3.93 | 98.2% | 97% | 0.51 / 2.5 | 8.6% | 9.6% | 135 s | 0.11% |
| 2 | p2-desc | 376,886 | 2.00 | 100.1% | 100% | 0.30 / 2.5 | 11.5% | 15.1% | 386 s | 0.22% |
| 2 | p3-asc | 392,293 | 1.99 | 99.7% | 100% | 0.30 / 2.5 | 12.1% | 14.0% | 368 s | 0.10% |
| 4 | p3-asc | 775,611 | 3.93 | 98.2% | 97% | 0.60 / 2.5 | 9.9% | 8.5% | 111 s | 0.15% |

| cores | passes | mean records/s | spread | reportable |
|---:|---:|---:|---:|---|
| 2 | 2 | 384,590 | 4.0% | yes |
| 4 | 3 | 718,847 | 12.2% | yes |
