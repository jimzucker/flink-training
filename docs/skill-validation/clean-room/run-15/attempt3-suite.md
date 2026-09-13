> **QUICK LOOK — not a result.** one pass per case. No spread, so no table: these numbers say the rig ran clean and roughly how fast, and nothing about how repeatable the ratio is. The record's own passes read 2.04-2.27x where a suite reported 2.15x. Do not publish or quote.

| field | value |
|---|---|
| axis | one worker growing: one task manager container capped at N cores, parallelism N, N slots |
| API level | Flink DataStream API, hand-written operators (no SQL, no Table API) |
| guarantee | state: exactly-once checkpointing; sink: at-least-once, idempotent by emitting the absolute position per key |
| checkpoint interval | 10000 ms |
| build hash | `4d8c12dd55ddb90a` (completeness passed for `4d8c12dd55ddb90a`) |
| passes per case | 1 |
| rate source | committed broker offsets on `block-trades` |
| CPU source | cgroup `cpu.stat usage_usec` |

**1→4 cores: 3.45× (86% of linear) — one pass per case, no spread measured.**

| cores | pass | records/s | tm cores | % of cap | throttled | broker cores | src idle | src BP | headroom | vantage |
|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | p1-asc | REFUSED (case) — task manager used 95.0% of its 1-core cap (floor 95%) — it is not the constraint | | | | | | | | |
| 2 | p1-asc | REFUSED (case) — task manager used 94.7% of its 2-core cap (floor 95%) — it is not the constraint | | | | | | | | |
| 4 | p1-asc | 885,736 | 4.01 | 100.3% | 100% | 0.60 / 2.5 | 4.5% | 42.9% | 300 s | 0.06% |
| 1 | sentinel | 257,016 | 0.99 | 99.5% | 100% | 0.14 / 2.5 | 0.4% | 63.0% | 1416 s | 0.24% |

| cores | passes | mean records/s | spread | reportable |
|---:|---:|---:|---:|---|
| 1 | 1 | 257,016 | 0.0% | yes |
| 4 | 1 | 885,736 | 0.0% | yes |
