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

**1→2 cores: 2.14× (107% of linear) — one pass per case, no spread measured.**
**2→4 cores: 1.84× (92% of linear) — one pass per case, no spread measured.**

| cores | pass | records/s | tm cores | % of cap | throttled | broker cores | src idle | src BP | headroom | vantage |
|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | p1-asc | 249,782 | 1.00 | 100.2% | 100% | 0.15 / 2.5 | 0.6% | 65.1% | 1450 s | 0.46% |
| 2 | p1-asc | 525,655 | 2.01 | 100.4% | 100% | 0.30 / 2.5 | 2.1% | 60.3% | 612 s | 0.28% |
| 4 | p1-asc | 964,913 | 3.91 | 97.7% | 93% | 0.66 / 2.5 | 5.9% | 35.4% | 250 s | 0.50% |
| 1 | sentinel | 242,087 | 0.99 | 98.7% | 100% | 0.13 / 2.5 | 0.5% | 57.7% | 1510 s | 0.26% |

| cores | passes | mean records/s | spread | reportable |
|---:|---:|---:|---:|---|
| 1 | 2 | 245,935 | 3.1% | yes |
| 2 | 1 | 525,655 | 0.0% | yes |
| 4 | 1 | 964,913 | 0.0% | yes |

Sentinel: the 1-core case first (249,782 rec/s) and last (242,087 rec/s), drift -3.1% across the suite; counted in that case's spread.
