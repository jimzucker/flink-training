> **QUICK LOOK — not a result.** two passes per case, not the configured number: enough for a spread, not enough to publish. A one-pass version of this table read 2->4 = 1.645 where the same build measured 1.910 with the memory it needed, and 1.539 where three passes read 1.678. Quote the spread with the ratio, or do not quote it.

| field | value |
|---|---|
| axis | one worker growing: one task manager container capped at N cores, parallelism N, N slots |
| API level | Flink DataStream API, this repository's own PositionsJob (no SQL, no Table API) |
| guarantee | state: exactly-once checkpointing; sink: at-least-once, idempotent by emitting the absolute position per key |
| checkpoint interval | 5000 ms |
| build hash | `d465a077bb70b189` (completeness passed for `d465a077bb70b189`) |
| passes per case | 2 |
| study | scaling: every case configured identically |
| workload | 150,000,000 records, symbolCount 4,096, accountCount 4, distinctSymbolKeys 4,096, distinctAccountKeys 16,384, symbolUpdates 150,000,000, accountUpdates 600,000,000, 5 outputs per input |
| rate source | committed broker offsets on `block-trades` |
| CPU source | cgroup `cpu.stat usage_usec` |

**1→2 cores: 2.06× (103% of linear) — one pass per case, no spread measured.**
**2→4 cores: 1.99× (99% of linear) — one pass per case, no spread measured.**

| cores | pass | records/s | tm cores | % of cap | throttled | broker cores | src idle | src BP | headroom | vantage |
|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | p1-asc | 60,720 | 1.00 | 99.7% | 100% | 0.09 / 2.5 | 0.0% | 39.6% | 2324 s | 0.34% |
| 2 | p1-asc | 120,201 | 1.95 | 97.3% | 100% | 0.16 / 2.5 | 0.0% | 37.6% | 1111 s | 0.22% |
| 4 | p1-asc | 236,306 | 3.80 | 95.0% | 100% | 0.31 / 2.5 | 0.0% | 41.3% | 492 s | 0.29% |
| 4 | p2-desc | 241,303 | 3.93 | 98.3% | 100% | 0.33 / 2.5 | 0.0% | 40.7% | 476 s | 0.16% |
| 2 | p2-desc | 120,031 | 1.91 | 95.3% | 100% | 0.14 / 2.5 | 0.0% | 38.9% | 1111 s | 0.22% |
| 1 | p2-desc | 56,771 | 0.96 | 96.2% | 100% | 0.07 / 2.5 | 0.0% | 40.8% | 2505 s | 0.25% |
| 1 | sentinel | 57,489 | 0.98 | 98.5% | 100% | 0.08 / 2.5 | 0.0% | 43.8% | 2473 s | 0.19% |

| cores | passes | mean records/s | spread | reportable |
|---:|---:|---:|---:|---|
| 1 | 3 | 58,327 | 6.8% | yes |
| 2 | 2 | 120,116 | 0.1% | yes |
| 4 | 2 | 238,804 | 2.1% | yes |

Sentinel: the 1-core case first (60,720 rec/s) and last (57,489 rec/s), drift -5.5% across the suite; counted in that case's spread.
