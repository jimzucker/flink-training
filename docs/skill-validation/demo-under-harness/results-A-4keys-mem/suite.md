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
| workload | 150,000,000 records, symbolCount 4, accountCount 4, distinctSymbolKeys 4, distinctAccountKeys 16, symbolUpdates 150,000,000, accountUpdates 600,000,000, 5 outputs per input |
| rate source | committed broker offsets on `block-trades` |
| CPU source | cgroup `cpu.stat usage_usec` |

**1→2 cores: 2.08× (104% of linear) — one pass per case, no spread measured.**
**2→4 cores: 1.94× (97% of linear) — one pass per case, no spread measured.**

| cores | pass | records/s | tm cores | % of cap | throttled | broker cores | src idle | src BP | headroom | vantage |
|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | p1-asc | 74,475 | 1.00 | 100.0% | 100% | 0.09 / 2.5 | 0.0% | 28.2% | 1875 s | 0.23% |
| 2 | p1-asc | 154,601 | 1.98 | 99.2% | 100% | 0.14 / 2.5 | 0.0% | 27.4% | 830 s | 0.20% |
| 4 | p1-asc | 296,655 | 4.01 | 100.2% | 100% | 0.29 / 2.5 | 0.0% | 48.4% | 359 s | 0.19% |
| 4 | p2-desc | REFUSED (None) —  | | | | | | | | |
| 2 | p2-desc | 151,928 | 1.91 | 95.4% | 100% | 0.13 / 2.5 | 0.0% | 27.5% | 848 s | 0.25% |
| 1 | p2-desc | 70,348 | 0.99 | 99.4% | 100% | 0.07 / 2.5 | 0.0% | 26.0% | 1995 s | 0.30% |
| 1 | sentinel | 76,248 | 0.96 | 96.2% | 100% | 0.07 / 2.5 | 0.0% | 25.4% | 1832 s | 0.17% |

| cores | passes | mean records/s | spread | reportable |
|---:|---:|---:|---:|---|
| 1 | 3 | 73,691 | 8.0% | yes |
| 2 | 2 | 153,264 | 1.7% | yes |
| 4 | 1 | 296,655 | 0.0% | yes |

Sentinel: the 1-core case first (74,475 rec/s) and last (76,248 rec/s), drift +2.4% across the suite; counted in that case's spread.
