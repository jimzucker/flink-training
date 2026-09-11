| field | value |
|---|---|
| axis | one worker growing: one task manager container capped at N cores, parallelism N, N slots |
| API level | Flink DataStream API, this repository's own PositionsJob (no SQL, no Table API) |
| guarantee | state: exactly-once checkpointing; sink: at-least-once, idempotent by emitting the absolute position per key |
| checkpoint interval | 5000 ms |
| build hash | `d465a077bb70b189` (completeness passed for `d465a077bb70b189`) |
| passes per case | 3 |
| study | scaling: every case configured identically |
| workload | 150,000,000 records, symbolCount 4, accountCount 4, distinctSymbolKeys 4, distinctAccountKeys 16, symbolUpdates 150,000,000, accountUpdates 600,000,000, 5 outputs per input |
| rate source | committed broker offsets on `block-trades` |
| CPU source | cgroup `cpu.stat usage_usec` |

**1→2 cores: 1.99× (99% of linear), range 1.90–2.08× across passes.**
**2→4 cores: 2.01× (101% of linear), range 1.93–2.11× across passes.**

| cores | pass | records/s | tm cores | % of cap | throttled | broker cores | src idle | src BP | headroom | vantage |
|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | p1-asc | 74,649 | 1.00 | 99.7% | 100% | 0.08 / 2.5 | 0.0% | 31.3% | 1872 s | 0.29% |
| 2 | p1-asc | 144,856 | 2.00 | 99.8% | 100% | 0.16 / 2.5 | 0.0% | 29.5% | 894 s | 0.27% |
| 4 | p1-asc | 294,499 | 3.82 | 95.5% | 99% | 0.30 / 2.5 | 0.0% | 46.7% | 369 s | 0.17% |
| 4 | p2-desc | 305,852 | 3.86 | 96.5% | 99% | 0.28 / 2.5 | 0.0% | 46.5% | 344 s | 0.58% |
| 2 | p2-desc | 147,732 | 1.92 | 95.9% | 100% | 0.13 / 2.5 | 0.0% | 29.1% | 876 s | 0.22% |
| 1 | p2-desc | 76,459 | 0.95 | 95.5% | 100% | 0.07 / 2.5 | 0.0% | 29.3% | 1826 s | 0.25% |
| 1 | p3-asc | 73,371 | 1.00 | 99.7% | 100% | 0.07 / 2.5 | 0.0% | 29.2% | 1895 s | 0.22% |
| 2 | p3-asc | 152,413 | 1.94 | 97.0% | 100% | 0.14 / 2.5 | 0.0% | 27.7% | 845 s | 0.36% |
| 4 | p3-asc | 294,971 | 3.98 | 99.6% | 100% | 0.29 / 2.5 | 0.0% | 46.8% | 363 s | 0.08% |
| 1 | sentinel | 74,298 | 0.99 | 99.5% | 100% | 0.07 / 2.5 | 0.0% | 25.8% | 1875 s | 0.41% |

| cores | passes | mean records/s | spread | reportable |
|---:|---:|---:|---:|---|
| 1 | 4 | 74,694 | 4.1% | yes |
| 2 | 3 | 148,334 | 5.1% | yes |
| 4 | 3 | 298,441 | 3.8% | yes |

Sentinel: the 1-core case first (74,649 rec/s) and last (74,298 rec/s), drift -0.5% across the suite; counted in that case's spread.
