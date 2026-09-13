| field | value |
|---|---|
| axis | one worker growing: one task manager container capped at N cores, parallelism N, N slots |
| API level | Flink DataStream API, hand-written operators (no SQL, no Table API) |
| guarantee | state: exactly-once checkpointing; sink: at-least-once, idempotent by emitting the absolute position per key |
| checkpoint interval | 10000 ms |
| build hash | `a5414abe5dd08261` (completeness passed for `a5414abe5dd08261`) |
| passes per case | 2 (`--quick`; the configured suite value is 3) |
| cases | [1, 2, 4], baseline 1 |
| backlog | 300,000,000 block trades over 8 partitions |
| outputs per input | 4 |
| rate source | committed broker offsets on `block-trades` |
| CPU source | cgroup `cpu.stat usage_usec` at window open and close |
| held still | broker cap 2.5 cores, job manager cap 0.5 cores, 8 partitions, checkpoint 10000 ms, sink retention 2 GiB per partition |
| quick look | quickLook=True, publishable=False |
| harness | lib.py `23e8b2ef6a87662b`, prove.py `36b381193ff5effd` |
| suite started / saved | 2026-09-07 12:57:04 EDT / 2026-09-07 13:22:46 EDT |
