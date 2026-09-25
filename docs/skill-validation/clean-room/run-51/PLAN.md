# Plan — clean-room run 51 (SQL build of the default business case)

**No one approved this plan.** It is a clean-room run with no human to ask
(SKILL.md §1a). It is written before any code, so a reader can see what the
run committed to before any number existed. Answers: `ANSWERS.md`.
Everything the answers did not settle: `ASSUMPTIONS.md`.

## The test

| | |
|---|---|
| the objective | near-linear scaling across 1, 2 and 4 cores. Each step must return **1.80× or better** on a doubling, judged on the low end of its range. Two steps are reported, 1→2 and 2→4 |
| where it runs | this laptop, in Docker Desktop (8 CPUs, 16 GB host, 9,937 MiB VM) |
| the stack | `flink:1.20.1-scala_2.12-java17` and `apache/kafka:3.9.0` (question 6: Apache) |
| API | **Flink SQL** (question 5), submitted as one `STATEMENT SET` from a Java main. The optimizer decides the graph; I do not hand-write operators |
| what is measured | the drain rate of a fixed backlog of orders at 1, 2 and 4 cores, read from committed broker offsets, with CPU, GC, back-pressure and broker columns beside it |
| second measurement | rows in the two position topics ÷ 5 (one symbol position + four account positions per order) |
| what is proved first | completeness with no tolerances on 3,000,000 orders, once clean and once with the worker killed mid-run; no throughput table for a build that has not passed |
| what is built | the SQL job, a deterministic generator (orders + prices), a verifier, and the dashboard (Prometheus + Grafana + the shipped Docker CPU exporter) |
| the shape of the suite | 3 passes per case, ascending, descending, ascending, then the 1-core baseline once more as a drift check: **10 measured cases** |
| the guarantee | exactly-once checkpointing every **10 s**; at-least-once `upsert-kafka` sinks made idempotent by writing the absolute value per key |
| how long it takes | **2.5 to 3.5 hours** end to end, estimated. Clean-room runs have taken two to three hours; a SQL build is expected to be slower per core, so each case drains less per second but the backlog is sized to the rate, so the wall clock per case is roughly the same |
| what it writes | suite backlog **60,000,000** orders, tiny proof **30,000,000**, completeness **3,000,000** — about 230 bytes each as JSON, so roughly 14 GB + 7 GB + 0.7 GB of Kafka log, plus position outputs under the harness's retention. 129 GB is free on the host. These are guesses: the tiny proof measures the rate and re-sizes them |
| what it occupies | ports 19092 (Kafka), 18081 (Flink REST), 13000 (Grafana), 19090 (Prometheus), 19110 (CPU exporter); 8 partitions; containers, volumes and network prefixed `st51` |
| where to watch it | `results/PROGRESS.txt` — one sentence, overwritten: which step of seven, which case of ten, roughly how long is left. I am a detached agent with no channel to anyone while it runs, so **I will not send updates**; whoever started this run relays them from that file |

## The pipeline

```
orders (Kafka, 8 partitions) ──scan once──┬─ GROUP BY symbol: SUM(qty), COUNT(*) ──────────────► positions-by-symbol   (upsert-kafka, 1 per order)
                                          ├─ UNNEST allocations ─ GROUP BY account,sub,symbol ─► positions-by-account  (upsert-kafka, 4 per order)
                                          ├─ TUMBLE 10 s by symbol ─ running SUM ─┐
                                          └─ UNNEST ─ TUMBLE 10 s by acct/sub/sym ─ running SUM ─┐
prices (Kafka) ── ROW_NUMBER latest per symbol ──── JOIN on symbol ───┴──────────────────────────┴─► market-values-by-symbol, market-values-by-account (every 10 s)
```

| topic | owner | per order |
|---|---|---|
| `orders` | harness (`topics.in`) | input |
| `positions-by-symbol` | harness (`topics.out[0]`) | 1 |
| `positions-by-account` | harness (`topics.out[1]`) | 4 |
| `prices` | pipeline (written by the generator once) | second input |
| `market-values-by-symbol` | pipeline (`topicsAlsoWritten`) | per 10 s window |
| `market-values-by-account` | pipeline (`topicsAlsoWritten`) | per 10 s window |

Design block in `pipeline.json` names the scan of `orders` and `prices`, the
unnest, the window aggregate, the join and all four sinks, so the completeness
step diffs them against the running plan.

## Completeness assertions (verifier, no tolerances)

1. distinct keys: 4,096 symbols and 16,384 account / sub-account / symbol keys, in every output topic, equal to the manifest and to the interview's prediction;
2. final position per key equals the manifest, both levels;
3. the two paths agree: per symbol, the four account positions sum to the symbol position;
4. every final market value equals final position × latest price, both levels, and carries the latest price;
5. each key appears in exactly one partition, in all four output topics;
6. per-key order: `updates` never goes backwards on the clean arm; at most once per key on the killed arm;
7. the position topics hold exactly 5 × orders rows on the clean arm (one per input, per aggregate).

## Order of work

1. Write the job, generator, verifier; build the jar; run `EXPLAIN` to check the orders scan appears once.
2. Write `pipeline.json` (with the dashboard in `extraServices` and the reporter in `flinkProperties` before the first `up`).
3. Dashboard provisioning files.
4. `prove.py all` detached; wait on `results/DONE` with `harness/watch.sh` through a scratch script that keeps the project path off its command line.
5. If a step misses 1.80×: `probe --repeats 9`, then `ceiling`, then §6a levers one at a time with predictions in `FIXES.md`, at most four, measured with `tinyproof`.
6. `SKILL-FEEDBACK.md` throughout.
