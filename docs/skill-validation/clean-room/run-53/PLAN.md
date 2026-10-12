# Plan — clean-room run st53

**No one approved this plan.** This is an unattended clean-room run with no
human to ask; SKILL.md §1a says to write the plan anyway and build against
it. It was written on 2026-10-11, before any code and before any number
existed. The six answers are in `ANSWERS.md` (the skill's defaults,
verbatim); everything they left open is in `ASSUMPTIONS.md`.

## The test

| | |
|---|---|
| the objective | near-linear scaling across 1, 2 and 4 cores. Each step must return **1.80× or better** on a doubling (judged on the low end of the step's range, with the harness's 1% margin). Two steps are reported, 1→2 and 2→4, the 2→4 step first |
| where it runs | this laptop (Apple Silicon, 8 cores, 16 GB, Docker VM 12,955 MiB), in Docker |
| the stack | `flink:1.20.1-scala_2.12-java17`, `apache/kafka:3.9.0`, plus Prometheus `v2.53.2`, Grafana `11.2.0` and the skill's Docker CPU exporter for the dashboard |
| what is measured | the drain rate of a fixed backlog of orders at each core count, read from committed broker offsets on the `orders` topic, with the worker's CPU, memory, garbage collection and Kafka's CPU and memory beside it. The second reading is the two position topics' rows divided by 5 |
| what is proved first | completeness with no tolerances, then re-checked after **killing the pipeline mid-run** (the worker container is killed at about 35% of the input and restarted). No throughput table is published for a build that has not passed both |
| what is built | the Flink DataStream job, a deterministic generator (orders and the price feed), a verifier, the dashboard, `README.md`, and `pipeline.json` |
| the shape of the suite | 3 passes per case at 1, 2 and 4 cores, ascending then descending then ascending, then the 1-core baseline once more as a drift check: **10 cases**, plus up to 6 more if a step's range straddles the target |
| the guarantee | **exactly-once checkpointing every 10 s**; at-least-once Kafka sinks made idempotent by publishing the absolute position per key |
| how long it takes | an estimate of **about 3 hours** for the chain once the build works (clean-room runs have taken 2–3 hours), plus about 1 hour to build and debug the job before it |
| what it writes | three backlogs of order records to Kafka: the suite's 250,000,000, the tiny proof's 180,000,000 and the completeness run's 15,000,000 — guesses until the tiny proof measures a rate. At roughly 40–60 bytes per record after compression that is about 20–25 GB of Kafka log, plus up to 32 GB of output topics (capped by retention). 117 GB is free on the host |
| what it occupies | ports **19092** (Kafka), **18081** (Flink REST), **13000** (Grafana), **19090** (Prometheus); 8 partitions per topic; containers, volumes and the network all prefixed `st53` |
| where to watch it | `results/PROGRESS.txt` — one sentence, overwritten by the harness: which step of seven, which case of ten, and roughly how long is left. This run is a detached agent with no channel to anyone while it runs, so it promises no updates; whoever started it can read that file |

## The pipeline

```
orders (8 partitions) -> order-source -> parse --main--> keyBy symbol ------------> position-mv-by-symbol --> positions-by-symbol
                                               \--side output--> keyBy account|sub|symbol -> position-mv-by-account -> positions-by-account
prices (8 partitions) -> prices-source -> parse-prices -- broadcast --> both keyed operators
position-mv-by-symbol  --every 10 s--> market-values-by-symbol
position-mv-by-account --every 10 s--> market-values-by-account
```

- The harness owns `orders` (topics.in) and `positions-by-symbol`, `positions-by-account` (topics.out, 5 records per order).
- `prices` is the pipeline's second input (`design.inputs`, `{in1}`), filled by the generator.
- `market-values-by-symbol` and `market-values-by-account` are the throttled outputs (`topicsAlsoWritten`, `design.every` = 10 s).

## Order of work

1. Write `pipeline.json`, `README.md`, the dashboard files.
2. Build the job, generator and verifier (one Maven module, shaded jar). Unit-check the generator's determinism and the verifier on the laptop against a hand-run stack before handing over to the harness.
3. `prove.py replay` and `prove.py selftest-pure`, then `prove.py up` and `prove.py preflight`, fixing anything it stops on.
4. `prove.py all`, detached, waiting on `results/DONE` through `harness/watch.sh` from a script that keeps the project path off every command line.
5. If a step misses, the §6 order: `prove.py probe --repeats 9`, `prove.py ceiling`, then at most four §6a changes, each written in `FIXES.md` with its prediction before it is measured, and reverted if the step got worse.
6. Leave the stack up. Commit the evidence the harness's `results/.gitignore` keeps.
