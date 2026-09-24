# Plan — clean-room run 48

**Nobody approved this plan.** It is an unattended clean-room run with no human
to ask (SKILL.md §1a). It is written before any code or number exists, so a
reader can see what the run committed to. The spec is `ANSWERS.md` (the skill's
six defaults, verbatim); what I decided beyond them is `ASSUMPTIONS.md`.

## The test, stated in full

| | |
|---|---|
| the objective | **near-linear** scaling across 1, 2 and 4 cores. Each step must return **1.90× or better** on a doubling. Two steps are reported, 1→2 and 2→4; 2→4 leads |
| where it runs | this laptop, in Docker Desktop (Apple Silicon, 8 CPUs, 16 GB host, 9,937 MiB VM) |
| the stack | `flink:1.20.1-scala_2.12-java17` and `apache/kafka:3.9.0` (question 6: Apache), plus Prometheus, Grafana and the skill's Docker CPU exporter for the dashboard |
| what is measured | the drain rate of a fixed backlog of orders at 1, 2 and 4 cores, read from the committed offsets on the `orders` topic, with the pipeline's CPU, GC, Kafka's CPU and memory, and back-pressure beside it. The second, independent reading is the position rows written ÷ 5 |
| what is proved first | completeness with no tolerances on a 20,000,000-order data set, then the same again after the pipeline is **killed mid-run** (at about 50%) and restarted. No throughput table is published for a build that has not passed both |
| what is built | the Flink DataStream job, a deterministic generator (orders and prices), a verifier, and the dashboard |
| the shape of the suite | **3 passes** per case, ascending then descending then ascending (1,2,4 / 4,2,1 / 1,2,4), then the 1-core baseline once more as a drift check: **10 cases** |
| the guarantee | **exactly-once** checkpointing every **10 s**; an **at-least-once** Kafka sink made idempotent by emitting the absolute position per key |
| how long it takes | estimate **2 to 3 hours** for the chain: about 15 min of completeness, 20–30 min of tiny proof, 5–10 min of fill, 45–75 min of suite. Plus the time to build and debug the job first |
| what it writes | three backlogs of orders: suite **250,000,000** records, tiny proof **150,000,000**, completeness **20,000,000**, plus 16,384 price records. At an assumed ~90 bytes per compressed record that is about 22 GB, 14 GB and 2 GB of Kafka log — **tens of gigabytes**. The output topics are capped by retention (2 GiB per partition). Host free disk was 123 GiB at the start and is checked before every case |
| what it occupies | ports **19092** (Kafka), **18081** (Flink REST), **19090** (Prometheus), **13000** (Grafana); 8 partitions per topic; containers, volumes and the network all prefixed `st48` |
| where to watch it | **`results/PROGRESS.txt`** — one sentence, overwritten: which step of seven, which case of ten, and roughly how long is left. This run is a detached agent with no channel to anyone while it runs, so it does **not** promise progress messages; whoever started it reads that file |

## The pipeline

```
orders (JSON, 8 partitions, keyed by symbol)
  -> kafka-source -> parse-orders (once)
       main:  order  -> keyBy symbol                   -> position-by-symbol + market-value-by-symbol
       side:  4 allocations -> keyBy acct|sub|symbol   -> position-by-account + market-value-by-account
prices (4 ticks x 4,096 symbols) -> prices-source -> broadcast to both keyed operators

position-by-symbol   -> positions-by-symbol       (1 row per input, absolute position)   topics.out
position-by-account  -> positions-by-account      (4 rows per input)                      topics.out
market-value-by-*    -> market-values-by-symbol / market-values-by-account (throttled, every 10 s)   topicsAlsoWritten
```

- Fan-out: 5 rows per input (1 + 4). The market values are per interval, not
  per input, and are not counted.
- Keys: 4,096 symbols; 16,384 account / sub-account / symbol.
- Every operator at parallelism = cores = slots. Same job graph at 1, 2 and 4.
- G1 pinned for the worker at every case (`-XX:+UseG1GC`), per §5.
- Memory: 1024m + 512m per core for the worker; Kafka 5g with a 768m heap.

## Order of work

1. Build the job, generator and verifier; unit-check the generator's
   determinism and the verifier on a hand-made case.
2. `prove.py up` and `prove.py preflight` by hand, to see the key-layout row
   and the memory rows before `flinkProperties` is baked into the stack. If
   preflight names a `pipeline.max-parallelism`, add it, `down`, and start again.
3. `prove.py down`, then `prove.py all` **detached**, waiting on
   `results/DONE` through a scratch script (never a loop naming `prove.py`).
4. If a step misses 1.90×: `prove.py probe --repeats 9`, then `prove.py
   ceiling`, then §6a levers one at a time with predictions in `FIXES.md`, at
   most four, each measured with the tiny proof and reverted if it made a step
   worse.
5. Leave the stack up at the end.
