# Assumptions

**Every entry here is mine (the agent's) and unapproved.** No human was
available. The six answers in `ANSWERS.md` are the skill's defaults; this file
lists what those answers did not settle, beside the decision each one feeds.

## What the answers mean, read literally

| question | what the default leaves open | my decision | feeds |
|---|---|---|---|
| 1 | how many inputs | **two**: `orders` (scaled, owned by the harness) and `prices` (the pipeline's own, filled once) | topics, generator |
| 1 | how many outputs | **four**: `positions-by-symbol`, `positions-by-account` (one record per input, in `topics.out`), `market-values-by-symbol`, `market-values-by-account` (throttled, in `topicsAlsoWritten`) | topics, design diff |
| 1 | "joins those to **each** of the position outputs" | prices are **broadcast** to both keyed aggregations; the account side cannot be joined by key to a symbol-keyed stream | job |
| 2 | "If the order has 4 allocations" | **every** order has exactly 4 allocations, one per account / sub-account pair, so fan-out is exactly 1 + 4 = **5** (`outputsPerInput: 5`) | fan-out, verifier |
| 2 | throttled market value | configurable `--mvIntervalMs`, default 10,000 ms; **not** counted in the fan-out | job, `design.every` |
| 3 | key names | symbols `SYM0000`..`SYM4095`; accounts `ACC0`, `ACC1`; sub-accounts `SUB0`, `SUB1`; account key `ACC0/SUB1/SYM0042` | generator, `keySets` |
| 3 | distinct keys | 4,096 symbol keys; 2 × 2 × 4,096 = **16,384** account / sub-account / symbol keys | verifier, `keySets` |
| 4 | "in order" | per key only (the one guarantee a keyed stream makes). Every output record carries `n`, the count of distinct orders applied to that key, which only rises; the verifier asserts it never goes backwards on the clean arm and at most once per key on the killed arm | verifier |
| 4 | "Duplicates ... counted once" | the generator writes 0.5% of orders twice (same id, same partition, immediately after the original), the way a retrying producer would; the job drops repeats by remembering, per key, the highest order id applied | generator, job |
| 5 | — | DataStream API, hand-written operators | job |
| 6 | — | `apache/kafka:3.9.0`; the job, generator and verifier use only the Apache Kafka client, no Schema Registry, nothing Confluent-only | images |

## Build decisions the answers did not make

- **Record formats.** JSON. Order: `{"id":…, "symbol":…, "qty":…, "ts":…, "allocations":[{"account":…, "sub":…, "qty":…} ×4]}`. Quantities are signed whole numbers: buys positive, sells negative, so a position can fall. The four allocation quantities sum to the order quantity. Price: `{"symbol":…, "ts":…, "priceCents":…}`. Prices are whole cents so position × price is exact in a `long`.
- **Input partitioning.** The generator writes order `symbol` to partition `symbolIndex mod 8`, so every symbol (and therefore every account / sub-account / symbol key) lives in one input partition. Order ids rise within each partition. That is what lets the job drop a repeat with one number per key (the highest id applied), which is bounded memory, not a set of every id seen (SKILL.md §4).
- **Prices.** 4,096 symbols × 4 timestamps = 16,384 price records, written once by the generator on its first run with a fixed seed that does not depend on `{count}` or `{seed}`, in shuffled order so "latest" has to be decided by timestamp, not by arrival. A later fill sees the topic already holds 16,384 records and writes nothing. "Latest price" = the price with the highest timestamp for that symbol.
- **Market value join.** Each keyed aggregation is one operator that both keeps the running position and holds the broadcast prices: on each order it updates the position and publishes it (one record per input), and it sets a processing-time timer at the next 10-second boundary; when the timer fires it publishes position × latest price for that key. So each key emits at most one market value per interval, and only for keys whose position changed. I merged "running position" and "market value" into one operator per key level rather than two, because a second operator with a broadcast input cannot be chained and would re-serialise all five records per order. The design diff names both operators by their combined names.
- **Market value and the final price.** The verifier's "final position × latest price" relies on every price having reached the job before each key's last market value is emitted. Prices are 16,384 small records read from the start of their topic; the first timer fires 10 s after the first order. Stated, not proved by construction.
- **Guarantee.** Exactly-once checkpointing (hashmap state, set by the harness) at `checkpointMs` = 10,000; Kafka sinks at-least-once, made idempotent by publishing the absolute position (and the count `n`) per key. Sinks keyed by the aggregation key and compressed with lz4.
- **Topics I own.** `prices`, `market-values-by-symbol`, `market-values-by-account`: 8 partitions each, `retention.bytes` 1 GiB, created by the generator if missing.
- **Partitions.** 8 on every topic, divisible by 1, 2 and 4.
- **Memory.** `tmMemoryBase` 1088m + `tmMemoryPerCore` 640m (1728m / 2368m / 3648m); broker 5g with a 1G heap (4 GiB page cache, above the harness's recorded floor of 3,328 MiB). Chosen so worker + broker + job manager fit the 12,955 MiB Docker VM.
- **Ports and names.** Project token `st56`; Kafka 19092, Flink REST 18081, Grafana 13000, Prometheus 19090 (the harness defaults; all free on this machine).
- **Dashboard.** The harness's own, default rows; no Redis store.

## Assertions that do not apply

None of SKILL.md §4's assertions is skipped. There are two paths over the same
input (symbol and account), so "two paths agree" is checked as: for every
symbol, the sum of its four account positions equals its symbol position, and
likewise for the `n` counts. Cardinality is checked on all four output topics.
