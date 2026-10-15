# Assumptions

**Every entry here is mine (the agent's) and unapproved.** No human was
available to ask. The six answers in `ANSWERS.md` are the skill's defaults; this
file records everything those answers did not settle, beside the decision it
feeds.

## What the answers fix, read literally

| answer says | what I build |
|---|---|
| two key spaces: symbol; account / sub-account / symbol | 4,096 symbols `S0000`..`S4095`; accounts `ACC1`,`ACC2`, each with `SUB1`,`SUB2`, so 4 account/sub-account pairs and 4 × 4,096 = **16,384** account keys |
| "every order has exactly 4 allocations" | every order allocates to all four pairs, once each. Fan-out = 1 symbol position + 4 account positions = **5 records per input**, `outputsPerInput` = 5 |
| two inputs | `orders` (scaled, owned by the harness, `topics.in`) and `prices` (mine, `design.inputs`) |
| two position outputs, per input | `positions-by-symbol`, `positions-by-account` — `topics.out` |
| two market values, throttled to 10 s | `market-values-by-symbol`, `market-values-by-account` — `topicsAlsoWritten`, interval configurable (`--mvIntervalMs`, default 10000) |
| prices joined to **both** aggregations | prices are **broadcast** to both keyed operators (the account side cannot be joined by key to a symbol-keyed stream) |
| duplicates counted once | the generator repeats a share of records on purpose; the job drops them with bounded memory (below) |
| DataStream, Apache Kafka | Flink 1.20.1 DataStream API; `apache/kafka:3.9.0`; plain Apache Kafka client everywhere, no Schema Registry |

## Decisions the answers did not settle

| decision | what I chose | why / what it feeds |
|---|---|---|
| order record format | compact JSON, e.g. `{"id":…,"sym":"S0042","qty":-120,"ts":…,"alloc":[{"acct":"ACC1","sub":"SUB1","qty":-30},…]}` | realistic, and parsing costs something per record, so the cores are the constraint and not the transport (§6a "make the pipeline cost something per record"). Parsed **once** in `parse-orders` |
| quantity sign | signed: a negative order quantity is a sell. Allocation quantities are a deterministic split of the order quantity into 4 non-zero-sum parts that add up exactly | lets positions go down, so the ordering check is judged on a count, not on the value (§4) |
| prices | integer cents; 3 price records per symbol with rising timestamps (latest wins by timestamp, not by arrival). Written by the generator **only when the `prices` topic is empty**, from a fixed price seed, so every fill sees the same prices | the price feed is a static reference stream during a drain. Market value = position × price in cents, exact integer arithmetic, so the verifier can compare with no tolerance |
| price arriving after a key's last market value | **not handled**: a market value is recomputed on the timer after a position change, with the latest price known then. Prices are all on the topic before any order is filled, so in this test every price is known before the first timer | stated so a reader knows the join is "latest price at the time of the throttled emit", which equals "latest price" only because the price feed is static during a run |
| market value throttle semantics | per key: the first position change after an emit registers a processing-time timer `interval` later; the timer emits one market value with the key's current position and latest price. Keys that did not change emit nothing | "emits a market value every 10 seconds" read as at most once per 10 s per key that changed |
| input partitioning | the generator writes each order to partition `symbolIndex % 8`; a deliberate repeat goes to the same partition right after its original | every key lives in exactly one input partition, so per-key order is preserved through the source, and repeat-dropping needs one number per key |
| dropping repeats with bounded memory (§4) | order ids rise within each input partition (`id = seqInPartition × 8 + partition`). Each keyed operator keeps, per key, the highest order id applied, and drops any order with an id not above it | one long per key, never a set of ids; does not grow with the backlog |
| share repeated on purpose | 0.5% of records (deterministic from the seed). The harness's count includes the repeats, so the topic holds exactly `count` records; `uniqueOrders` = count − duplicateRecords | the harness reads the log end back against the manifest count |
| ordering evidence | every output record carries `n`, the number of inputs applied to that key so far | §4: judge "never backwards" on something that only rises per key |
| sink guarantee | at-least-once Kafka sinks, keyed by the aggregation key (one key → one partition), emitting the **absolute** position per key; lz4-compressed | §4's usual answer; idempotent by value |
| state guarantee | exactly-once checkpointing, hashmap state backend (the harness's) | §4 |
| checkpoint interval | **5,000 ms** | §5 measured the two-count disagreement at 10 s against 5 s (10.5% → 1.19% at four cores); the state here is small (≈20k keys plus 4,096 prices per subtask), so a 5 s checkpoint is cheap |
| `settleS` | 20 s | the throttle (10 s) is slower than the checkpoint, and the harness default is checkpoint + 2 s |
| partitions | 8, on every topic I own as well as the harness's | divides 1, 2 and 4 |
| output topics' partitions | the harness recreates `topics.out` and `topicsAlsoWritten` itself, with its own retention; I do not create them | harness contract |
| `prices` retention | created by the generator with `retention.bytes` 1 GiB | §3: retention on every topic written and never drained |
| key names | fixed width (`S0000`…), so string hashing is stable; preflight will say whether they divide evenly over 1, 2 and 4 subtasks | §3 key-spread row |
| distinct-key and two-path panels | the two keyed operators register their own metrics: `distinctKeys` (a counter raised when a key is first seen) and `quantityApplied` (sum of signed quantity applied) | §7: Flink exports neither. Both restart from zero if the job restarts, and the panel description says so |
| worker memory | `tmMemoryBase` 1024m + `tmMemoryPerCore` 640m (1,664m at 1 core, 2,304m at 2, 3,584m at 4; the harness sets the container limit 1.25× that) | every subtask the same memory (§6). My guess, not measured; the state is small |
| broker memory | `kafkaMemory` 5632m, `kafkaHeap` 1G: 4.5 GB of page cache | above the 4.25 GB floor the harness reads from its record; the VM is 12,955 MiB and the 4-core worker needs 4,480 MiB of limit, so I did not take the larger figure the example shows |
| Kafka CPU cap, job manager CPU cap | 2.5 and 0.5 cores | leaves the 4-core case its cores on an 8-core host |
| dashboard | Prometheus + Grafana + the shipped `docker_cpu_exporter.py`, all in `extraServices`, all CPU-capped (0.3, 0.3, 0.2) | §7 |
| ports | Kafka 19092, Flink REST 18081, Grafana 13000, Prometheus 19090, exporter not published | told these are free |
| project token | `st55` | prefixes every container, volume and network |
| passes | 3 per case (ascending, descending, ascending) + the sentinel = 10 cases | §5 |

## Assertions that do or do not apply (§4)

All of §4's assertions apply to this pipeline, and the verifier makes each one:
distinct keys (4,096 and 16,384), totals equal to the manifest, the two paths
agree (account positions summed per symbol equal the symbol positions), never
backwards per key on the clean arm and at most once on the killed arm, each key
in one partition, and repeats counted once. Market values at both levels are
checked as final position × latest price.

One that is weaker than it sounds: "never backwards" on the market-value topics
is judged on `n` carried in each market value. A market value is emitted per
interval, so it samples `n`; it can only rise on a clean run, and that is what is
checked.
