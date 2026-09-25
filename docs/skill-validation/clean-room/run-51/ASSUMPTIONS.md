# Assumptions

Everything below is **mine and unapproved**. No human was available (clean-room
run 51). Each line names the claim or the check it feeds. The answers
themselves are in `ANSWERS.md`.

## The business case, as built

| # | assumption | what it feeds |
|---|---|---|
| A1 | Every order carries **exactly 4 allocations**, one for each account / sub-account pair (A0/0, A0/1, A1/0, A1/1). Question 2's "if the order has 4 allocations it emits 5 records" is taken as the constant, not an example. | `outputsPerInput` = 1 + 4 = **5**, the second measurement of throughput. A variable allocation count would make the fan-out a property of the test data, which §5 forbids. |
| A2 | The order quantity equals the sum of its allocation quantities. Allocation quantities are uniform 1–100. | Two paths over the same input must agree exactly (§4): the symbol position must equal the sum of the four account positions for that symbol. |
| A3 | Symbols are drawn uniformly from 4,096 (`S0000`–`S4095`). Accounts `A0`, `A1`; sub-accounts `0`, `1`. So 4,096 symbol keys and 4 × 4,096 = **16,384** account / sub-account / symbol keys, once enough orders have arrived for every combination (every order covers all 4 account pairs of its symbol, so 16,384 needs only every symbol seen). | Cardinality assertion in the verifier; `keySets` in preflight. |
| A4 | "Duplicates have to be handled and not double counted" is read as **redelivery duplicates** — records replayed after a failure — not duplicate order ids in the input. Every order id in the input is unique, as question 1 says ("an order arrives with a unique id"). Duplicates are handled by exactly-once checkpointed state plus a sink that writes the absolute value per key, so a replayed record is never folded twice and a repeated output row is harmless. The killed completeness arm is what tests this. De-duplicating by order id in SQL would mean state that grows with every order ever seen (hundreds of millions of ids), which the business case did not ask for. | The killed-arm assertions; the guarantee in the header. |
| A5 | Prices: 3 ticks per symbol (timestamps 1, 2, 3), integer **cents**, deterministic from a fixed seed that does not change with the backlog. The latest price is the tick with the highest timestamp. Prices are written once, before any job starts, to a topic `prices` the pipeline owns (not in `topics.in` or `topics.out`). Integer cents keep "market value = final position × latest price" exact, with no rounding. | The market-value assertion. |
| A6 | Market value output rows: `symbol, position, price, market_value, updates` and `account, sub_account, symbol, position, price, market_value, updates`. `updates` is the number of order / allocation rows folded in, carried so the verifier can check per-key order on a count that must rise. | Per-key ordering check on the market-value topics. |
| A7 | "Published in order" is checked **per key** (the only order a keyed stream promises): on the clean arm a key's `updates` never goes backwards in any output topic; on the killed arm it may step backwards **at most once** per key and must end on the exact final value (§4). | Verifier. |
| A8 | The market value is throttled on **processing time**, a 10-second tumbling window (interval configurable by a job argument, default 10 s), as the SQL row of the skill's table says. A market value row is emitted per key per window in which that key changed, and again whenever that key's latest price changes. | `topicsAlsoWritten`; `settleS`. |
| A9 | Market values are **not** counted in the fan-out: they are per interval, not per input. | `outputsPerInput` = 5. |

## How it is built (SQL)

| # | assumption | what it feeds |
|---|---|---|
| B1 | One Java `main` builds a `TableEnvironment`, declares the tables in DDL and submits all four `INSERT`s in **one `STATEMENT SET`**, so the orders topic is scanned once. `EXPLAIN` is printed at submit and I check it shows one scan of `orders`. | "read the input once" (§6a row 1). |
| B2 | One position row per allocation via `CROSS JOIN UNNEST`. | Fan-out 5. |
| B3 | Every output is an `upsert-kafka` table with `PRIMARY KEY` = the aggregation key, JSON key and value. That makes each key land in exactly one partition and makes the sink idempotent by the absolute value. | §4 "each key appears in exactly one partition"; the guarantee. |
| B4 | Every aggregate carries a `COUNT(*)` column so that no update is ever a no-op (the skill's SQL table: an unchanged row is not guaranteed to be written). Mini-batch stays **off**, so one input row gives one output row per aggregate. | `outputsPerInput` = 5 holding exactly. |
| B5 | Latest price per symbol is a `ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY ts DESC) = 1` over the price topic, then a **regular join on symbol** to both running positions (SQL has no broadcast). Each account key carries exactly one symbol, so the join does not reorder a key. | Two market values, both joined. |
| B6 | The prices source uses its own consumer group (`<group>-prices`) and always reads from the earliest offset, so the harness's consumer group only ever holds the orders topic. | Committed-offset measurement reads only the input. |
| B7 | Guarantee: exactly-once checkpointing every 10 s; at-least-once `upsert-kafka` sink made idempotent by the absolute value per key. | Header fields. |

## Checks that do not apply, or apply loosely

| check | why |
|---|---|
| Preflight's key-spread row | It hashes keys the way DataStream does. A SQL job keys on a binary row, so its layout is not the one checked. The skill itself says: "treat its PASS as unchecked for SQL". I will report it as **not checked for this build**, whatever it prints. |
| Distinct keys = the interview's prediction, on the completeness backlog | Holds only if the completeness backlog touches every symbol. At 3,000,000 orders over 4,096 symbols every symbol is seen (expected ~730 orders each), so 4,096 and 16,384 are asserted exactly, from the manifest and against the prediction. |
| Market-value per-key order on the killed arm | Asserted on `updates` (must not step back more than once per key). The price column may change once as the price ticks arrive at startup; that is a price change, not a reordering, and is not asserted. |

## Rig choices the answers did not settle

| # | assumption | what it feeds |
|---|---|---|
| C1 | Run token and project prefix `st51`. Ports: Kafka 19092, Flink REST 18081, Grafana 13000, Prometheus 19090, CPU exporter 19110. | Everything the stack occupies. |
| C2 | 8 partitions (divides 1, 2 and 4). | Even input split. |
| C3 | Worker memory: `tmMemoryBase` 1088m + `tmMemoryPerCore` 640m (1,728m / 2,368m / 3,648m at 1 / 2 / 4 cores). Broker `kafkaMemory` 4608m with a 1G heap, which leaves 3.5 GB of page cache — above the harness's recorded floor of 3,328 MiB — and keeps worker at 4 cores + broker + job manager = 3,648 + 4,608 + 1,600 = 9,856 MiB inside this 9,937 MiB VM. Section 6a's warning (run 45: a broker sized past the VM starved the four-core worker) is why I did not take the example's 6g. | Whether the worker, not memory, is the constraint. |
| C4 | Backlog sizes are **guesses to be re-sized by the tiny proof**: suite 60,000,000, tiny proof 30,000,000, completeness 3,000,000, kill at 50%. The skill says a SQL build ran at about a quarter of the DataStream builds' throughput per core; I have not measured mine. | Tiny proof's backlog check. |
| C5 | `settleS` 25 s: the final 10-second window has to close and its market value pass the join before completeness cancels the job; the default (checkpoint + 2 s = 12 s) leaves 2 s of slack. | Completeness of the market-value topics. |
| C6 | Prometheus reporter through `flinkProperties` (plugin already in the image), Prometheus + Grafana + the shipped Docker CPU exporter through `extraServices`, each with a CPU cap. | Dashboard (§7). |

## Recorded after the runs

- Preflight's key-spread row **passed** (symbol keys 1035/1004/1032/1025 per subtask at 4 cores,
  account keys 4064/4137/4050/4133). As planned, this is **not checked for this build**: it hashes
  the key strings the DataStream way, and a SQL job keys on a binary row.
- The dashboard was built and every panel has data over the suite's time range (checked by
  querying Prometheus for each panel's expression). It was not rendered to an image: no
  renderer is installed in the stack.
- The final table (`results/suite.*`) is suite A, run with `prove.py suite` after `prove.py
  tinyproof` passed on its own, not inside `prove.py all`: two `all` chains stopped at the tiny
  proof on single noisy readings of this same build (FIXES.md, SKILL-FEEDBACK.md S12).
