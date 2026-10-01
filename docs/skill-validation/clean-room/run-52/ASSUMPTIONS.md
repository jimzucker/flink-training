# Assumptions

**Every line here is mine, made by the agent of clean-room run 52, and nobody
approved any of it.** The six interview answers are the skill's defaults
(`ANSWERS.md`). Below is everything those answers did not settle, beside the
claim or check it feeds.

## The six answers themselves

| question | answer taken | how |
|---|---|---|
| 1. what goes in and out | orders in (scaled), prices in (small); positions by symbol and by account / sub-account / symbol out per input; two market values out every 10 s | default, verbatim |
| 2. fan-out | 5 per order (1 + 4 allocations); market value throttled, not counted | default, verbatim |
| 3. keys | 4,096 symbols; 2 accounts × 2 sub-accounts × 4,096 symbols = 16,384 account keys | default, verbatim |
| 4. exactly right | per-key order; final positions match the input at both levels; market value = final position × latest price at both levels; duplicates counted once | default, verbatim |
| 5. API | DataStream | default |
| 6. Kafka | Apache (`apache/kafka:3.9.0`) | default |

## What the answers left open

| # | assumption | what it feeds |
|---|---|---|
| A1 | **Every order has exactly 4 allocations, one to each of the 4 account / sub-account pairs** (A1/S1, A1/S2, A2/S1, A2/S2). The order quantity is split among them at random; the four parts sum to the order quantity exactly. | the constant fan-out of 5 (`outputsPerInput`), and the 16,384 account keys all appearing |
| A2 | Symbols are drawn **uniformly** from `SYM0000`…`SYM4095`. A real market is skewed; the interview did not ask for skew, and skew would put a key-skew ceiling on the 4-core case that is not about cores. | the key-spread preflight row and the scaling claim |
| A3 | Quantities are **signed** (buys positive, sells negative), 1–1,000 shares, so a position can go down. "Never goes backwards" is therefore judged on a per-key **sequence number** (how many orders that key has absorbed), carried in every output row, not on the position's value. | the ordering assertion in the verifier |
| A4 | Prices are integer **cents** and positions integer shares, so market value = position × price is exact long arithmetic, with no rounding to argue about. Each symbol gets 3 price ticks with fixed timestamps; "latest" means the largest timestamp. | the market-value assertion, "exactly" |
| A5 | The price feed is written by the generator on every fill, **the same deterministic ticks every time** (a fixed price seed, independent of the backlog seed). Re-writing identical ticks is harmless: the job keeps the tick with the largest timestamp. The topic is `prices`, 8 partitions, keyed by symbol, with `retention.bytes` set. | completeness, and the price join being reproducible |
| A6 | Orders are keyed by **symbol** on the Kafka topic, so all of one symbol's orders are in one partition, and order ids rise within a partition. | the duplicate handling in A7 |
| A7 | "Duplicates are counted once" is tested two ways (SKILL.md §4): a restart's replay (exactly-once checkpointing + absolute values at the sink), and **a producer-style retry**: the generator re-sends 0.5% of records (every 200th record repeats the order sent just before it), in the same partition. The job drops a repeat because each aggregation keeps the last order id it applied per key and ignores an id it has already passed. Ids are unique and rise within a partition (A6), so this is exact. | the duplicate assertion; the dropped repeats make sink rows trail input by 0.5%, so `outputsPerInput` is 5 × 199/200 = **4.975** per record on the topic, not 5. It was first set to 5 with 1 repeat in 400; the completeness design diff compares `outputsPerInput` with the measured rows per record **exactly** (5 against 4.987) and stopped the run. 1 in 200 makes the true figure exactly 4.975, so the declared and measured values agree to the digit. See SKILL-FEEDBACK.md |
| A8 | The market value is emitted per key on a **10 s processing-time tick**, configurable by `--mvIntervalMs`, and only when the key's position or its symbol's price changed since the last emission. A key whose symbol has no price yet waits for the next tick. | "emits a market value every 10 seconds"; the final-value assertion |
| A9 | Each output is written with the aggregation key as the Kafka record key, so every key lands in exactly one partition. | the one-partition-per-key assertion, and per-key order end to end |
| A10 | Records are JSON. Generator and sinks compress with **lz4**: it keeps a 200,000,000-record backlog to a few GB on the broker and leaves the broker's page cache a chance on a 9.9 GB VM. Decompression is pipeline work, so it is inside what is measured. This is a design decision made before any number, not a tuning change. | disk budget; broker page cache |
| A11 | Prices reach both aggregations by **broadcast** (§1 says the account side cannot be keyed to a symbol-keyed price stream). | the design diff |
| A12 | The "two paths agree" assertion: for every symbol, the sum of its four account / sub-account positions equals the symbol position. | completeness |
| A13 | `settleS` = 25 s: the market value ticks every 10 s and must emit its final value after the last order is committed (one checkpoint of 10 s, then up to one tick). | completeness on the two market-value topics |
| A14 | Worker memory 1024m base + 512m per core (1,536m / 2,048m / 3,072m). Broker 4,352m with a 1G heap, which leaves 3.25 GB of page cache — the floor the harness enforces. Chosen to fit the 9,937 MiB VM with the dashboard; the memory row in preflight will report the sum. | the memory-per-subtask rule; the broker page-cache floor |
| A15 | Backlog sizes are **guesses before any rate exists**: suite 200,000,000, tiny proof 100,000,000, completeness 10,000,000. The tiny proof measures the real rate and the harness says what to change. | backlog headroom guard |
| A16 | Market-value rows carry the position sequence and the price timestamp; "in order" for a market value means neither goes backwards per key. | ordering assertion on the market-value topics |

## Assertions that do not apply

None. The pipeline has two paths over one input (A12), predicted key counts
(4,096 and 16,384), keyed outputs (A9), a kill arm, and deliberate repeats (A7),
so every row of SKILL.md §4's table has something to compare.
