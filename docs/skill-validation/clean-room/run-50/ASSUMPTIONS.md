# Assumptions

**Every line here is mine and unapproved.** No human was available for this
clean-room run. The six answers in `ANSWERS.md` are the skill's defaults
(SQL for question 5, as instructed). Everything below is what those answers
did not settle, written beside the claim it feeds, so a reader can tell what was
assumed from what was agreed.

Written 2026-09-24, before any code. Later changes are appended at the bottom
with the reason, never edited in place.

## What is built

| # | assumption | why | the claim it feeds |
|---|---|---|---|
| A1 | **Input `orders`** (the one the harness fills and measures). One JSON record per order: `id` (unique, the order's sequence number), `sym`, `qty`, `ts`, `allocs` = array of `{acct, sub, qty}`. | question 1 | the throughput number is orders per second |
| A2 | **Every order carries exactly 4 allocations, one to each account / sub-account pair** (ACC1/SUB1, ACC1/SUB2, ACC2/SUB1, ACC2/SUB2). Order `qty` = the sum of the allocation quantities. | question 2's worked example ("If the order has 4 allocations it emits 5 records"), and question 3's 2 accounts × 2 sub-accounts = 4 pairs. A *constant* 4 makes the fan-out a property of the job, which the second measurement divides by | `outputsPerInput = 5` |
| A3 | Allocation quantities are **signed** (buys and sells), 1–500 lots either way, drawn from a seeded random stream. | realism; a position that only grows is not a position | the verifier's totals |
| A4 | **4,096 symbols**, `S0000`–`S4095`, chosen uniformly per order. Account keys = 4 × 4,096 = **16,384**. | question 3 | distinct-key assertions: 4,096 and 16,384 |
| A5 | **Second input `prices`** (the pipeline's own, not the harness's): `{sym, ts, px}`, 3 ticks per symbol with increasing `ts`, price to 2 decimal places. Written by the generator on every fill from a **fixed price seed**, so re-writing it is idempotent. "Latest price" = the tick with the greatest `ts` per symbol. | question 1 ("keyed by symbol and timestamp"); the skill leaves the price feed to the pipeline | market value = final position × latest price |
| A6 | **"Duplicates" means records replayed after a failure**, not duplicate orders in the input. Handled by exactly-once checkpointing (state) + an upsert sink that writes the absolute value per key (sink). The generator never emits a duplicate order id. | §4 of the skill ties "duplicates" to the replay after a kill; de-duplicating hundreds of millions of order ids would need state that grows with the backlog, which is a different pipeline | the killed arm of completeness |
| A7 | **Positions are published one per input**: one row per order on `positions-by-symbol`, one per allocation on `positions-by-account`. Each row carries the absolute position and `orders` = how many inputs that key has absorbed. | question 1/2 | fan-out 5 |
| A8 | **`orders` (a per-key count) is the ordering witness.** Quantities are signed, so a position legitimately goes down; "never goes backwards" is asserted on the count instead, which must rise by exactly one per row on the clean run. | §4 asks the verifier to assert per-key order | the ordering assertion |
| A9 | **Market value is throttled with a 10-second processing-time tumbling window**: each key whose position changed inside a window gets one market-value row when the window closes, and again whenever its price changes. Rows carry position, price, market value and `orders`. | question 2 ("throttle it to a configurable interval defaulting to 10 seconds") — SQL has no timer API, a window is SQL's throttle | `market-values-by-symbol`, `market-values-by-account` |
| A10 | **Both market values** — by symbol and by account / sub-account / symbol. | question 1 says "each"; the skill says two runs built only one | design diff |
| A11 | **The price join is a regular SQL join on symbol, not a broadcast.** In SQL the optimizer chooses the exchange; a join on `symbol` re-partitions the account-level positions by symbol, which keeps every account key on one subtask (all its rows share a symbol), so per-key order still holds. The skill's "broadcast the prices" is DataStream advice; SQL cannot ask for a broadcast join in streaming mode. | question 5 = SQL | per-key order of market values |
| A12 | **Read the input once**: all four aggregations hang off one `orders` scan. In SQL this is the optimizer's source/sub-plan reuse inside one `STATEMENT SET`, not a hand-written side output. To be checked on the running plan (one source vertex). | §6a row 1 | the source vertex |
| A13 | Sinks are **upsert-kafka**, keyed by the aggregation key, so Kafka's default partitioner puts each key in exactly one partition, and a replay rewrites the absolute value. At-least-once delivery (the connector's default). | §4 "two settings" | exactly-once checkpointing + at-least-once idempotent sink |
| A14 | Sink and generator writes are **lz4-compressed**. | §6a says compression is what made one run's top case measurable; applied from the start, not as a tuning lever, so it is not a change chased after a number | the transport side of the cases |

## How it is measured

| # | assumption | why |
|---|---|---|
| M1 | Cases 1, 2, 4 cores; baseline 1; **3 passes** per case, plus the harness's sentinel. | the skill's unconditional 1/2/4; three passes so each step has more than one adjacent pair |
| M2 | 8 partitions on every harness topic and on `prices`. | divisible by 1, 2 and 4 |
| M3 | Checkpoint every **10,000 ms**, exactly-once. | the skill's configured default |
| M4 | Worker memory **1024m base + 640m per core** (1,664 / 2,304 / 3,584m). State is small here (≈ 41,000 keys across all operators); the memory is for parsing garbage and network buffers. | must be per subtask (§6); sized so worker + broker + job manager fit the 9,937 MiB VM |
| M5 | Broker **4,608m with a 1g heap** (3,584m of page cache), CPU cap 2.5. Job manager 0.5 core. | the harness's recorded floor is 3,328m of cache; 3,584m + the worker's 3,584m + 1,600m job manager = 9,792m of a 9,937 MiB VM |
| M6 | Backlog guesses, to be corrected by the tiny proof: suite **120,000,000**, tiny proof **60,000,000**, completeness **4,000,000**, kill at 40%. I have no rate yet; my guess is 50–200k orders/s at 4 cores for a SQL job with JSON parsing and four keyed aggregations. | the harness says guess high, then re-size from the tiny proof |
| M7 | Garbage collector: G1 on every case (the harness pins it). | §5 |
| M8 | `pipeline.max-parallelism` is not set unless preflight's key row asks for it. **Caveat:** preflight hashes the key *strings*; a SQL job hashes the key *row* (a binary row), so its prediction may not describe this job. To be checked, not assumed. | question 5 = SQL |

## Assertions that do or do not apply (§4)

| assertion | applies? |
|---|---|
| distinct keys = predicted | yes: 4,096 symbol keys, 16,384 account keys, on both position and market-value outputs |
| every aggregation sums to the manifest exactly | yes: per-key position and per-key order count, both key levels |
| two paths agree | yes: the account positions summed per symbol equal the symbol position, for every symbol |
| per-key values never go backwards | yes, on the per-key order count (A8); on the killed arm, at most one backward step per key, and the final value still exact |
| each key in exactly one partition | yes, all four output topics |
| market value = final position × latest price | yes, exactly (decimal arithmetic), both key levels, on the last row per key; and every row's own `mv = pos × px` |

## Appended later

- **M4 changed (2026-09-24 23:21), a tuning change, not a new assumption:** worker memory is now 1792m base + 544m per core (2,336 / 2,880 / 3,968m). Reason and measurements in `FIXES.md` fix 1. A second step (2560m + 400m) was measured and reverted (fix 2).
- **M6 was right about the order of magnitude:** the tiny proof measured 103,867 orders/s at 4 cores and said the suite needs 23,370,052 records; 120,000,000 were configured.
- **`sourceVertexMatch` narrowed from `orders` to `Source: orders`** after reading the worker's metric names (the source vertex is `Source: orders[1] -> (Calc[2], …)`), before the chain ran.
- **`design.operators` use bare operator words and sink table names** because the harness strips brackets and `=` before matching (`SKILL-FEEDBACK.md` F8).
