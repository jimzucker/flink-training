# Assumptions — clean-room run 48

**Every line here is mine and unapproved.** No human was available. The six
interview answers are the skill's defaults, copied verbatim into `ANSWERS.md`.
Everything below is what those defaults did not settle, written next to the
claim it feeds, so a reader can tell what was agreed (the defaults) from what I
decided (this file).

## What the defaults settle, restated so the build can be checked against it

| from the default | what it means for the build |
|---|---|
| two inputs: orders, and prices keyed by symbol and timestamp | two Kafka topics: `orders` (the one the harness fills, measures and drains) and `prices` (mine: created and filled by the generator, read by the job, never touched by the harness) |
| positions by symbol and by account / sub-account / symbol, published as they change, one per input | two keyed aggregations, each publishing the **absolute** position on every input: `positions-by-symbol`, `positions-by-account`. These are `topics.out` |
| 1 position per symbol + 1 per allocation, 4 allocations → 5 records | `outputsPerInput` = 5 |
| prices joined to **each** position output, market value every 10 s for both | two throttled outputs, `market-values-by-symbol` and `market-values-by-account`, both in `topicsAlsoWritten`, neither in `topics.out` |
| 4,096 symbols, 2 accounts × 2 sub-accounts | 4,096 symbol keys, 4 × 4,096 = **16,384** account / sub-account / symbol keys |
| in order, final positions exact at both levels, MV = final position × latest price at both levels, duplicates not double counted | the verifier asserts all of it with no tolerance (section 4 table) |
| DataStream, Apache Kafka | `flink:1.20.1-scala_2.12-java17`, `apache/kafka:3.9.0` |

## What I decided, and the claim each one feeds

| # | assumption (mine, unapproved) | the claim it feeds |
|---|---|---|
| A1 | **Every order carries exactly 4 allocations, one to each account / sub-account pair** (ACC1/SUB1, ACC1/SUB2, ACC2/SUB1, ACC2/SUB2). The default gives "if the order has 4 allocations it emits 5 records" as an example; the harness needs a *constant* fan-out, and 4 is the only count that uses every account key on every order. | `outputsPerInput` = 5 exactly; the second measurement (sink rows ÷ 5) is only valid if this holds |
| A2 | Symbols are named `SYM0000`…`SYM4095` and each order picks one **uniformly**. Real order flow is skewed (a few hot symbols); the default says nothing about popularity, and a uniform draw is the choice that does not bias the scaling result either way. A skewed workload would be a different study. | the key-layout preflight row; per-core throughput |
| A3 | The account-level key is the string `ACC1\|SUB1\|SYM0001`. Keys are Java `String`s in the job so the harness's key-group check (which hashes the manifest's key strings) sees exactly the keys Flink hashes. | the key-layout preflight row |
| A4 | Orders have a side (`B`/`S`) and a quantity of 100 × (4…100). Positions are signed: buys add, sells subtract. The four allocation quantities are a random split of the order quantity, each at least 100 and summing to it exactly. | "positions must match the input" |
| A5 | **How duplicates arise and how they are removed.** 0.5% of input records are a re-delivery of one of the previous 64 orders on the same partition (a producer retry, byte-identical). Each order carries a **per-symbol sequence number** assigned upstream; the job drops an order whose sequence is not above the last one applied for that key. This keeps dedup state at one number per key instead of every order id ever seen (hundreds of millions). It relies on A6. A duplicate still re-emits the key's current absolute position, unchanged, so every input record produces exactly 5 output rows and the fan-out stays constant. | "duplicates handled and not double counted"; `outputsPerInput` = 5 |
| A6 | The generator writes each order to partition `symbolIndex mod 8`, so every symbol lives in exactly one input partition and its orders arrive in sequence. | per-key order; A5's dedup |
| A7 | Every output row carries a `version`: the count of distinct orders applied to that key. "Published in order" is checked as **`version` never decreasing per key** (clean arm), and going backwards at most once per key (killed arm, section 4). Ordering holds per key only — not across keys, not across partitions — which is all a keyed stream promises. | the ordering assertions |
| A8 | Prices: 4 ticks per symbol (16,384 records) with increasing timestamps, prices in **integer cents**, generated from a fixed seed that does not depend on the backlog seed, so every manifest agrees on the latest price. "Latest" means highest timestamp. Market value = position × price in cents, exact 64-bit integer arithmetic. The generator writes `prices` once, the first time it runs, and leaves it alone after that (it checks the record count). | "market value = final position × latest price", exactly |
| A9 | **"A market value every 10 seconds" is read as a throttle**: each key has a processing-time timer on 10-second boundaries (interval configurable, default 10,000 ms); when it fires, a market value is emitted if the key's position or its symbol's price has changed since the last one emitted. An unchanged key emits nothing. A key whose symbol has no price yet waits. | the two market-value outputs; the drain check that both topics receive records |
| A10 | Prices reach both aggregations by **broadcast** (as the skill's diagram draws it): the account side is keyed on account / sub-account / symbol and cannot be joined to a symbol-keyed stream by key. | both market values |
| A11 | Position and market value for one key level live in **one** keyed operator (one per level), because the market value needs the position state and a second keyed operator would re-shuffle every position update. The operator is named `position-by-symbol + market-value-by-symbol` (and the account equivalent) so the design diff can find both names in the running plan, and each output has its own named sink. This is a build decision, stated so the diff is not read as two separate vertices. | the design diff |
| A12 | Orders are JSON (about 300 bytes), parsed **once** with Jackson into an object, then fanned out: the order to the symbol aggregation, its four allocations to the account aggregation through a side output (section 6a, "read the input once"). | per-record cost; the job graph |
| A13 | Both the generator and the job's sinks write with **lz4 compression**. Chosen before any number existed, for disk (the backlog is tens of gigabytes) and because the sinks carry five rows per input. It is not a tuning lever applied after measuring. | the disk budget; the broker's write load |
| A14 | State backend is the harness's (`hashmap`, file checkpoints). 20,480 keys of small state fit on the heap at every case. | memory per subtask |
| A15 | Pipeline memory: 1024m base + 512m per core (1,536m / 2,048m / 3,072m at 1 / 2 / 4 cores). Broker: 5g with a 768m heap, which leaves 4,352 MiB for page cache — the least the harness's record accepts. Worker at 4 cores + broker + job manager = 3,072 + 5,120 + 1,600 = 9,792 MiB of a 9,937 MiB VM, before the dashboard's ~768 MiB: **over-committed on paper**, relying on the broker's cache being elastic, as preflight's own note says is normal. | "memory is not the constraint"; the broker page-cache guard |
| A16 | Market values are verified from what reaches the topic before the harness cancels the job: it waits one checkpoint interval + 2 s (12 s) after the last offset commits, and the throttle fires within 10 s of the last change. If that margin is too thin the clean arm will say so. | the market-value assertions |
| A17 | Completeness backlog 20,000,000 orders (100,000,000 position rows), killed at 50%. At a guessed ~150,000 orders/s on one core that is about 13 checkpoint intervals, enough for the kill to land mid-run. | the kill test |
| A18 | Backlog sizes (suite 250,000,000; tiny proof 150,000,000) are **guesses made before anything was measured**, sized for a four-core rate up to about 800,000 orders/s. The tiny proof measures the real rate and says whether they are enough. | the headroom guard |

## Section 4 assertions: which apply

All six apply; none is skipped.

| assertion | how this pipeline makes it |
|---|---|
| distinct keys = predicted | 4,096 symbol keys and 16,384 account keys in both the position and the market-value topics |
| every aggregation sums to the manifest exactly | final position and final `version` per key equal the manifest's, at both levels |
| two paths over the same input agree | for every symbol, the four account keys' final positions sum to the symbol's final position |
| each key's values never go backwards | `version` per key, clean arm: never decreases; killed arm: at most one backward step per key, then the same final value |
| each key appears in exactly one partition | checked on all four output topics |
| after killing mid-run, all of the above still hold | the `killed` arm runs every assertion above with the relaxed ordering rule |

Plus, beyond the table: the clean arm must publish **exactly** 1 symbol row and 4
account rows per input record (duplicates included), and every final market
value must equal final position × latest price, at both levels.

## Where I changed the design after writing it

No change to the design: the design diff matched on the first completeness run and on every one after it.

One configuration change after measuring, recorded in `FIXES.md` as T1: `pipeline.max-parallelism: 440` (from Flink's default of 128) for a more even key layout. Mine, unapproved, and it had no measurable effect.
