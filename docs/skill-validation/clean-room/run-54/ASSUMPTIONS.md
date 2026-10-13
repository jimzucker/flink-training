# Assumptions

**Every line here is mine and unapproved.** Clean-room run 54 had no human to
ask. `ANSWERS.md` holds the six interview answers (the skill's defaults,
verbatim). This file holds what those answers did not settle, beside the claim
each one feeds. A reader should treat each as a decision they have not yet
agreed to.

## The input

| assumption | why | the claim it feeds |
|---|---|---|
| **Every order has exactly 4 allocations, one to each account / sub-account pair** (ACC1/SUB1, ACC1/SUB2, ACC2/SUB1, ACC2/SUB2). | Question 2 says "if the order has 4 allocations it emits 5 records". 2 accounts × 2 sub-accounts is 4 pairs. The harness needs a fan-out that is a constant per input, so the count cannot vary. | `outputsPerInput` = 5; the second measurement divides output rows by 5 |
| Allocation quantities are 1–1,000 shares each. The order quantity is their sum. Each order is a buy or a sell (50/50); a sell lowers every position it touches. | The answers give no sizes. Sells make the position go down as well as up, so a position value that only rises cannot be mistaken for an order check (SKILL §4: "The value itself may go down"). | the final-position assertions |
| Symbols are `SYM0000`–`SYM4095`, chosen uniformly at random per order from a seeded generator. | 4,096 symbols is the answer to question 3; uniform is mine. | 4,096 symbol keys; 16,384 account keys |
| The account key is the string `ACC1/SUB1/SYM0000`. | Flink hashes the key string, and preflight hashes the same strings from the manifest, so the two must be spelled the same way. | the key-spread preflight row |
| Order ids rise within each input partition: id = partition × 2^40 + sequence. | SKILL §4 asks for repeats to be dropped with bounded memory: one number per input partition per key. | duplicates counted once |
| **0.2% of orders are written twice on purpose**, the repeat immediately after the original, in the same partition, with the same id. `tradeCount` in the manifest counts every record on the topic, repeats included; every total counts each order once. | SKILL §4 asks for a small share of repeats; run 48 used 0.5%. I chose 0.2% to leave room inside the harness's 5% two-way-count tolerance, because a dropped repeat writes no output. | "duplicates are counted once"; the two-way count trails by 0.2% by construction |
| Records are compact JSON, about 200 bytes an order. The generator compresses its batches with lz4. | The answers name no format. JSON costs something to parse, which keeps the pipeline on its cores rather than on the broker (SKILL §6a, "make the pipeline cost something per record"). | where the cores go; backlog size on disk |

## The second input: prices

| assumption | why | the claim it feeds |
|---|---|---|
| Prices are a topic `prices`, 8 partitions, keyed by symbol. Each symbol has **3 prices** with rising timestamps; the latest is the one with the highest timestamp. Price is an integer number of cents. | "Keyed by symbol and timestamp" and "latest price" need more than one price per symbol to mean anything. Cents keep market value exact. | market value = final position × latest price, exactly |
| The prices do **not** depend on the backlog seed. Every fill writes the same 12,288 price records, and only when the topic holds fewer than that. | The suite, the tiny proof and completeness use different seeds and all read the same `prices` topic. Seed-dependent prices would leave several "latest" prices per symbol. | the market-value assertion |
| Prices are written **before** the orders, and the job reads them from the start of the topic. | A market value needs a price. Until a symbol has one, its market value is held and retried at the next 10-second boundary. | every key gets a market value |
| The job takes a price only if its timestamp is not older than the one it holds. | Makes "latest" independent of read order. | same |

## The pipeline

| assumption | why | the claim it feeds |
|---|---|---|
| One operator per key level holds the running position **and** emits the market value. It reads the prices as a broadcast. | SKILL §1: "Broadcast the prices to both aggregations." Putting the market value in the same keyed operator avoids a second shuffle. The diagram in the skill draws them as two boxes; this build makes them one operator with two outputs. | the design diff; the job graph |
| Market value is emitted on 10-second boundaries of processing time, per key, only when the position changed since the last one emitted. | "Every 10 seconds", "throttled". Emitting unchanged values would make the output count meaningless. | `design.every` = 10 s |
| Market-value records carry the same input count `n` as the position they were computed from. | Gives the market values a per-key value that only rises, so their order can be checked too (question 4: "positions and market values must be published in order"). | the ordering assertion on the market values |
| A changed price does **not** by itself trigger a new market value. | The prices here are written once, before any order, so every key's final market value is computed after its final price has arrived. A price feed that changes during the run would need this; this one does not. | stated here so a reader does not assume it |
| Output records are keyed by their aggregation key, partitioned by Kafka's default key hash, lz4-compressed. | One key, one partition: per-key order survives end to end (SKILL §4). Compression is SKILL §6a's fourth lever, chosen at design time, not as a fix. | the "each key in exactly one partition" assertion |

## Measurement settings

| assumption | why | the claim it feeds |
|---|---|---|
| Checkpoint interval 10 s; settle 15 s after the last input in completeness. | 10 s matches the throttle. The default settle is 12 s; 15 s gives the last 10-second market value margin. | completeness on the market values |
| Partitions 8; cases 1, 2, 4; baseline 1; 3 passes. | 8 divides every case. 3 passes is the example's figure and gives 10 measured cases with the sentinel. | the table |
| Worker memory 1,024 MiB + 512 MiB per core (1,536 / 2,048 / 3,072 MiB). Broker 5,632 MiB with a 1 GiB heap (4.5 GiB of page cache). | The Docker VM has about 12,955 MiB. 4 cores' worker limit (3,840 MiB) + broker (5,632) + job manager (2,048) + 1 GiB spare = 12,544 MiB. The example's figures (3,648 MiB worker, 6 GiB broker) would not leave the spare gigabyte the harness asks for. The state here is 20,480 small keys, so the worker needs little heap. | memory is not the constraint |
| Backlogs: suite 250,000,000; tiny proof 150,000,000; completeness 6,000,000. Kill at 35%. | A guess before any rate is known (harness README: "Guess high, run the tiny proof, and re-size"). 250M covers about 500,000 orders/s at four cores. 6M is 10 checkpoint intervals if one core does 60,000/s, and 4 at 150,000/s; the kill needs several. | backlog headroom |
| Kafka broker capped at 2.5 cores; job manager 0.5; Grafana 0.3, Prometheus 0.3, exporter 0.2. | The harness's defaults for the first two. 4 + 2.5 + 0.5 + 0.8 = 7.8 of 8 host cores. | the broker is not the constraint |

## Assertions that do not apply, or apply in part

| assertion (SKILL §4) | here |
|---|---|
| two paths over the same input agree exactly | applies: the sum of every symbol position equals the sum of every account position, and both equal the manifest's signed total |
| distinct keys = the interview's number | applies: 4,096 and 16,384, checked on every output topic |
| each key's values never go backwards | applies to all four outputs on the clean run. On the killed run, at most one backward step per key |
| market value = final position × latest price | applies to both market-value topics; the latest price comes from the manifest, not from the job |
| the input repeats a share on purpose | applies: 0.2% |
