# Assumptions

**Every line here is mine and nobody approved it.** No human was available for
this run. The six interview answers are the skill's own offered defaults,
copied word for word into `ANSWERS.md`; everything below is what those
defaults did not settle and I decided on my own. Each row says which claim it
feeds, so a reader can tell what rests on it.

## What the interview answers left open

| # | assumption (mine, unapproved) | the claim it feeds |
|---|---|---|
| A1 | Every order carries **exactly 4** allocations, one per account / sub-account pair. The default says "if the order has 4 allocations it emits 5 records"; it does not say every order has 4. A constant 4 is what makes `outputsPerInput` a constant 5. | the second vantage point, which divides sink rows by 5 |
| A2 | The 4 symbols are `AAPL`, `MSFT`, `NVDA`, `TSLA`; the 2 accounts are `ACC1`, `ACC2`; the sub-accounts are `SUB1`, `SUB2`. The account key is the string `ACC1\|SUB1\|AAPL`. The default gives counts, not names, and Flink's key-group assignment depends on the exact strings. | the key-layout preflight row, and the `pipeline.max-parallelism` it names |
| A3 | Order and allocation quantities are **strictly positive**, so a position never decreases. | the "never goes backwards" assertion in section 4 — with negative quantities the assertion would have to be dropped |
| A4 | Allocation quantities **sum to the order quantity**. | the assertion that the two key levels sum to the same total — without it they are two unrelated numbers |
| A5 | "Latest price" means the price record with the **largest timestamp** seen for that symbol, not the last one to arrive. Prices arrive by broadcast and broadcast ordering across subtasks is not guaranteed. | the market-value assertion: final position x latest price, identically on every subtask |
| A6 | Prices **increase** with timestamp, and there are 8 price records per symbol, all written once by the generator before the job starts. | the market value never goes backwards either: a non-decreasing position times a non-decreasing price is non-decreasing |
| A7 | The record format is compact JSON. An order is about 175 bytes, a position row about 40. The default says nothing about the wire format, and record size decides the disk budget and where the pipeline saturates. | the disk projection, and whether the broker or the cores are the constraint |
| A8 | "Duplicates have to be handled and not double counted" is delivered by **exactly-once checkpointing** on the state plus an **at-least-once sink made idempotent by emitting the absolute position**, and additionally by dropping an order whose id equals the last id already folded into that key. The generator never produces the same order twice, so the only duplicates in this run are replays after the kill. | the killed arm of the completeness run |
| A9 | Orders are produced **round-robin across all 8 partitions**, not partitioned by symbol. Partitioning by symbol would leave 4 of 8 partitions empty and the 4-core case reading half its subtasks idle. The cost is that per-key input order is not defined across partitions, which is why A8 does not rely on a per-key sequence number. | the "input divides evenly across subtasks" guard |
| A10 | The market-value timer is **processing time**, not event time. The input carries no watermark-bearing time the interview asked for, and the throttle is described as "every 10 seconds", which is wall clock. | nothing in the throughput table; it decides only when market-value rows appear |

## Which section 4 assertions apply, and which do not

Section 4 says an assertion with nothing to compare is written down, not
skipped. All six apply to this pipeline:

| assertion | applies | why |
|---|---|---|
| distinct keys = the number predicted | yes | 4 and 16, from question 3 |
| every aggregation sums to the manifest exactly | yes | both key levels |
| two paths over the same input agree exactly | yes | symbol total = account total, by A4 |
| each key's published values never go backwards | yes | by A3 and A6 |
| each key appears in exactly one partition | yes | the sinks set the Kafka record key to the aggregation key |
| after killing the pipeline mid-run, all of the above still hold | yes | with backwards allowed at most once per key |

## What I changed in the design after writing it down

Nothing yet. If the design diff in the completeness run says the build is
missing something the interview asked for, the **build** is what changes.
If it says the design named something the interview never asked for, the
design changes and the change is recorded here with its reason.

## Things I decided that the harness, not the interview, forced

| # | assumption (mine, unapproved) | why |
|---|---|---|
| B1 | Project token `st47`; ports 19092 (Kafka), 18081 (Flink REST), 13000 (Grafana), 19090 (Prometheus), 19110 (the container CPU exporter). | `project` must be letters and digits only; the ports were stated free on this machine |
| B2 | Backlog sizes start as **guesses** — 150,000,000 for the suite, 90,000,000 for the tiny proof, 8,000,000 for completeness — and are re-sized from the rate the tiny proof measures. A first attempt cannot know the rate. | the harness fails the chain if the suite backlog is short of what the largest case needs |
| B3 | Worker memory `tmMemoryBase` 1024m + `tmMemoryPerCore` 640m, broker `kafkaMemory` 4608m with `kafkaHeap` 1G. At 4 cores that is 3,584 + 4,608 + 1,600 = 9,792 MiB against a 9,937 MiB virtual machine. The shipped example's 6g broker would not fit beside a 4-core worker here. | the memory-is-not-the-constraint guard, and the broker's page cache |
| B4 | Kafka is capped at 2.0 cores, the job manager at 0.5, and the three dashboard containers at 0.65 between them. At the 4-core case that is 7.15 of 8 host cores. | nothing sharing the cores under test may be uncapped |
| B5 | The sink producers do **not** compress to begin with. Compression is on the tuning list as a lever, and starting with it would mean the run never measured what it was worth here. | recorded in `FIXES.md` if it is used |
