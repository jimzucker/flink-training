# Assumptions

**Every line below is mine, made by the agent running clean-room run `st53`,
and none of it is approved.** No human was available. The six answers in
`ANSWERS.md` are the skill's defaults taken verbatim; this file is everything
those answers did not settle, each next to the claim or check it feeds.

## The business case, as built

| # | assumption | what it feeds |
|---|---|---|
| A1 | **Every order carries exactly 4 allocations, one for each account / sub-account pair** (A1/S1, A1/S2, A2/S1, A2/S2). Question 2 says "if the order has 4 allocations it emits 5 records", and question 3 says 2 accounts × 2 sub-accounts, so 4 is the only count that makes the fan-out a constant. | `outputsPerInput` = 1 symbol position + 4 account positions = **5**. The second vantage divides sink rows by it. |
| A2 | The 4 allocation quantities add up to the order quantity. An order's quantity is signed: positive is a buy, negative a sell, so positions go up and down. | The verifier's "two paths agree" check: per symbol, the sum of the four account positions equals the symbol position. |
| A3 | Symbols are `SYM0000`…`SYM4095` (4,096), chosen uniformly per order. Accounts `A1`, `A2`; sub-accounts `S1`, `S2`. Account keys are `A1\|S1\|SYM0000` etc.: 4 × 4,096 = **16,384**. | Distinct-key assertions: 4,096 symbol keys and 16,384 account keys, exactly. `keySets` for the key-spread preflight. |
| A4 | Quantities are whole numbers (each allocation 1–250 shares) and prices are whole cents. Market value is position × price in cents, a 64-bit integer, so "final position × latest price" can be checked exactly with no rounding. | The market-value assertion, with no tolerance. |
| A5 | **"Latest price" means the price with the greatest timestamp for that symbol, not the one that arrived last.** The price feed holds 3 prices per symbol (12,288 records), written in an order where the newest is not always last. | The market-value assertion; the broadcast state keeps the newest timestamp per symbol. |
| A6 | The price feed is filled once by the generator (it checks the `prices` topic and writes it only if it is short), with a fixed seed independent of the backlog seed, so every backlog sees the same prices. | The verifier can compute the expected latest price per symbol from the manifest. |
| A7 | "Published in order" is checked on a per-key **order count** carried in every output record (`orders`: how many orders this key has applied). A position itself moves up and down with buys and sells, so it cannot be the thing that "never goes backwards"; the count can. | §4's "each key's published values never go backwards" — asserted on positions and on market values, both key levels. |
| A8 | The market value is emitted on a processing-time timer every 10 s (configurable, `--mvIntervalMs`), for each key whose position or price changed since its last market value. A key with no price yet emits nothing until one arrives. | The throttled outputs `market-values-by-symbol` and `market-values-by-account`, declared in `topicsAlsoWritten` and `design.every`. |
| A9 | Positions and the market value are computed in **one keyed operator per key level**, which also receives the broadcast prices — not a separate market-value operator after the aggregation. Splitting them would add a network hop per position update for no change in what is published. | `design.operators`: `position-mv-by-symbol`, `position-mv-by-account`. The skill's diagram draws them as two boxes each; the build has one. |
| A10 | **Duplicates.** §4 asks the input to repeat a small share on purpose, as a retrying producer would. The generator repeats 0.5% of orders, each repeat written straight after its original in the same partition. The job drops a repeat by its order id: ids increase within a partition, the id says which partition it was written to (`id mod 8`), and each key keeps the highest id it has applied from each of the 8 partitions. An order id at or below that is a repeat. This costs 8 longs of state per key, not a set of every id ever seen. **It depends on ids increasing per partition**, which is true of this generator and would need a different design for a source that does not guarantee it. | Every total still matches the manifest, which counts each order once. The measured fan-out trails 5 by the repeat share (0.5%), inside the harness's 5%. |
| A11 | Replay after a restart is handled by exactly-once checkpointing of that same per-key state, not by the id check: the restored state and the restored offsets agree, so replayed orders are applied once. | The killed completeness arm. |
| A12 | Records are JSON text: realistic, and it gives the job real work per record (parsing once, writing five records). Order ids, record layout and topic keys are in `README.md`; real records are in `results/records.md`. | Per-record cost, so the cores are what the measurement constrains (§6a "make the pipeline cost something per record"). |

## Build decisions the skill leaves to the build

| # | decision | why |
|---|---|---|
| B1 | Exactly-once checkpointing every 10 s; Kafka sinks at-least-once, each record carrying the **absolute** position (never a delta), keyed by the aggregation key. | §4's two settings. |
| B2 | Input is parsed **once**; allocations leave the parse operator through a side output. | §6a row 1 ("read the input once"). |
| B3 | Prices are **broadcast** to both keyed operators. | §1: the account side cannot be joined to a symbol-keyed stream by key. |
| B4 | Sinks compress with lz4 from the start. | §6a row 4; decided before any number exists, not as a fix. |
| B5 | Object reuse is on. | Fewer copies per record; nothing in the job holds a reference across calls. |
| B6 | No `pipeline.max-parallelism` unless preflight's key row asks for one. 4,096 and 16,384 keys over 128 key groups should spread evenly. | §3 key-spread row. |
| B7 | Worker memory: 1024m base + 640m per core (container limit 1024m + 800m per core). Broker: 5g with a 768m heap (4.25 GB page cache, the floor the harness reads from its record). Both copied from a recorded configuration the harness marks as accepted (`record/configs.json`, run 20), not measured here. | Fits the 12,955 MiB Docker VM: worker 4,224m at 4 cores + broker 5,120m + job manager 2,048m + dashboard 896m = 12,288m. |
| B8 | Kafka gets 2.5 cores, the job manager 0.5, Grafana 0.3, Prometheus 0.3, the CPU exporter 0.2. | Leaves the 4-core worker its cores on an 8-core host. |
| B9 | Backlog sizes are guesses until the tiny proof measures a rate: suite 250,000,000; tiny proof 180,000,000; completeness 15,000,000; kill at 35%. Sized for a 4-core rate of up to about 1,000,000 orders a second. | The tiny proof re-sizes them if they are wrong. |

## Assertions that do not apply, or are weaker than they look

- **Distinct keys equal the prediction** — asserted exactly (4,096 and 16,384), which assumes the completeness backlog of 15,000,000 orders reaches every key. At about 3,660 orders per symbol it does; the manifest records how many keys had orders.
- **Two paths agree** — applies: symbol positions against the sum of account positions per symbol.
- **Each key in exactly one partition** — applies to all four output topics.
- **Latency** — not claimed, so the end-to-end latency panel and the event-time lag panel (both "when the claim is about latency" or "a pipeline left running") are left out of the dashboard.
- **Late records dropped** — the pipeline has no windows, so there is nothing late. Panel left out.
- **Network bytes between workers** — one worker on this axis. Panel left out.
