# Pipeline design

![Pipeline](pipeline.svg)

## Problem

Process a stream of **block trades** and publish **positions** aggregated two ways
in parallel. Then join **prices** to those positions and publish **market value**
the same two ways, emitted once per minute.

A block trade is a single order that is split across several accounts. One trade
of 400 shares allocated to 4 accounts becomes 4 allocations of 100 shares each.
That split is what makes the two aggregations differ: by symbol there is one key
per symbol, by account there is one key per account/sub-account/symbol pair.

## Object model

| Type | Fields | Notes |
|---|---|---|
| `BlockTrade` | `tradeId`, `symbol`, `side`, `quantity`, `allocations[]`, `eventTime` | one order, many allocations |
| `Allocation` | `account`, `subAccount`, `quantity` | a slice of a block trade |
| `Price` | `symbol`, `price`, `eventTime` | latest price per symbol |
| `Position` | key, `quantity`, `updatedAt` | running signed sum of allocation quantity |
| `MarketValue` | key, `quantity`, `price`, `marketValue`, `windowEnd` | `quantity × price` |

`side` is applied as a sign: BUY adds, SELL subtracts. Position is therefore a
running signed sum, not a count.

## Walkthrough

Numbered left to right, in the order the demo talks through them.

| # | Element | Key | Value |
|---|---|---|---|
| 1 | `orders` topic | `tradeId` | `BlockTrade` |
| 2 | `prices` topic | `symbol` | `Price` |
| 3 | `positions.by-symbol` topic | `symbol` | `Position` |
| 4 | `positions.by-account` topic | `acct/sub/sym` | `Position` |
| 5 | `mv.by-symbol` topic | `symbol` | `MarketValue` |
| 6 | `mv.by-account` topic | `acct/sub/sym` | `MarketValue` |

**Part 1 — positions.** Reads ①. *Split by allocation* turns one `BlockTrade`
into N `Allocation` records. The stream is then keyed two different ways off that
same split, and *Aggregate* maintains a running position per key. The two
aggregations run in parallel from one source, which is the point of the exercise:
the same input aggregated different ways at the same time. Writes ③ and ④.

**Part 2 — market value.** Reads ③, ④ and ②. *Join price × position* attaches the
latest price for the symbol to each position. *Window 1 min* emits one market
value per key per minute. Writes ⑤ and ⑥.

The price stream is shared by both joins — one source, consumed twice, keyed by
`symbol` in both cases. For the account-keyed join the symbol is extracted from
the composite key.

## Expected inputs and outputs

The numbers the demo is verified against. Every output ties back to a numbered
element above.

| Parameter | Input |
|---|---|
| Trades | 10 / sec |
| Symbols | 4 unique |
| Accounts | 4 unique |
| Allocations per trade | 4 |

| # | Sink | Rate | Unique keys | Why |
|---|---|---|---|---|
| 3 | `positions.by-symbol` | 10 / sec | **4** | one key per symbol |
| 4 | `positions.by-account` | 40 / sec | **16** | 10 trades × 4 allocations; 4 accounts × 4 symbols |
| 5 | `mv.by-symbol` | 4 / min | **4** | one emit per key per minute |
| 6 | `mv.by-account` | 16 / min | **16** | one emit per key per minute |

Sink 4 emits 4× the rate of sink 3 because each trade fans out to 4 allocations.
Sinks 5 and 6 emit once per minute per key regardless of input rate, because the
window collapses all updates in that minute to a single value.

## Determinism

The demo has to be reproducible and the numbers have to be explainable, so:

- **Generators are seeded.** The same seed produces the same trades in the same
  order, so two runs produce identical output.
- **Fixed reference data.** Exactly 4 symbols, 4 accounts, 4 allocations per
  trade — the numbers above are arithmetic, not statistics.
- **Event time, not processing time.** Windows key off an event timestamp stamped
  at the generator, so a slow consumer changes latency but never changes results.
- **Every record carries `tradeId`.** A single trade can be followed from ① through
  to ⑤ and ⑥ in the logs, so any number on screen can be traced back to its input.

## Diagram compliance

The assignment sets six rules for a pipeline diagram. Checked one by one:

| # | Rule | How it is met |
|---|---|---|
| 1 | Highlight keys and values between operators | Every edge is labelled `K:` / `V:`; legend defines the notation |
| 2 | Show sources and sinks | Generators outlined in green on the left; all six Kafka topics use the topic glyph |
| 3 | Use colours sparingly | Three only — blue for Part 1, amber for Part 2 and the price stream, green for generators; everything else greyscale |
| 4 | Number left to right in talk order | ① … ⑥, sources first, then Part 1 sinks, then Part 2 sinks — the same numbering the expected-output table uses |
| 5 | Kafka icons for Kafka sources and sinks | Topic glyph on all six topics; generators are plain boxes because they are not Kafka |
| 6 | Boxes name the operation | *Split*, *Aggregate*, *Join*, *Window* |

The one line crossing in the diagram — the price stream passing the
`positions.by-account` feed — is drawn as a hop so it reads as "passes over",
not "connects to".
