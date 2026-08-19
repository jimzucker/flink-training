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
| `BlockTrade` | `tradeId`, `symbol`, `side`, `quantity`, `allocations[]`, `eventTime` | one order, many allocations; `side` is BUY or SELL |
| `Allocation` | `account`, `subAccount`, `quantity` | a slice of a block trade |
| `Price` | `symbol`, `price`, `eventTime` | latest price per symbol |
| `Position` | key, `quantity`, `updatedAt` | running signed sum of allocation quantity |
| `MarketValue` | key, `quantity`, `price`, `marketValue`, `windowEnd` | `quantity × price at window close` |

`side` is applied as a sign: **BUY increases the position, SELL decreases it**.
Position is therefore a running signed sum, not a count, and a key whose sells
exceed its buys holds a legitimate negative (short) position.

The generator emits a **random mix of buys and sells**, because a demo where
everything is a buy proves nothing about the sign handling. Randomised does not
mean unverifiable — see [Determinism](#determinism): the generator is seeded, so
the mix is the same on every run and the expected net position per key is
arithmetic that can be asserted exactly.

Side affects **values only**. It changes the quantity on a key; it does not
change how many keys exist or how often they update, so every rate and key count
in the expected-output table below holds regardless of the buy/sell mix.

## Walkthrough

Numbered left to right, in the order the demo talks through them.

| # | Element | Key | Value |
|---|---|---|---|
| 1 | `orders` topic | `tradeId` | `BlockTrade` |
| 2 | `prices` topic | `symbol` | `Price` |
| 3 | `positions-by-symbol` topic | `symbol` | `Position` |
| 4 | `positions-by-account` topic | `acct/sub/sym` | `Position` |
| 5 | `mv-by-symbol` topic | `symbol` | `MarketValue` |
| 6 | `mv-by-account` topic | `acct/sub/sym` | `MarketValue` |

**Part 1 — positions.** Reads ①. *Split by allocation* turns one `BlockTrade`
into N `Allocation` records. The stream is then keyed two different ways off that
same split, and *Aggregate* maintains a running position per key. The two
aggregations run in parallel from one source, which is the point of the exercise:
the same input aggregated different ways at the same time. Writes ③ and ④.

**Part 2 — market value.** Reads ③, ④ and ②. *Join price × position* attaches the
latest price for the symbol to each position. *Window 1 min* is a tumbling
one-minute window that emits one market value per key per minute, computed as:

```
marketValue = position quantity at window close
            × last price for that symbol at or before window close
```

**Price at close**, not an average or a VWAP across the window. Both inputs are
taken as of the same instant — the window boundary — so the number is a snapshot
that can be reconciled against the position topic at that timestamp rather than
a blend of prices that matches nothing observable. Writes ⑤ and ⑥.

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
| 3 | `positions-by-symbol` | 10 / sec | **4** | one key per symbol |
| 4 | `positions-by-account` | 40 / sec | **16** | 10 trades × 4 allocations; 4 accounts × 4 symbols |
| 5 | `mv-by-symbol` | 4 / min | **4** | one emit per key per minute |
| 6 | `mv-by-account` | 16 / min | **16** | one emit per key per minute |

Sink 4 emits 4× the rate of sink 3 because each trade fans out to 4 allocations.
Sinks 5 and 6 emit once per minute per key regardless of input rate, because the
window collapses all updates in that minute to a single value.

## Determinism

The demo has to be reproducible and the numbers have to be explainable, so:

- **Generators are seeded.** The same seed produces the same trades — symbols,
  accounts, quantities and **buy/sell sides** — in the same order, so two runs
  produce byte-identical output. Randomised input and reproducible output are
  not in conflict: the randomness is a fixed, replayable sequence.
- **Expected results are computed, not observed.** Because the sequence is fixed,
  the expected net position for every key can be derived arithmetically from the
  seed and asserted exactly. A test that only checks "some number appeared" would
  not catch an inverted sign; asserting the exact signed quantity does.
- **The sign is tested in both directions.** Fixtures cover a key whose net is
  positive, one whose net is negative, and one that nets to zero.
- **Every key starts flat.** Positions open at zero; there is no opening book to
  load, so the state at any point is derivable from the trades alone.
- **Every key emits every minute.** A key with no trade during a minute still
  holds a position, so it still emits. That is what makes sinks 5 and 6 a steady
  4 and 16 per minute rather than a number that varies with activity.
- **Fixed reference data.** Exactly 4 symbols, 4 accounts, 4 allocations per
  trade — the numbers above are arithmetic, not statistics.
- **Event time, not processing time.** Windows key off an event timestamp stamped
  at the generator, so a slow consumer changes latency but never changes results.
- **Every record carries `tradeId`.** A single trade can be followed from ① through
  to ⑤ and ⑥ in the logs, so any number on screen can be traced back to its input.

## Ordering and watermarks

**Prices arrive in order, by construction.** The `prices` topic is keyed by
`symbol`, so every price for a symbol lands on one partition, and Kafka preserves
order within a partition. A single seeded generator emits them in event-time
order. There is no path by which a price for a symbol overtakes an earlier one,
so the windows use a **monotonically-increasing timestamp** watermark rather than
bounded out-of-orderness, and allowed lateness is zero. Nothing arrives late
because nothing can.

**Idleness is the real risk, not lateness.** Each join has two inputs and its
watermark advances at the slower of them. If an input goes quiet — a symbol with
no price for a while, or a partition with no traffic — the watermark stalls and
the one-minute windows stop firing, even though every record that did arrive was
perfectly in order. Sinks 5 and 6 would simply go silent, which looks identical
to a broken pipeline during a demo.

The fix is an **idle-source timeout**: after a short period with no records, a
partition stops holding the watermark back and lets the rest of the stream
advance. This is configuration, not semantics — it changes when a window fires,
never what it contains. Verified in step 05 by running with fewer active symbols
than partitions and confirming sinks 5 and 6 still emit on the minute.

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
`positions-by-account` feed — is drawn as a hop so it reads as "passes over",
not "connects to".
