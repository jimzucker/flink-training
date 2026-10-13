# Interview answers

Clean-room run 54. No human was available, so every answer below is the
default the skill offers with the question (`~/.claude/skills/scalable-flink-skill/SKILL.md`, section 1),
copied verbatim. Nobody approved them; what they leave open is in `ASSUMPTIONS.md`.

## 1. What goes into the pipeline, and what comes out?

> Default: an order arrives with a unique id and symbol, order quantity and a
> list of allocations; each allocation is keyed by account / sub-account and
> carries a quantity. The pipeline maintains positions by
> account+subaccount+symbol and by symbol. That input is the one scaled up to
> drive the pipeline to capacity. A second input carries prices, keyed by
> symbol and timestamp; the pipeline joins those to **each** of the position
> outputs and emits a market value every 10 seconds for both — symbol /
> position / price / market value, and account / sub-account / symbol /
> position / price / market value. Positions themselves are published as they
> change, one per input — only the market value is throttled.

## 2. Does one input produce more than one output?

> Default: one trade input produces 1 position per symbol and one position
> per allocation. If the order has 4 allocations it emits 5 records. For the
> market value we want to throttle it to a configurable interval defaulting to
> 10 seconds.

## 3. What are the keys, and how many distinct ones?

> Default: two key spaces. Symbol, and account / sub-account / symbol.
> 4,096 symbols, 2 accounts and each has 2 sub-accounts.

## 4. What has to be exactly right?

> Default: positions and market values must be published in order. At the end
> the positions, at both symbol and account / sub-account / symbol, must match
> the input, and market values — **at both of those key levels** — must be
> final position × latest price. Duplicates — a record delivered twice after a
> failure, when a restart replays it — are counted once.

## 5. Which Flink API should we use?

> Default: DataStream.

## 6. Which Kafka do you want to use?

> Default: Apache.

## What the answers fix, in one table

| | |
|---|---|
| inputs | 2: `orders` (scaled, filled and measured by the harness) and `prices` (the pipeline's own) |
| outputs per input | `positions-by-symbol` and `positions-by-account` — one record per input to each key it touches |
| throttled outputs | `market-values-by-symbol` and `market-values-by-account` — every 10 seconds per changed key |
| fan-out | 5 per order (1 symbol position + 4 account positions), market values not counted |
| distinct keys | 4,096 symbols; 2 accounts × 2 sub-accounts × 4,096 symbols = 16,384 account keys |
| API / Kafka | DataStream / Apache Kafka (`apache/kafka:3.9.0`) |
