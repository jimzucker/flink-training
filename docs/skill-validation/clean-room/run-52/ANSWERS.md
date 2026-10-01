# Interview answers

No human was available (clean-room run 52). SKILL.md §1 says: when there is
nobody to ask, answer the six questions yourself and write the answers down.
The brief for this run said to take the offered default for every question.
Each answer below is the default, copied verbatim from
`~/.claude/skills/scalable-flink-skill/SKILL.md` §1. Nobody approved them.

## 1. What goes into the pipeline, and what comes out?

*Default: an order arrives with a unique id and symbol, order quantity and a
list of allocations; each allocation is keyed by account / sub-account and
carries a quantity. The pipeline maintains positions by
account+subaccount+symbol and by symbol. That input is the one scaled up to
drive the pipeline to capacity. A second input carries prices, keyed by
symbol and timestamp; the pipeline joins those to **each** of the position
outputs and emits a market value every 10 seconds for both — symbol /
position / price / market value, and account / sub-account / symbol /
position / price / market value. Positions themselves are published as they
change, one per input — only the market value is throttled.*

## 2. Does one input produce more than one output?

*Default: one trade input produces 1 position per symbol and one position
per allocation. If the order has 4 allocations it emits 5 records. For the
market value we want to throttle it to a configurable interval defaulting to
10 seconds.*

## 3. What are the keys, and how many distinct ones?

*Default: two key spaces. Symbol, and account / sub-account / symbol.
4,096 symbols, 2 accounts and each has 2 sub-accounts.*

*Make sure the cardinality is realistic, as 4K symbols vs 4 will materially
impact the application design.*

## 4. What has to be exactly right?

*Default: positions and market values must be published in order. At the end
the positions, at both symbol and account / sub-account / symbol, must match
the input, and market values — **at both of those key levels** — must be
final position × latest price. Duplicates — a record delivered twice after a
failure, when a restart replays it — are counted once.*

## 5. Which Flink API should we use?

*Default: DataStream.*

## 6. Which Kafka do you want to use?

*Default: Apache.*

## What the answers fix, in numbers

| | value |
|---|---|
| inputs | 2: `orders` (scaled, owned by the harness) and `prices` (the pipeline's own) |
| outputs per input (counted fan-out) | 5: 1 position by symbol + 4 positions by account / sub-account / symbol |
| throttled outputs (not fan-out) | 2: market value by symbol, market value by account / sub-account / symbol, every 10 s |
| distinct symbol keys | 4,096 |
| distinct account / sub-account / symbol keys | 2 × 2 × 4,096 = 16,384 |
| API | DataStream |
| Kafka | Apache, `apache/kafka:3.9.0` |
