# Interview answers

No human was available (clean-room run 55). As instructed, every one of the six
questions in SKILL.md §1 was answered with **the default the skill offers**,
copied verbatim below from `~/.claude/skills/scalable-flink-skill/SKILL.md`
(skill commit `e0e13f7`). Nobody approved these answers; they are the skill's
defaults, taken as given.

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

(The skill adds: "Taking the default means every order has exactly 4
allocations, one to each account / sub-account pair of question 3, so every
order emits 5 records.")

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
