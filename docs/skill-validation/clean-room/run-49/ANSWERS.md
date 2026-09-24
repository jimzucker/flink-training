# Interview answers

Clean-room run: no human was available. Every answer is the default the skill
offers with the question, copied verbatim from `SKILL.md` section 1 — except
question 6, where the answer is **Confluent** (the one thing this run exists to
test). Nobody approved these; see `ASSUMPTIONS.md` for what they left open.

## 1. What goes into the pipeline, and what comes out?

An order arrives with a unique id and symbol, order quantity and a
list of allocations; each allocation is keyed by account / sub-account and
carries a quantity. The pipeline maintains positions by
account+subaccount+symbol and by symbol. That input is the one scaled up to
drive the pipeline to capacity. A second input carries prices, keyed by
symbol and timestamp; the pipeline joins those to **each** of the position
outputs and emits a market value every 10 seconds for both — symbol /
position / price / market value, and account / sub-account / symbol /
position / price / market value. Positions themselves are published as they
change, one per input — only the market value is throttled.

## 2. Does one input produce more than one output?

One trade input produces 1 position per symbol and one position
per allocation. If the order has 4 allocations it emits 5 records. For the
market value we want to throttle it to a configurable interval defaulting to
10 seconds.

## 3. What are the keys, and how many distinct ones?

Two key spaces. Symbol, and account / sub-account / symbol.
4,096 symbols, 2 accounts and each has 2 sub-accounts.

Make sure the cardinality is realistic, as 4K symbols vs 4 will materially
impact the application design.

## 4. What has to be exactly right?

Positions and market values must be published in order. At the end
the positions, at both symbol and account / sub-account / symbol, must match
the input, and market values — **at both of those key levels** — must be
final position × latest price. Duplicates have to be handled and not double
counted, in all cases.

## 5. Which Flink API should we use?

DataStream.

## 6. Which Kafka do you want to use?

Confluent.
