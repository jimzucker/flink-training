# The interview, answered

No human was available for this run. Section 1 of the skill says that when
there is nobody to ask, the six questions are answered by whoever is running
and written down as stated assumptions. **This run took the skill's own
offered default for every one of the six**, copied word for word out of
`SKILL.md` section 1. Nothing here was agreed by a person.

The defaults are reproduced verbatim. My reading of each — what I am
actually going to build from it — follows underneath, marked as mine.

---

## 1. What goes into the pipeline, and what comes out?

> *Default: an order arrives with a unique id and symbol, order quantity and a
> list of allocations; each allocation is keyed by account / sub-account and
> carries a quantity. The pipeline maintains positions by
> account+subaccount+symbol and by symbol. That input is the one scaled up to
> drive the pipeline to capacity. A second input carries prices, keyed by
> symbol and timestamp; the pipeline joins those to **each** of the position
> outputs and emits a market value every 10 seconds for both — symbol /
> position / price / market value, and account / sub-account / symbol /
> position / price / market value. Positions themselves are published as they
> change, one per input — only the market value is throttled.*

**What I read that as (mine).** Two inputs and four outputs.

| topic | who owns it | why |
|---|---|---|
| `orders` | the harness | the one input filled, measured and drained |
| `prices` | me | a second input; the harness is told nothing about it |
| `positions-by-symbol` | the harness | grows by a constant multiple of `orders` |
| `positions-by-account` | the harness | same |
| `market-values-by-symbol` | me, declared in `topicsAlsoWritten` | emitted on a timer, not per input |
| `market-values-by-account` | me, declared in `topicsAlsoWritten` | same |

Two position outputs and two market values. The prices reach both
aggregations by broadcast, because the account side is keyed on
account / sub-account / symbol and cannot be joined to a symbol-keyed price
stream by key at all.

## 2. Does one input produce more than one output?

> *Default: one trade input produces 1 position per symbol and one position
> per allocation. If the order has 4 allocations it emits 5 records. For the
> market value we want to throttle it to a configurable interval defaulting to
> 10 seconds.*

**What I read that as (mine).** Every order carries exactly 4 allocations, so
every order writes 1 row to `positions-by-symbol` and 4 rows to
`positions-by-account`: `outputsPerInput` is 5. The two market values are
emitted every 10,000 ms per key and are counted in neither that number nor
`topics.out`.

## 3. What are the keys, and how many distinct ones?

> *Default: two key spaces. Symbol, and account / sub-account / symbol.
> 4 symbols, 2 accounts and each has 2 sub-accounts.*

> *Make sure the cardinality is realistic, as 4K symbols vs 4 will materially
> impact the application design.*

**What I read that as (mine).** 4 symbol keys and 2 x 2 x 4 = 16
account / sub-account / symbol keys. Both key sets are named in `keySets` so
preflight can ask Flink where they land, because 4 keys do not spread over 4
subtasks by themselves.

## 4. What has to be exactly right?

> *Default: positions and market values must be published in order. At the end
> the positions, at both symbol and account / sub-account / symbol, must match
> the input, and market values — **at both of those key levels** — must be
> final position × latest price. Duplicates have to be handled and not double
> counted, in all cases.*

**What I read that as (mine).** The verifier asserts, with no tolerances:
every key's published values never go backwards on the clean drain and at
most once per key on the killed one; the final position per key equals the
generator manifest exactly at both key levels; the two key levels sum to the
same total; every key lands in exactly one partition; and the last market
value per key equals that key's final position times its latest price, at
both key levels.

## 5. Which Flink API should we use?

> *Default: DataStream.*

## 6. Which Kafka do you want to use?

> *Default: Apache.*

**What I read that as (mine).** `apache/kafka:3.9.0`, with
`images.kafkaLibs` pointing at `/opt/kafka/libs` so the harness can mine the
broker image for its client jar.
