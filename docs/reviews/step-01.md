# Step 01 — Pipeline design: review

## Round 1

### Asked

1. **Is `side` really signed?** If block trades are BUY-only this is moot; if SELLs
   exist and position should be a gross count, the design is wrong.
2. **Window semantics for sinks 5 and 6** — price at window close, or averaged
   across the window?
3. **Topic names** — keep these, or match an existing convention?
4. **Does the diagram read correctly?**

### Feedback

> 1  you have buy and sell trades(generator should randomize, but make it testable
> verifiable)  buy increases positioning and sell decreases position
> 2 price at close
> 3 what are names u used?
> 4 link to diagram for review?

Followed by:

> 3 names ise - instead of period for prefoxes

### Actions

| Feedback | Action |
|---|---|
| Buy and sell, randomized but verifiable | Generator emits a random buy/sell mix from a **seeded** sequence, so the mix is identical every run and expected net positions are computable arithmetic. Fixtures cover keys that net positive, negative and zero. Recorded that side affects values only — never key counts or rates |
| Buy increases, sell decreases | Confirmed as designed; stated explicitly in the design doc and shown on the diagram as `BUY +qty / SELL -qty` |
| Price at close | Window computes `position at close × last price at or before close`. Not an average or VWAP — an averaged price reconciles against nothing observable. Diagram operator boxes relabelled `MV @ close` |
| What are the topic names | Listed; then renamed on request |
| Use `-` instead of `.` for prefixes | `positions.by-symbol` → `positions-by-symbol`, and the same for the other three. 17 occurrences across the diagram, design doc and README |
| Link to the diagram for review | Published as a review page: https://claude.ai/code/artifact/65b81ff2-fe6f-4690-b323-3b8f256da462 |

### Outcome

_Pending round 2._
