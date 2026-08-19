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

## Round 2

### Asked

1. Allowed lateness on the one-minute window — drop a late price, or emit a correction?
2. Keys with no activity still emit, keeping the count a steady 4 and 16 per minute?
3. Every key starts flat at zero?

### Feedback

> 2 yes
> 3 correct
> 1  we must process prices in order are we not, quest does not make sense

### Actions

| Feedback | Action |
|---|---|
| Question 1 does not make sense; prices are processed in order | **Correct, and the question was wrong.** `prices` is keyed by symbol, so each symbol lands on one partition and Kafka preserves order within it; a single seeded generator emits in event-time order. No price can overtake an earlier one. Design now states monotonically-increasing-timestamp watermarks and zero allowed lateness, because nothing can arrive late |
| — | The real risk was misidentified as lateness. It is **idleness**: each join advances at its slower input, so a quiet partition stalls the watermark and the windows stop firing, with every record perfectly in order. Handled with an idle-source timeout — configuration, not semantics; it changes when a window fires, never what it contains. Verification added to step 05: run with fewer active symbols than partitions, confirm sinks 5 and 6 still emit on the minute |
| Keys with no activity still emit | Confirmed and documented — this is what makes sinks 5 and 6 a steady 4 and 16 per minute rather than a count that varies with activity |
| Every key starts flat | Confirmed and documented — no opening book, so state at any moment is derivable from the trades alone |

### Outcome

Approved. Squash-merged to `main`, tagged `step-01`.
