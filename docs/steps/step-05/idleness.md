# Step 05 — idleness, and proof that the setting matters

Step 01's review identified idleness, not lateness, as the real watermark risk:
a join advances at its slower input, so a partition with no traffic stalls the
watermark and the windows stop firing — with every record that did arrive
perfectly in order. Sinks 5 and 6 simply go quiet, which during a demo is
indistinguishable from a broken pipeline.

It is not a hypothetical. With four partitions and four symbols, hashing leaves
partitions empty in an ordinary run:

```
prices:0:0      positions-by-symbol:0:0
prices:1:600    positions-by-symbol:1:145
prices:2:200    positions-by-symbol:2:58
prices:3:0      positions-by-symbol:3:0
```

Two of four partitions hold nothing on both topics.

## The two runs

Identical input, identical everything, one setting changed:

| | `IDLENESS_MS=5000` | `IDLENESS_MS=0` |
|---|---|---|
| positions-by-symbol | 200 | 200 |
| positions-by-account | 800 | 800 |
| **mv-by-symbol** | **12** | **0** |
| **mv-by-account** | **48** | **0** |

Part 1 is unaffected either way, because it does not window. Part 2 produces
nothing at all without idleness — no error, no failed job, no restart. The job
sits in RUNNING and emits silence.

That is why the setting has an off switch: a guarantee nobody has seen fail is
indistinguishable from one that was never needed.


## A verification bug this run exposed

The first CI run of this step failed, reporting `positions-by-account: 0` where
800 records existed, and sinks 5 and 6 as empty. Neither was true.

Every topic was being read by its own console consumer with a fixed timeout, and
on a two-core runner competing with two Flink jobs those consumers routinely hit
the timeout before receiving anything. A timed-out read returns zero records,
which is **the same answer as an empty topic** — so the verification reported
missing data whenever the machine was slow, and the failure looked exactly like a
pipeline bug.

It is now read by `TopicDump`, which fetches the end offsets first and consumes
until every partition reaches them. Completion is then a fact rather than a
guess, and a read that falls short says so and stops:

```
a topic could not be read to its end offsets; the checks below would be
reporting a read failure as missing data, so stopping here instead.
```

One process reads all six topics, which also removed a JVM start per topic.
Under `read_committed` the end offset is the last stable offset, so transaction
markers are handled without having to reason about them.


## Verifying windows without faking the clock

An earlier version of this step verified the windows by compressing event time:
records were emitted in two seconds while being stamped a second apart, so three
minutes of event time passed almost instantly. That tests the window arithmetic
but never exercises the mode the demo runs in, and the timestamps correspond to
nothing that happened.

The verification now runs the generator exactly as the demo does — wall-clock
event times, paced in real time, at the demo rates — and shortens the **window**
instead, from sixty seconds to ten. The window length is already a runtime
parameter, so this is the same code on the same clock, and the demo keeps its
minute.

The cost is that the number of windows is no longer predictable: it depends on
where the run happens to start relative to a boundary. That is a property of a
real clock rather than a defect, so the count is read from the data and only
floored. What stays exact is everything inside the windows.

### Partial windows

Three boundary cases arise once the clock is real, and each is handled rather
than avoided.

| Case | Behaviour |
|---|---|
| The first window is partial | Still emits. Market value is a snapshot at the close, not a rate, so partial coverage does not distort it |
| The last window never closes | Correct: no watermark reaches its boundary. It is why the window count is read from the data |
| Keys first appear in different windows | A key emits from the first boundary after it is seen, so early keys have more windows than late ones |

That last case broke the original check, which asserted `records == keys x
windows`. With 16 account keys appearing at different moments, one key starting a
window late makes the true answer 63 rather than 64, and the check would have
called correct output a failure. It now asserts the properties that must hold
regardless: no key skips a window once it starts, no key stops early, and there
is exactly one record per key per window it took part in.


## The gap that was hiding a bug

Prices were checked against the price topic, but quantities were not checked
against anything. The quantity half of every market value rested on the job
agreeing with itself.

Adding the check — for each market value, the last position for that key strictly
before the window close, recomputed from the position topic — failed immediately:
**14 of 16** on sink 5 and **56 of 64** on sink 6.

By hand, for AAPL at the first window close:

```
positions before the boundary   ... 1787406209755  -2400
                                    1787406209853  -2800   <- last before close
market value reported                              -3600
```

The reported quantity came from a position recorded *after* the window closed.

The cause is the one already fixed for prices, missed for positions.
`processElement` overwrote the quantity on arrival, and **watermarks are emitted
periodically rather than per record** — by default every 200ms — so several
positions from after a boundary are routinely processed before the timer for that
boundary fires. The state then held a position from the window's own future.

Positions are now recorded against the window they fall in, exactly as prices
are, and a window closes against its own. A window with no trade inherits the
previous one, which is what lets a quiet key keep reporting the position it still
holds.

Both directions now reconcile against the topics that fed them:

```
sink 5 quantity differs from the position topic   0
sink 6 quantity differs from the position topic   0
price at close differs from the price topic       0
```

The lesson is the one the assignment keeps making: a number that reconciles
against nothing is not a verified number, however plausible it looks. Market
values had been arithmetically consistent — `quantity x price` always matched —
while the quantity itself was wrong.


## A wrong assumption, caught by CI

Step 01's review concluded that nothing can arrive late, because a key's records
occupy one partition and Kafka preserves order within it. That is true of prices
and **false of positions**, and the difference is easy to miss: `orders` is keyed
by `tradeId`, not by symbol, so trades for one symbol are spread across
partitions and Part 1 merges them. Each partition it reads is ordered; what it
emits is not.

Part 2 was reading those streams with a monotonic-timestamp watermark, which
fires window timers as soon as any later timestamp appears. A position arriving
after its window had closed was then missed, and the window reported the position
as of slightly the wrong instant.

It passed on a development machine and failed on a slower CI runner — the
signature of a race rather than an arithmetic fault. Two changes:

- the position streams allow a bounded amount of out-of-orderness
- a window closes on the position with the greatest event time within it, rather
  than the last one to arrive

The check that caught it is the quantity reconciliation added one step earlier,
which had itself been added to close a gap. Without it, the only symptom would
have been a market value that was slightly wrong and entirely plausible.
