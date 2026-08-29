# Demo runbook

The requirements are specific about how this goes: go left to right, do not jump
around, and explain inputs and outputs that match the example walked through
beforehand. This is that walkthrough.

## Before anyone is watching

```bash
source scripts/env.sh
mvn package -DskipTests
./scripts/demo.sh
```

`demo.sh` starts the stack, creates the topics and submits both jobs **with a
ten-second window**. The requirements specify one minute and the job defaults to
it; ten seconds is used for the demo purely so the market value sinks say
something within the first few seconds rather than after a minute of dead air.

**Say that out loud when you get to sinks ⑤ and ⑥.** It is a presentation choice,
not a difference in the calculation, and it is better volunteered than noticed.

Have these open, in this order, and leave them open:

| Tab | For |
|---|---|
| The pipeline diagram | the design, talked through first |
| <http://localhost:3000/d/flink-training/block-trade-pipeline> | the live numbers |
| <http://localhost:8081> | the running jobs, if anyone asks |
| A terminal | the trade trace |

**Check the dashboard draws on the machine you are presenting from.** A browser
extension was enough to stop Grafana drawing panels here, with no error anywhere —
the dashboard was perfect and the screen was blank. That is indistinguishable
from a broken pipeline in front of an audience.

## What to say, in order

### 1. The problem, then the design

Block trades in, positions out two ways, then market value. Walk the diagram left
to right using the numbers on it — ① through ⑥. Every number said out loud from
here on refers to one of those.

### 2. The expected output, before running anything

State what the run will produce, so the audience is checking a prediction rather
than being shown a result:

| Input | |
|---|---|
| Trades | 10 / sec |
| Symbols | 4 |
| Accounts | 4 |
| Allocations per trade | 4 |

| # | Sink | Rate | Unique keys |
|---|---|---|---|
| ③ | positions-by-symbol | 10 / sec | 4 |
| ④ | positions-by-account | 40 / sec | 16 |
| ⑤ | mv-by-symbol | once per key per window | 4 |
| ⑥ | mv-by-account | once per key per window | 16 |

Sink ④ is four times sink ③ because every block splits across four accounts. Say
that before it appears on screen.

### 3. Start it, then switch to Grafana

The stack comes up with the generators idle by default, so the dashboard is
visibly empty while the design is being explained. Then, on cue:

```bash
./scripts/start-generators.sh
```

Graphs going from flat to flowing while people watch is more persuasive than a
dashboard that was already busy when it appeared. If you would rather not touch a
terminal mid-demo, `GENERATOR_START=2` starts the data two minutes after the
stack comes up.

Switch to the dashboard and let it fill. Then read it left to right, which is how
it is laid out:

- ① orders settles at **10/sec**
- ③ positions-by-symbol settles at **10/sec** — one update per trade
- ④ positions-by-account settles at **40/sec** — one per allocation
- ③ and ⑤ show **4** unique keys; ④ and ⑥ show **16**

Sinks ⑤ and ⑥ step rather than flow. That is what once-per-key-per-window looks
like, and it is worth pointing at before someone asks — as is the fact that the
window is ten seconds here rather than the specified minute, so that they say
something while people are watching.

### 4. Explain any number on the screen

This is the part worth rehearsing. Take a trade — any trade — and walk it through:

```bash
./scripts/verify-run.sh        # or reuse an existing dump
./scripts/trace-trade.sh T000000098
```

It prints the block trade, the single position-by-symbol update it caused, the
four position-by-account updates, and the market values of the windows it closed
— and states the arithmetic tying them together:

```
the four accounts sum to -2800, which is what sink 3 reports  OK
the account market values sum to -207900.00, which is what sink 5 reports  OK
```

Offer this rather than waiting to be asked.

### 5. Scaling, if there is time

Not a live run — each case needs minutes of warm-up, which is dead air. Run it
beforehand and show two cases:

```bash
scripts/scale-units.sh          # 2 and 4 units, the demo pair
```

| units | orders/sec | vs previous | Flink cores | broker cores | back-pressure |
|---|---|---|---|---|---|
| 2 | 65,721 | — | 2.00 of 2 | 0.25 | 28.5% |
| 4 | **129,056** | **1.96×** | 3.94 of 4 | 0.39 | 51.6% |

A unit is one core and one degree of parallelism, bought together — a KPU in
Managed Service for Apache Flink. Two things to say:

1. **Double the units, double the throughput** — 1.96× of it, which is as close
   to linear as this gets.
2. **Flink used every core it was given**, 2.00 of 2 and 3.94 of 4, while the
   broker stayed under half a core. That is what makes the first point mean
   something: Flink is the constrained component, and you can only show that
   something scales when it is the thing that is constrained. Step 10 got this
   wrong — it varied parallelism while Flink already had every core it could
   use, and the answer came back flat.

Two cases, one claim, no dead air. The full curve is the backup slide below.

#### Backup: the whole curve, and where it stops

`UNITS="1 2 4 8" OUT=docs/steps/step-12/units.txt scripts/scale-units.sh` — only
if asked, or if there is real time:

| units | orders/sec | vs previous | Flink cores | broker cores | back-pressure |
|---|---|---|---|---|---|
| 1 | 30,505 | — | 1.00 | 0.10 | 24.1% |
| 2 | 65,721 | 2.15× | 2.00 | 0.25 | 28.5% |
| 4 | 129,056 | 1.96× | 3.94 | 0.39 | 51.6% |
| 8 | 151,969 | **1.18×** | 4.98 | 0.48 | 72.7% |

**It stops at eight, and the broker is at 0.48 cores while being the thing in the
way.** It has run out of write throughput, not CPU: 151,969 orders/sec is 759,845
records/sec against the ~750,000 one broker accepts, because each order becomes
five records. A broker that looks idle can still be the ceiling, and the only way
to tell is to have both CPU figures beside the throughput.

**If asked whether that is Flink's ceiling or the laptop's** — the laptop's. The
same script against a two-broker MSK cluster returns 1.73× for that last doubling
instead of 1.18×, with Flink using 7.99 of its 8 cores instead of 4.98. Nothing
about the job changed, only what was in its way. That run was a confirmation
rather than a demo; its step ratios behave oddly in a way that is not yet
explained, and its two highest points were measured once each.

### 6. If someone asks something you cannot answer

Say so, and take it as an action item to change the logging until you can. That
is the instruction in the requirements, and it is better than an explanation
invented on the spot.

## Questions worth having an answer ready for

**Why does sink ④ update four times as often as ③?**
Every block trade splits across four accounts. The quantities reconcile — the
four accounts sum to the block — but the update counts differ by design.

**Why do the market value sinks look like steps?**
They emit once per key per window, not continuously.

**Why is the window ten seconds and not a minute?**
Only for the demo, so the sinks say something without a minute of waiting. The
job defaults to the specified minute; `demo.sh` overrides it. The calculation is
identical either way, and the verification runs against both.

**Why did a number appear a moment late?**
Delivery is exactly-once, so a record is not readable until the checkpoint that
produced it commits. That is a floor under latency, and it is deliberate: a
position is a running sum, and a replayed record would be a wrong number rather
than a duplicate.

Have the numbers ready. The pipeline itself takes about 60ms at the median and
110ms at p99, which is on the dashboard. What a consumer waits for is about 2.5s
at the median with a five-second checkpoint interval, and about 0.5s with a
one-second interval — five times shorter, five times lower, with the maximum
landing within one interval each time. Offering that comparison answers the
question before it becomes an argument.

```bash
./scripts/measure-latency.sh
```

**Why does the dashboard show an aborted checkpoint?**
One per job, at startup. With a one-second interval the first checkpoint triggers
before every task is running and is aborted; the count then stays put while
completed checkpoints climb. A count that keeps rising during a run would be the
thing to worry about.

**Are the positions ever negative?**
Yes. Buys add and sells subtract, so a key whose sells exceed its buys holds a
short. Seeing negatives is evidence the sign handling is live.

**Is this reproducible?**
Yes, and it is checked rather than claimed. `scripts/verify-run.sh` asserts every
figure above with no tolerances, including recomputing each window's closing
price and quantity independently from the topics that fed them.
