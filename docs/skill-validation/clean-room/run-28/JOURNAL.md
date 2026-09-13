# Journal

## Step 0 — the interview that did not happen

The skill says to interview one question at a time before building. There is no
human available, so here are the questions, and the assumption taken in place of
each answer. Everything after this is judged against them.

| # | question | assumption taken |
|---|---|---|
| 1 | What is the input event, and what comes out? | A **block trade** — one symbol, one side, one quantity, and the four accounts it is to be allocated to with their target weights. Out: a running position for the symbol, a running position for each (account, symbol), and, every ten seconds of event time, the market value of each symbol and of each account at the window close. |
| 2 | Does one input become several outputs? | **5 per trade**, exactly: one symbol position update and four account position updates. Window output is a further 24 records (8 symbols + 16 accounts) per 10 s window — 0.03% of the stream at this record density, so `outputsPerInput` is 5. Fan-out lands on the write side; the broker gets five records for every one it serves. |
| 3 | What are the keys, and how many distinct ones? | 8 symbols, 16 accounts, 4 accounts named per block → **8** symbol keys, **128** (account, symbol) keys, **16** account keys for the roll-up. Small and fixed, so every output is arithmetic and can be asserted exactly. |
| 4 | What has to be exactly right? | Every position is a running sum, so a replayed record is a wrong number, not a duplicate. Two settings: **exactly-once checkpointing** for the keyed state, **at-least-once sink**, made idempotent by emitting the *absolute* position per key and the *absolute* market value per key per window. Tested by killing the worker mid-drain. |
| 5 | Who watches, and what must they believe? | Engineers: correctness first (the completeness run), capacity second. |
| 6 | Where does it run? | This laptop, in Docker, everything prefixed `mvs29`. |
| 7 | What claim? | *"On one worker, this block-trade position and market-value pipeline scales linearly with cores: doubling the cores and the parallelism doubles the trades per second."* |
| 8 | Which axis? | **One worker growing** — one task manager container capped at N cores, parallelism N, N slots. Not workers multiplying. |
| 9 | Which API level? | **DataStream, hand-written operators.** No SQL, no Table API — asked for explicitly. |

Further assumptions, taken without an answer:

- **"Position by account" is a position in a symbol.** An account holding four
  symbols has four positions, not one. Market value is then well defined per
  (account, symbol) and is rolled up to an account total, which is what the
  `mv-by-account` topic carries.
- **Prices are a second Kafka topic**, one price per symbol per second of event
  time, produced by the same generator. It is not the topic the throughput is
  read from — rate is committed offsets on the block-trade topic only.
- **Market value at close** means: the position after every record with an event
  timestamp inside the window, times the last price for that symbol at or before
  the window's close instant. Implemented with an event-time timer on the last
  millisecond of the window, so both are read at the same instant. Not an
  average, not a VWAP.
- **The window is event time**, driven by timestamps in the records, so the
  number of window outputs is a function of the input and can be asserted
  exactly. Processing time would make it a function of how fast the drain ran.
- Cases **1, 2 and 4 cores** with baseline 1, because the task asks for that
  curve. The skill would otherwise drop the one-unit case as the structurally
  weakest.

## Step 1 — build

`job/` is the pipeline: `Domain` (instruments, wire format, the exact integer
split, the clock), `MarketValueJob` (the graph), `GenerateBacklog` (deterministic
producer + the manifest of expected answers), `VerifyCompleteness` (the
assertions).

Decisions worth writing down:

- **Key values are chosen, not natural.** Eight symbols hashed straight into
  Flink's 128 key groups do not land 2-2-2-2 across four subtasks. The busiest
  subtask would then set the pace and the four-core case would sit below its cap
  — a scaling shortfall with nothing to do with scaling. `balancedKeys()` picks
  an integer per logical key so key *i* lands in key group *i x (128/n)*: an
  exact round robin at parallelism 1, 2 and 4. A bijection, so nothing computed
  changes.
- **Every case has the same graph.** Five vertices, every edge into a keyed
  operator a HASH shuffle, at parallelism 1 as much as at 4. The skill's warning
  about a chained baseline (211,533 vs 140,308 rec/s on the same job) does not
  apply because there is no version of this graph that chains.
- **The span of the backlog lands mid-window.** `count / partitions` is chosen
  = 10,000k + 5,000 ms so the last window boundary is 5 seconds away from the
  final watermark. Which windows close is then not a knife-edge question about
  whether `forMonotonousTimestamps` emits `maxTs` or `maxTs - 1`.

## Step 2 — three defects, each found by measuring

Everything below was found before any table existed, on a 440,000-record smoke
drain and three short rate probes. None of it is in the published numbers except
as the build they were measured on.

**1. The price stream raced the trade stream.** First smoke drain: positions
exact, key sets exact, window *count* exact — and every market value wrong.
`mv-by-symbol AAPL` at the first window close read 14,515,613,329 against an
expected 1,936,440,975, and the last three windows all read the same number. The
price topic is 2,000x smaller than the trade topic, so its reader finishes in one
breath; a price applied on arrival is the last price, not the price at the close.
The same argument applies to quantity: a watermark says "nothing further at or
below W", not "the reader has stopped", and in a drain event time runs about 60x
wall clock, so the running total when a close timer fires is seconds of event
time past the close.

Fixed by making both a genuine event-time snapshot: quantities accumulate as a
delta per window close and are folded in when that close fires; prices are held
by their own close instant and the last one at or before the close is chosen when
it fires. Neither is applied on arrival. Re-verified: exact, including the
windowed outputs.

**2. Watermark alignment cost half the throughput.** The first fix needed a bound
on how much price history each key holds, and `withWatermarkAlignment` is the
mechanism designed for it. Measured, one rig, one build, one variable — the
alignment drift, made a job argument so the build did not change:

| 4 cores | rec/s | worker | source idle |
|---|---:|---:|---:|
| drift 30 s | 162,526 | 1.10 of 4 (27%) | 88.4% |
| drift effectively off | drained 24 M inside the warm-up | — | — |

An 88% idle source is the harness's own "starved, not overwhelmed" signature and
would have been refused. Alignment was removed and the bound came from somewhere
else: the symbol operator compresses prices to one entry per window on arrival
(only the last price of a window can be that window's close price, and prices for
one symbol arrive in order), and it hands the account keys the price *at the
close* — 16 records per symbol per window, in step with the close — instead of
fanning every raw price out to 128 keys.

**3. Two things that were not the cause.** With the source 24% back-pressured and
the worker at 82.6% of a 4-core cap, the obvious stories were garbage collection
and checkpointing. Measured instead: GC 1.46% of capacity, checkpoints avg 90 ms,
max 230 ms, state 77 kB. Neither. The real cause was that the probe's backlog ran
out inside the measurement window — the rate collapses as the last records drain
and takes the cap fraction with it. On a backlog with 26 M records still to go,
the same build read **853,652 rec/s at 99.1% of cap**. This is why the harness
refuses a case whose backlog lacks a checkpoint interval of headroom at close.

**4. Allocation, and the one-core collector.** The output path built a String key
("A07|NVDA") and a String value for every one of the five records per trade.
Replaced with a precomputed key byte array per logical key and digits written
straight into a buffer. Measured on the same probe, 4 cores: 549,656 -> 743,088
rec/s.

At one core the JVM sees one CPU and picks the *serial* collector, and GC then
scales with how little heap it has. Measured, one variable (`caps.tmMemoryBase`),
same build:

| worker memory at 1 core | GC, harness measure | rec/s |
|---|---:|---:|
| 1,408 m | 22.6% | 184,398 |
| 2,560 m | 12.3% | 162,785 |
| 3,584 m | **8.15%** | 227,843 |

The harness refuses a case whose GC exceeds 11% of capacity as a memory ceiling,
and at 1,408 m and 2,560 m it would have been right to. Settled on
`tmMemoryBase 3072m + tmMemoryPerCore 512m`, which leaves the four-core case at
the 4,096 m it already measured 99-100% of cap on and gives the one-core case
enough to keep the serial collector out of the way.

## Step 3 — the chain, three attempts

`prove.py all --quick`, run cold each time.

| attempt | start | outcome | cost |
|---|---|---|---:|
| 1 | 21:22 | **FAIL at completeness** — killed-worker arm wrote 672 of 3,600 window records | 10.2 min |
| 2 | 21:34 | **FAIL at tiny proof** — the fill wrote 151,268,997 of 160,040,000 records | 12.9 min |
| 3 | 21:50 | **suite clean; `report` FAIL because 2→4 misses the claim floor** | 52.1 min |

**Attempt 1** is the kill test paying for itself. Positions came through the kill
exact — 8 and 128 replay rewinds, no gap, every final quantity equal to the
manifest — and market value stopped dead at the kill. The price topic is 2,000x
smaller than the trade topic and is read to its end in the first second, so the
replacement worker resumed that source at its end offset with nothing to emit,
its watermark restarted at `Long.MIN_VALUE`, and `min(trades, prices)` never
advanced again. `withIdleness(15 s)` on the price source. Nothing about the clean
drain would ever have shown this.

**Attempt 2** is the generator's own read-back paying for itself. Nine producers
in one JVM at 128 MB of buffer each is 1.15 GB against `-Xmx1g`; the
`OutOfMemoryError` is an `Error`, `catch (Exception)` missed it, and a producer
thread died in silence with 8,771,003 records unsent. Bounded the buffers to
32 MB, caught `Throwable`, added an uncaught-exception handler, raised the heap,
and re-tested a 160 M fill on its own before spending another chain on it — it
then filled in 86 s where the broken version had taken 200 s to fail.

**Attempt 3.** Completeness passed both arms for build `437a14a2910b1df8`; the
tiny proof read 1c 185,637 and 4c 749,620 rec/s, both at 99.8% of cap, ratio
4.04x inside the 3.0–5.0 bound, and its self-test fired all 20 guards. It also
reaped the `tail -f` and the wait loop I had running against `results/` — which is
the rule working, and the skill says so in as many words. The suite ran seven
cases in 29.7 minutes with every case at 96–100% of cap and no ceiling.

Result: **1→2 = 2.131x, interval [1.975, 2.329], meets the claim. 2→4 = 1.909x,
interval [1.779, 2.038], does not** — the point estimate is 95.4% of linear and
the interval's lower bound is 89.0%, and the claim is judged on the bound. Two
passes cannot separate 95% from 89%; three might. See `REPORT.md`.
