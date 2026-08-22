# Journal

A record of each step: what drove it, what was decided, how it was verified, and
what came out of its review.

Steps follow the deliverable order the assignment lists. Each is developed on its
own branch, reviewed, reworked if the review calls for it, then squash-merged to
`main`. Branches are kept so the working history stays inspectable.

| Step | Deliverable | Local stack after this step | Branch | Status |
|---|---|---|---|---|
| 00 | Scaffold | — | `step-00-scaffold` | done |
| 01 | Pipeline design | — | `step-01-pipeline-design` | done |
| 02 | Generators | Kafka | `step-02-generators` | in review |
| 03 | CI | Kafka | `step-03-ci` | done |
| 04 | Part 1 — positions | Kafka, Flink | `step-04-part1-positions` | in review |
| 05 | Part 2 — market value | Kafka, Flink | `step-05-part2-marketvalue` | in review |
| 06 | Observability | Kafka, Flink, Prometheus, Grafana | `step-06-observability` | in review |
| 07 | Correctness demo | full | `step-07-correctness-demo` | not started |
| 08 | Local Docker demo | full | `step-08-docker-local` | not started |
| 09 | Latency | full | `step-09-latency` | not started |
| 10 | Scale | full | `step-10-scale` | not started |
| 11 | AWS | full | `step-11-aws` | not started |
| 12 | Final demo | full | `step-12-final-deck` | not started |

The local Docker stack is stood up early and grown one service at a time, rather
than assembled whole near the end. Everything runs against real Kafka and a real
Flink cluster from the first line of code, so no wiring is built twice.

The assignment lists its deliverables as demo milestones, not as a build order.
Both orderings satisfy the same milestones in the same sequence; this one puts
the infrastructure in place before the step that needs it. In particular
"demo numbers are correct using Grafana metrics" (step 07) cannot run before
Grafana exists, so observability (step 06) has to precede it either way.

CI lands at step 03: after the first module with tests exists, and before the
Flink jobs it needs to guard.

---

## Step 00 — Scaffold

- **Branch:** `step-00-scaffold`
- **Prompt:** Set up the project from the assignment, delivered step by step in the
  order the deliverables are listed. Step 0 is the scaffold: parent POM,
  `.gitignore`, `JOURNAL.md`, `docs/` layout, README skeleton.

### Approach

Build only what later steps need, and make the version choices explicit now so
they are not relitigated later.

- **Flink 1.20.4, Java 17.** Flink 1.20 is the last 1.x LTS. The deciding factor
  is step 9: AWS Managed Service for Apache Flink runs the 1.x line, so choosing
  Flink 2.x would strand the AWS deliverable.
- **Parent POM carries no modules yet.** Each module arrives with the step that
  needs it, so the build always reflects what actually exists.
- **JDK guard in the build.** This machine's default JDK is 25; Flink 1.20 does
  not run on it. The enforcer plugin fails immediately with an instruction rather
  than letting it surface as a stack trace inside Flink.
- **Jar and image versions held together.** `flink:1.20.5` is on Docker Hub but
  1.20.5 is not on Maven Central, so both are pinned to 1.20.4.

### Commands

```bash
mvn validate                        # with default JDK 25 -> fails by design
source scripts/env.sh && mvn validate   # with Java 17 -> passes
mvn dependency:get -Dartifact=<each pinned Flink artifact>
```

### Results

| Check | Outcome |
|---|---|
| `mvn validate` on JDK 25 | Fails with `Flink 1.20 requires Java 17. Run: source scripts/env.sh` |
| `mvn validate` on JDK 17 | Passes, exit 0 |
| Resolve `flink-streaming-java:1.20.4` | OK |
| Resolve `flink-clients:1.20.4` | OK |
| Resolve `flink-connector-kafka:3.4.0-1.20` | OK |
| Resolve `flink-metrics-prometheus:1.20.4` | OK |
| Resolve `flink-test-utils:1.20.4` | OK |
| Docker image `flink:1.20.4-java17` | Exists on Docker Hub |

Every pinned version was resolved against the real repository rather than assumed.

### Verification

The JDK guard is verified in both directions — it fails on the wrong JDK and
passes on the right one. A guard only tested in the passing direction would not
be evidence of anything.

### Review

Round 1 feedback confirmed Flink 1.20, the `groupId`, and the scala-free image;
asked that `.claude/` not be tracked, that changes be pushed, and that CI be
added to the plan.

- `.claude/` added to `.gitignore`
- `main` and `step-00-scaffold` pushed to origin
- CI added as step 03; former steps 03–10 shifted to 04–11

Full exchange: [`docs/reviews/step-00.md`](docs/reviews/step-00.md)

**Outcome:** approved, squash-merged to `main`, tagged `step-00`.

---

## Step 01 — Pipeline design

- **Branch:** `step-01-pipeline-design`
- **Prompt:** Produce the pipeline design: a diagram meeting all six of the
  assignment's diagram rules, the sink numbering that the verification table and
  Grafana panel order will both key off, and the design write-up.

### Approach

- **Hand-authored SVG, not Mermaid.** The assignment asks for Kafka icons,
  numbered badges in reading order, and `K:`/`V:` labels on every edge. Mermaid
  renders none of those cleanly. SVG also scales into the final deck without
  re-drawing.
- **Numbering follows the assignment's own table.** It already calls the outputs
  sinks 3, 4, 5 and 6, which fixes 1 and 2 as the two sources. Sources first,
  then Part 1 sinks, then Part 2 sinks — the same order the demo talks through.
- **One numbering, three artefacts.** The diagram badges, the expected-output
  table and the eventual Grafana panel order all use ①–⑥, so a number on a
  dashboard can be pointed at on the diagram without translation.
- **Composite key shortened to `acct/sub/sym`** on the diagram, defined in the
  legend and spelled out in the design doc. At full length it collided with the
  Part 1 boundary and the ④ badge.

### Commands

```bash
python3 -c "import xml.dom.minidom as m; m.parse('docs/design/pipeline.svg')"
python3 -m http.server 8791 --bind 127.0.0.1     # serve for rendering
# rendered and screenshotted in Chrome at each revision
```

### Results

The diagram went through four rendered revisions. Rendering it rather than
trusting the markup is what caught every one of these:

| Revision | Problem found by rendering | Fix |
|---|---|---|
| 1 | Kafka glyph read as a 4-point asterisk, not a topic | Redrew as a 3-node graph |
| 1 | `K:`/`V:` labels collided with topic glyphs and names | Centred labels on edge midpoints, widened gaps |
| 1 | Price riser tangled with the position feeds | Re-routed under the diagram, added a line hop |
| 1 | Canvas overflowed the viewport | Widened viewBox, built a scaled preview page |
| 2 | `K: acct/subAcct/symbol` overlapped badge ④ | Shifted right-hand geometry +40px |
| 3 | Same label straddled the Part 1 group boundary | Shortened to `acct/sub/sym`, defined in legend |
| 3 | Legend inherited `text-anchor:middle`, ran off-canvas | Gave it its own anchor-start class |
| 4 | 250px of vertical dead space | Raised the price row, trimmed canvas to 790px |

Final render: [`docs/steps/step-01/pipeline-render.jpg`](../docs/steps/step-01/pipeline-render.jpg)

### Verification

Against the assignment's six diagram rules:

| # | Rule | Met |
|---|---|---|
| 1 | Keys and values between operators | every edge labelled `K:` / `V:` |
| 2 | Sources and sinks shown | generators green on the left, six topic glyphs |
| 3 | Colours used sparingly | three — blue, amber, green; rest greyscale |
| 4 | Numbered left to right in talk order | ①–⑥, matching the expected-output table |
| 5 | Kafka icons for Kafka | topic glyph on all six topics |
| 6 | Boxes name the operation | Split, Aggregate, Join, Window |

Arithmetic checked against the assignment's table: sink 4 at 40/sec is 10 trades
× 4 allocations, and 16 keys is 4 accounts × 4 symbols. Both reproduce the
assignment's stated figures.

### Review

Round 1 confirmed the signed-position design and settled the two open semantics
questions, and renamed the topics.

- **Buys and sells**: generator emits a random mix from a seeded sequence — the
  mix is identical every run, so expected net positions stay computable rather
  than merely observable. Side affects values only, never key counts or rates.
- **Price at close**: `position at close × last price at or before close`. An
  averaged price would reconcile against nothing observable in the system.
- **Topic prefixes** now use `-` rather than `.`: `positions-by-symbol`,
  `positions-by-account`, `mv-by-symbol`, `mv-by-account`.
- **Review page** published so the diagram can be looked at and shared:
  https://claude.ai/code/artifact/65b81ff2-fe6f-4690-b323-3b8f256da462

Full exchange: [`docs/reviews/step-01.md`](../docs/reviews/step-01.md)

Round 2 corrected an error in my own review question. I had asked how late
prices should be handled; prices are keyed by symbol, so each symbol lands on a
single partition where Kafka preserves order, and a single seeded generator emits
them in event-time order. No price can arrive late, so the watermark uses
monotonically-increasing timestamps and allowed lateness is zero.

The real risk is **idleness**, not lateness: each join advances at its slower
input, so a quiet partition stalls the watermark and the one-minute windows stop
firing even though every record was in order — sinks 5 and 6 go silent, which
during a demo is indistinguishable from a broken pipeline. Handled with an
idle-source timeout and verified in step 05.

Also confirmed: keys with no activity still emit (which is what makes the counts
steady rather than activity-dependent), and every key starts flat at zero.

**Outcome:** approved, squash-merged to `main`, tagged `step-01`.

---

## Step 02 — Generators

- **Branch:** `step-02-generators`
- **Prompt:** Deterministic block trade and price generators publishing to real
  Kafka in Docker, at the rates the expected-output table is written against.

### Approach

- **The sequence is pure; the pacing is not.** `BlockTradeGenerator` and
  `PriceGenerator` are seeded iterators with no clock and no Kafka in them. Event
  times come from a counter, so the data a seed produces is identical whether a
  test pulls it as fast as it can or a publisher paces it at ten a second. That
  is what makes the demo randomised *and* exactly reproducible, and it means a
  slow consumer can never change the results.
- **Two threads, two rates.** Orders and prices are paced independently, because
  the assignment's scale cases turn one knob without the other.
- **A pacer that does not drift.** The target time for record *n* is computed
  from the start, not from the previous record, so a run does not fall
  progressively behind its nominal rate.
- **BigDecimal for price, walking in quarters.** Market value has to be
  explainable to the decimal and binary floating point cannot hold most
  two-decimal prices exactly. Quarters keep the arithmetic checkable by eye.
- **Every symbol priced on every tick.** No symbol can go quiet on the price
  side, which is one half of the idleness problem identified in step 01.
- **Explicit topic creation, auto-create off.** Partition count is a design
  decision, not a side effect of whoever produces first.
- **gzip rather than lz4.** The native compression codecs make Java 17 print a
  restricted-method warning on every start. The demo is given live from a
  console, so a clean start is worth more than the compression difference at
  these volumes.

### Commands

```bash
docker compose -f docker/compose.yml up -d
mvn verify                                     # 17 unit + 2 integration
DURATION_SECONDS=10 java -jar generators/target/generators.jar
./scripts/verify-topics.sh
```

### Results

19 tests pass. `mvn verify` is green.

| Suite | Tests | What it protects |
|---|---|---|
| `BlockTradeGeneratorTest` | 7 | allocations sum to the block, split covers every account, both sides produced, sign follows side, event time monotonic, ids unique and sorted |
| `ExpectedNumbersTest` | 4 | the assignment's table as assertions — 10/sec, 40 allocations, 4 symbol keys, 16 account keys |
| `DeterminismTest` | 6 | byte-identical across runs, different seed differs, sequence pinned by hash, prices exact quarters |
| `KafkaPublisherIT` | 2 | round-trip through a real broker; **every symbol occupies exactly one partition** |

Against the running stack, 10 seconds at the demo rate:

```
orders   record count 100 · symbols 4 · allocations 400 · account keys 16
         allocations-sum-mismatches 0 · sides 2 · duplicate ids 0
         mix 42 BUY / 58 SELL
prices   record count 40 · symbols 4 · non-positive 0 · off-quarter 0
all checks passed
```

Reproducibility measured on the wire rather than asserted in-process — two
separate 10-second runs, consumed back from Kafka and hashed:

```
run 1  213d847f804ed9d0a057471c87da453d0ae6cea0e06ca7824e13002b43205d5c
run 2  213d847f804ed9d0a057471c87da453d0ae6cea0e06ca7824e13002b43205d5c
```

Evidence: [`docs/steps/step-02/`](../docs/steps/step-02/)

### Verification

The integration test earns its place by proving the claim step 01's review
turned on: because prices are keyed by symbol, **each symbol lands on exactly one
partition**, so its prices cannot be consumed out of order. That is asserted
against a real broker, not argued from the documentation.

`ExpectedNumbersTest` encodes the assignment's own table, so if a later change
breaks the 4 / 16 key counts the build fails rather than the dashboard quietly
showing the wrong number during a demo.

### Review

Round 1 asked for prices at 1000/sec, ten sub-accounts per account, and
wall-clock event times so latency can be measured from creation to sink.

- **Prices restructured to round-robin**, one per call rather than a burst of
  every symbol, so the configured rate is a plain count of prices per second.
  Verified even at 2501 per symbol out of 10004, spread 0.
- **Ten sub-accounts per account** — reverted in round 2 as a typo. Account keys
  stay at 4 x 4 = **16**, matching the assignment. Removing the sub-account draw
  restored the original random sequence exactly: the pinned trades hash returned
  to `241903ac...`, the value it held before the change.
- **Event time now comes from a `LongSupplier`.** `live()` uses the wall clock
  and is the default; `replaying()` uses a counter for byte-identical runs.
  Reproducibility splits in two as a result: the seed fixes the *content* of the
  sequence in both modes, and replay exists for when identical bytes must be
  shown. Asserted rather than assumed.
- **The pinned sequence hash failed on purpose** in both directions — once when
  the sub-account draw was added, once when it was reverted — and landed back on
  its original value. The guard did exactly what it exists for.
- **Runs are now bounded by record count, not duration.** Proving reproducibility
  through the broker showed two replay runs still differing: a six-second run
  emits 60 or 61 trades depending on where the clock falls. Every shared record
  was byte-identical, but the boundary was not. `MAX_TRADES` and `MAX_PRICES`
  make it exact, and a record-bounded run finishes as soon as its limits are met.
- **Two bugs found in the verification script itself**, both false alarms rather
  than defects in the generators: a fixed-size prefix read slices unevenly
  across partitions and made an exactly-even round-robin look uneven, and a run
  ends mid-cycle so the record count is nominal ±1. Rates are now checked with a
  tolerance while the invariants stay exact.

Full exchange: [`docs/reviews/step-02.md`](../docs/reviews/step-02.md)

Round 2 reverted the sub-account expansion as a typo; the 1000/sec price rate,
the round-robin generator and wall-clock event times all stand.

Two record-bounded replay runs now produce byte-identical topics, and the orders
hash equals the value `DeterminismTest` pins in-process — so the sequence the
unit test guards and the bytes that reach the broker are provably the same
thing.

**Outcome:** approved, squash-merged to `main`, tagged `step-02`.

---

## Step 03 — CI

- **Branch:** `step-03-ci`
- **Prompt:** Add CI, now that there is a module with tests and an exact
  verification that can gate a build.

### Approach

- **Three jobs, each proving something different.** Build and test compiles on
  Java 17 and runs all 23 tests. Verify-the-numbers brings up Kafka and asserts
  the assignment's expected-output table against what actually lands on the
  topics. ShellCheck lints the verification scripts.
- **The numbers job is the point.** It runs the same `verify-run.sh` used
  locally, so a change that quietly breaks the numbers fails the build rather
  than surfacing during a demo. CI that only compiles would not have caught any
  of the faults found in step 02.
- **Scripts are linted, not trusted.** They are part of how correctness is
  demonstrated, so they get the same treatment as the Java.
- **`--wait` only on kafka.** The topics service is a one-shot that creates the
  six topics and exits, which `--wait` reads as a failed service.

### Commands

```bash
docker run --rm -v "$PWD:/mnt" -w /mnt koalaman/shellcheck:stable scripts/*.sh
docker compose -f docker/compose.yml up -d --wait kafka   # the exact CI sequence
./scripts/verify-run.sh
git push -u origin step-03-ci                             # triggers the workflow
gh run watch <id> --exit-status
```

### Results

Verified locally first — ShellCheck through a container, and the exact cold-start
sequence CI uses — then verified for real by pushing the branch and watching the
run. A workflow that has never executed is not evidence of anything.

[Run 32436156688](https://github.com/jimzucker/flink-training/actions/runs/32436156688): **success**

| Job | Result |
|---|---|
| Build and test | SUCCESS — 23 tests, including the Testcontainers integration test |
| Verify the expected numbers | SUCCESS — every check exact |
| Shell scripts | SUCCESS |

One ShellCheck finding, fixed with a justified disable: the sourced-or-executed
idiom in `env.sh` reads as unreachable to a static checker that cannot know
which it is.

Evidence: [`docs/steps/step-03/ci-run.md`](../docs/steps/step-03/ci-run.md)

### Verification

The runner reproduced the local numbers exactly — 100 orders, 400 allocations,
16 account keys, 400 prices at 100 per symbol, and the same **43 BUY / 57 SELL**
mix seen on the development machine.

That last figure is worth more than it looks. The runner is Linux on x86 and the
development machine is macOS on ARM, so an identical buy/sell mix demonstrates
the seeded sequence is reproducible across platforms and not merely across runs
on one box. Reproducibility that only held locally would be worth very little
when the demo runs on AWS.

### Review

Round 1: gate merges on CI, no nightly run.

Gating only means something if a failing check can stop a merge, and a squash
commit created locally and pushed straight to `main` has never been seen by CI —
it is a new SHA with no checks against it. So from step 04 the merge goes
through a pull request instead: open it from the step branch, let CI run, then
squash-merge. The result is identical — one commit per step on a linear `main`,
step branches kept — but the commit is now provably green before it lands rather
than tested immediately afterwards.

Branch protection is on with `enforce_admins: true`, which matters — without it
the repository owner bypasses the rule, and the owner is everyone who merges
here. Confirmed by attempting a direct push rather than trusting the settings
page: GitHub rejected it with *3 of 3 required status checks are expected*.
`required_linear_history` is on too, enforcing at the server what the workflow
has been doing by hand since step 00.

Full exchange: [`docs/reviews/step-03.md`](../docs/reviews/step-03.md)

**Outcome:** approved, squash-merged to `main`, tagged `step-03`.

---

## Step 04 — Part 1: positions

- **Branch:** `step-04-part1-positions`
- **Prompt:** Build Part 1 — split block trades by allocation, aggregate
  positions by symbol and by account, write sinks 3 and 4. Flink joins the stack.

### What building it found

The implementation contradicted the design, and the design was wrong.

The diagram had *Split by allocation* feeding both aggregations. But the expected
output says sink 3 emits **10/sec** and sink 4 **40/sec**, and if both sides are
fed from the split then both emit once per allocation — 40/sec each. The
quantities would still be correct, because allocations sum to the block; only the
*rate* would be wrong.

That is a bad failure mode: nothing crashes, every number reconciles, and the
error only shows up when someone reads a rate off a dashboard and it is four
times what the handout says. Corrected in code and in the diagram: the symbol
side takes the block trade whole and bypasses the split.

### Approach

- **Flink POJOs, not records, inside the pipeline.** Flink 1.20 does not
  recognise Java records as POJOs and falls back to Kryo, which cannot reliably
  instantiate them. `PositionUpdate` and `PositionState` are plain classes with
  public fields and a no-argument constructor, which gets Flink's own POJO
  serialiser. The records in `common` remain the wire format.
- **The pipeline is separated from the environment.** `PositionsJob.build()`
  takes a `DataStream`, so it can be driven by a test source instead of Kafka.
- **Emit on every change, not on a timer.** That is what makes sinks 3 and 4
  match the input rate exactly, and what lets the demo point at a number and name
  the trade that produced it — `lastTradeId` and `updateCount` travel on the
  record rather than being reconstructed from logs.
- **At-least-once delivery.** A position is a running sum, so a replayed record
  would double-count. That only arises on failure and restore, which the demo
  does not exercise; exactly-once needs Kafka transactions and a read-committed
  consumer, and belongs with the AWS deployment.

### Results

23 unit and integration tests pass. `mvn verify` is green.

| Suite | Tests | What it protects |
|---|---|---|
| `OperatorTest` | 4 | symbol side emits once per trade, account side once per allocation, sells decrement both, and the two sides reconcile |
| `AccumulatePositionTest` | 7 | running signed sum under Flink's own harness — goes short, nets to zero and still emits, keys independent, and **survives a snapshot and restore** |

End to end against the real cluster, feeding exactly 100 trades:

```
positions-by-symbol   100 records over 4 keys      (one per trade)
positions-by-account  400 records over 16 keys     (one per allocation)
updateCount runs 1..n per key with no gaps
reconciliation  AAPL -2800  AMZN -4000  GOOG -2000  MSFT 3200
                symbol totals match account totals
all checks passed, with no tolerances
```

Negative positions are present and expected: they are shorts, and their presence
is what shows the sign handling is live rather than untested.

Evidence: [`docs/steps/step-04/`](../docs/steps/step-04/)

### Verification

The reconciliation check is the strongest available statement about Part 1: the
same question — what is the position in each symbol — answered two different
ways, through 100 block-level updates and through 400 allocation-level updates,
and required to agree. A fault in the split, the sign, or the keying breaks it.

`updateCount runs 1..n per key` is the other one worth naming: it proves each
key's update sequence has no gaps and no duplicates, which a record count alone
would not catch.

### Review

Round 1: at-least-once rejected, parallelism must be a runtime knob because the
demo scales 2 → 4, and empty sinks 5 and 6 confirmed as expected.

- **Exactly-once** on both sinks, with a transactional id prefix each and a
  transaction timeout that outlasts a checkpoint while staying under the broker
  maximum. Two consequences recorded rather than hidden: the sinks now advance
  once per checkpoint rather than continuously, which is also a floor under
  end-to-end latency; and every reader must use `read_committed`.
- **It broke the record counter silently.** Each committed transaction appends a
  marker that advances the partition offset without being a record, so summing
  end offsets over-counts and a wait keyed on it can finish before the data is
  there — the verification would have measured a half-written topic. Counting now
  consumes with `read_committed`.
- **Proved under failure.** A guarantee about failure is untested without one, so
  a task manager is killed mid-run. The job recovered from checkpoint and the
  sinks held exactly 100 and 400 records with update counts still running 1..n.
  Under at-least-once both of those checks would have failed.
- **Parallelism** is now a runtime setting passed to the submitting client, where
  the graph is built. Verified against the running job: all three vertices at
  parallelism 4.

Full exchange: [`docs/reviews/step-04.md`](../docs/reviews/step-04.md)

**Outcome:** awaiting round 2.

---

## Step 05 — Part 2: market value

- **Branch:** `step-05-part2-marketvalue`
- **Prompt:** Build Part 2 — join prices to positions, emit market value once per
  minute at the price at close, write sinks 5 and 6. Verify the idleness handling
  identified in step 01's review.

### Approach

- **Event-time timers on window boundaries, not a windowed aggregate.** The
  required semantics are a snapshot at an instant — the position as of the
  boundary times the last price at or before it — not a summary of an interval.
  A windowed reduce describes what happened during the minute; this describes
  what was true when it ended, which is the thing that reconciles against the
  position topic at that timestamp.
- **Prices broadcast, not keyed.** The account side is keyed on
  account/sub-account/symbol and cannot be joined to a symbol-keyed stream by key
  alone.
- **Event time decoupled from emission rate in replay.** Covering three window
  boundaries needs three minutes of event time, which previously meant three
  minutes of waiting. A window test that slow is a window test that stops being
  run. Replay now advances event time per record independently of how fast
  records are emitted, so 200 seconds of event time takes two seconds. The live
  demo is untouched: event time is the wall clock, which is what makes latency
  measurable.

### Two bugs the replay found

Both would have survived unit tests, and both produce plausible-looking output.

1. **Timers registered from the watermark, not the record.** When processing runs
   ahead of event time — a replay, or catching up after a restart — the watermark
   is already past the record, so the next boundary computed from it is in the
   future and every window the data actually covers is skipped. Symptom: two jobs
   RUNNING, Part 1 perfect, sinks 5 and 6 permanently empty.

2. **Only the latest price kept per symbol.** Correct while records arrive in step
   with the clock, wrong the moment the price stream races ahead: a window closes
   against a price from its own future. The number still looks like a price and
   still multiplies out correctly — it just reconciles against nothing. Prices are
   now held per window, and a window looks backwards for the last price at or
   before its boundary.

Both now have regression tests naming the scenario rather than the mechanism.

### Results

22 unit tests pass, 11 of them on window semantics. Full pipeline verified end to
end in one command:

```
sink 3   200 records over  4 keys
sink 4   800 records over 16 keys
sink 5    12 records = 4 keys x 3 windows
sink 6    48 records = 16 keys x 3 windows
duplicate key/window pairs                    0
marketValue != quantity x price               0
price at close differs from the price topic   0
symbol totals match account totals           OK
all checks passed, with no tolerances
```

The last of those is the strongest available statement about Part 2: the closing
price of every window, recomputed independently from the raw price topic and
required to match what the sink published. Twelve of twelve, then again for all
of sink 6.

Idleness proved by turning it off: identical input, one setting changed, sinks 5
and 6 drop from 12 and 48 to **zero** while Part 1 is unaffected. Details in
[`docs/steps/step-05/idleness.md`](../docs/steps/step-05/idleness.md).

### An operational finding

Repeatedly deleting and recreating topics against a long-lived broker, while
transactional producers hold state, left that broker unable to serve new
consumers — offsets reported 200 records and a console consumer read zero for 60
seconds, with the broker reporting healthy and Flink still consuming happily. A
fresh stack behaved correctly immediately. CI starts clean every run so it is
unaffected, but a local stack reused across many verification cycles can reach
that state, and the symptom looks like a data bug rather than an environment one.

### Review

_Pending._

---

## Step 06 — Observability

- **Branch:** `step-06-observability`
- **Prompt:** Prometheus and Grafana join the stack. Dashboard laid out as the
  requirements ask — sources left, sinks right, latency — with panels numbered as
  on the pipeline diagram.

### Approach

- **The jobs publish a key-count gauge.** The expected-output table is stated in
  unique keys, four by symbol and sixteen by account, and Flink has no metric for
  that. Keys are partitioned across subtasks, so summing the gauge gives the
  total without double counting. It counts from when a subtask started, so a
  restart resets it until every key has been seen again — recorded rather than
  hidden.
- **Panels carry the diagram's numbers.** A figure on the dashboard can be
  pointed at on the diagram without translation, which is the whole reason the
  numbering was fixed in step 01.
- **Grafana renders its own screenshots.** Images for the deck have to be
  reproducible, and depending on a browser turned out to be a mistake — see below.

### What went wrong, and what it cost

The dashboard rendered nothing in the browser: correct model stored, queries
returning data, chrome and navigation drawing fine, and an empty panel area with
no JavaScript errors. Three things were tried and two were wrong.

1. **Suspected the dashboard JSON.** A minimal single-panel dashboard, built the
   canonical way, was also blank. Not the JSON.
2. **Suspected the Grafana version.** 11.5 enables the Scenes renderer and the
   toggles are GA, so it cannot be turned off; pinned back to the 10.4 LTS line.
   Still blank. Not the version — though the pin is kept, since a demo tool
   should not sit on a renderer that changes under it.
3. **Rendered server-side instead.** The dashboard drew perfectly. The browser
   was the problem all along: an extension in it manipulates the page root, and
   that was enough to stop Grafana drawing panels while leaving no error behind.

That is worth keeping in mind for the demo itself. A dashboard that works
everywhere except the machine it is being presented on is indistinguishable from
a broken pipeline, and there is nothing in the logs to say otherwise.

Two real defects were found once it could be seen at all, both invisible without
rendering it:

- Stat panels were blank because the reducer was named `lastNonNull`. The correct
  id is `lastNotNull`; an unknown reducer displays nothing rather than
  complaining.
- Row panels stopped everything below them from drawing. Removed — they were
  decoration.

### Results

Dashboard: [`docs/steps/step-06/dashboard.png`](../docs/steps/step-06/dashboard.png)

The rates on it are the design, visible:

| Panel | Reads |
|---|---|
| ① orders | 10/sec |
| ② prices | 1000/sec |
| ③ positions-by-symbol | 10/sec — one per trade |
| ④ positions-by-account | 40/sec — one per allocation |
| ③ / ⑤ unique keys | **4** |
| ④ / ⑥ unique keys | **16** |

Sinks 5 and 6 step rather than flow, which is what emitting once per key per
window looks like on a rate graph.

### Review

_Pending._
