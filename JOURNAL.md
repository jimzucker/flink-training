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
| 07 | Correctness demo | full | `step-07-correctness-demo` | in review |
| 08 | Local Docker demo | full | `step-08-docker-local` | in review |
| 09 | Latency | full | `step-09-latency` | in review |
| 10 | Scale | full | `step-10-scale` | in review |
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

Round 1: keep anonymous access, and close the CI gap.

CI now runs `scripts/verify-dashboard.sh`, which checks the dashboard is
provisioned and well formed, that **every panel query returns data**, that the
key counts read 4 and 16, and that it renders to a non-trivial image.

The query check is the one that matters. Every panel names a Flink operator, and
those names come from `.name()` calls in the jobs — renaming one silently blanks
a panel with nothing failing and nothing logged. That is only noticed by looking
at it, and during a demo that is too late.

The checks were proved by reintroducing the defects rather than trusting them:

| Defect reintroduced | Caught |
|---|---|
| `lastNonNull` reducer id | 10 unknown stat reducers |
| a row panel | 1 row panel |
| an operator renamed out of existence | 1 panel query returning nothing |

All three failed the check, and the run exited non-zero.

Full exchange: [`docs/reviews/step-06.md`](../docs/reviews/step-06.md)

**Outcome:** approved, squash-merged to `main`, tagged `step-06`.

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

Round 1: keep anonymous access, and close the CI gap.

CI now runs `scripts/verify-dashboard.sh`, which checks the dashboard is
provisioned and well formed, that **every panel query returns data**, that the
key counts read 4 and 16, and that it renders to a non-trivial image.

The query check is the one that matters. Every panel names a Flink operator, and
those names come from `.name()` calls in the jobs — renaming one silently blanks
a panel with nothing failing and nothing logged. That is only noticed by looking
at it, and during a demo that is too late.

The checks were proved by reintroducing the defects rather than trusting them:

| Defect reintroduced | Caught |
|---|---|
| `lastNonNull` reducer id | 10 unknown stat reducers |
| a row panel | 1 row panel |
| an operator renamed out of existence | 1 panel query returning nothing |

All three failed the check, and the run exited non-zero.

Full exchange: [`docs/reviews/step-06.md`](../docs/reviews/step-06.md)

**Outcome:** approved, squash-merged to `main`, tagged `step-06`.

---

## Step 07 — Correctness demo

- **Branch:** `step-07-correctness-demo`
- **Prompt:** The assignment's fourth deliverable — demonstrate the numbers and
  calculations are correct at small volume using Grafana.

### Approach

The verification already asserted the numbers. What was missing was the ability
to *explain* one, which is what the requirements actually ask for: be able to
explain the figures, and if you cannot answer a question, change the logging
until you can.

`scripts/trace-trade.sh` takes a trade id and prints its whole provenance —
the block trade, the single position-by-symbol update it caused, the four
position-by-account updates, and the market values of the windows it closed. It
then states the arithmetic tying them together rather than leaving the audience
to do it:

```
the four accounts sum to -2800, which is what sink 3 reports  OK
the account market values sum to -207900.00, which is what sink 5 reports  OK
```

That works because every record carries `lastTradeId`, added in step 04 for
exactly this reason.

`docs/demo-runbook.md` is the walkthrough: what to open, what to say, in what
order, and answers ready for the questions most likely to be asked — why sink 4
updates four times as often as sink 3, why the market value sinks look like
steps, why a number appeared a moment late, and whether positions go negative.

### Results

A trade traced end to end, and reconciling both ways:

| | |
|---|---|
| ① `T000000098` | BUY 400 AAPL, four allocations of 100 |
| ③ AAPL | −2800 |
| ④ each of four accounts | −700 → **sum −2800, matching ③** |
| ⑤ AAPL | −2800 × 74.25 = −207900.00 |
| ⑥ each of four accounts | −700 × 74.25 = −51975.00 → **sum −207900.00, matching ⑤** |

Evidence: [`docs/steps/step-07/`](../docs/steps/step-07/)

### Review

Round 1: a ten-second window is acceptable for the live demo.

`scripts/demo.sh` now brings the whole stack up and submits both jobs with a
ten-second window, printing what to open and in what order — so nothing is typed
from memory in front of an audience. The job still defaults to the specified
minute; only the demo overrides it.

That is the one deviation from the specification visible on screen, so the
runbook says to volunteer it when reaching sinks 5 and 6 rather than let someone
notice it. It is a presentation choice, not a difference in the calculation, and
the verification runs against both settings. Confirmed by running it: market
value appears within the first ten seconds rather than after a minute of nothing.

Questions 1 and 2 went unanswered, so the runbook stays as written and the demo
rate stays at the stated 10 trades/sec.

Full exchange: [`docs/reviews/step-07.md`](../docs/reviews/step-07.md)

A follow-up audit of every window reference found one stale claim: the README
said the demo kept the specified minute, which step 07 had just made untrue. The
window now has one authoritative statement of its three settings — the specified
minute as the job default and on the diagram, ten seconds in the demo and in the
verification — and the design doc says explicitly that the interval is a runtime
parameter while the calculation is not.

The diagram still shows the minute, deliberately: it is the design, and the
requirements are specific about keeping it uncluttered. The runbook carries the
spoken caveat instead.

**Outcome:** approved, squash-merged to `main`, tagged `step-07`.

---

## Step 08 — Local Docker demo

- **Branch:** `step-08-docker-local`
- **Prompt:** The assignment's fifth deliverable — the whole thing running
  locally on Docker with Kafka and Flink, green from a cold start in one command.

### Approach

**The jars build inside Docker.** A multi-stage build compiles them with Maven
and produces one image carrying the generators and another carrying the jobs and
the Flink client. `docker compose up` then needs nothing but Docker — no JDK on
the host, and no "did you build first?", which is not a step anyone should have
to remember in front of an audience. Dependencies resolve in their own layer, so
editing a source file does not re-download the world.

**Ordering is declared, not hoped for.** Submitting before the topics exist fails
the job with `UnknownTopicOrPartition` and it then restarts in a loop, so the
submitter waits for the topic creation to complete. A task manager also registers
a moment after its container starts, so the submitter waits for a free slot
rather than racing it.

**The local stack uses the ten-second window.** This stack *is* the demo, so it
matches what the demo runs. The job still defaults to the specified minute; only
this stack overrides it.

### The bug the cold start found

The generators container came up and immediately began restarting. Running
unbounded — which is exactly how the runbook says to start the data — the
generator exited on start.

A refactor in step 02 had moved `running.set(false)` outside the branch that
owned it, so the run-forever path stopped its own threads immediately. Every run
until now had been bounded by record count or duration, so nothing had exercised
it: the demo's own command was broken and no test covered it.

`scripts/verify-cold-start.sh` now checks the generator's **restart count**, not
just that the container is up. A container that exits and is restarted by Docker
looks alive in `docker compose ps` while doing nothing at all.

### Results

```
one command, from nothing
  came up in 36s

services            kafka, jobmanager, taskmanager, prometheus, grafana, generators  running
generator restarts  0
flink jobs running  2
data reaching every topic                       orders, prices, positions x2  OK
key counts                                      4, 16, 4, 16  OK
dashboard renders                               92510 bytes
cold start came up green
```

Evidence: [`docs/steps/step-08/`](../docs/steps/step-08/)

### Review

Round 1 asked for the generators to be triggerable, with auto, manual and delayed
options, and for the cold start to run in CI.

`GENERATOR_START` now takes `manual`, `auto`, or a number of minutes, and manual
is the default. Graphs going from flat to flowing while people watch is the most
persuasive moment in the demo, and a stack that is already busy when it appears
gives it away. The delay gives both, letting data arrive on cue without touching
a terminal. All three were tested rather than assumed, and a bad value is
rejected at start rather than silently defaulting, since that would be discovered
mid-demo.

The cold-start check follows the same default, so it exercises the demo path
rather than a convenience path: everything up with zero records produced, then
the trigger, then data flowing.

A `Cold start` job runs in CI and is now a required check. It is deliberately
separate from the numbers job, which builds on the host and starts services
individually — which is exactly why that job did not catch the generator exiting
on start. Running the deliverable the way it is actually delivered is the only
thing that would have.

Full exchange: [`docs/reviews/step-08.md`](../docs/reviews/step-08.md)

**Outcome:** approved, squash-merged to `main`, tagged `step-08`.

---

## Step 09 — Latency

- **Branch:** `step-09-latency`
- **Prompt:** The assignment's sixth deliverable — demonstrate that latency for
  orders is low, using logging.

### Approach

Two numbers are needed, and reporting only the first would flatter the result.

- **What the pipeline takes.** The operators publish a
  `processingLatencyMillis` histogram: how old a trade is when its position is
  computed. It reaches Prometheus as quantiles and is on the dashboard. The same
  figure goes into the position log line, since the requirements ask for latency
  to be demonstrated using logging and it belongs next to the number it explains.
- **What a consumer waits for.** An operator cannot see this. Under exactly-once
  a record is not readable until the checkpoint that produced it commits, so
  `scripts/measure-latency.sh` reads the topics from outside and records how old
  each record was when it became readable.

This is only measurable at all because the generator stamps records with the wall
clock — the decision made in step 02.

### Results

```
processing   p50   59 ms    p99  110 ms

orders                 p50=9      p99=17     max=22      (ms)
positions-by-symbol    p50=2488   p99=4981   max=4988    (ms)
positions-by-account   p50=2524   p99=4985   max=5024    (ms)
```

The input path is single-digit milliseconds. The positions are not, and the
difference is the checkpoint commit rather than the work.

That was confirmed by changing one setting:

| Checkpoint interval | p50 | max |
|---|---|---|
| 5s | 2488 ms | 4988 ms |
| 1s | **497 ms** | **1041 ms** |

Five times shorter, five times lower, with the maximum landing inside one
interval each time — which is what a uniform wait for the next commit looks like.
It is the strongest available evidence that the delay is the guarantee and not
the pipeline.

Evidence: [`docs/steps/step-09/`](../docs/steps/step-09/)

### Verification

Reporting the operator metric alone would have shown 59ms and looked excellent
while a consumer waited two and a half seconds. Measuring from outside is what
makes the figure honest, and the checkpoint comparison is what makes it
explainable rather than merely stated.

### Review

Round 1: show market value latency, move the checkpoint interval to one second,
and chart checkpoints.

- **Market value is now measured**, from the window close rather than the trade,
  since the window is the specification and not a delay. It steps rather than
  spreads, and the figures say so — p95, p99 and the maximum are the same number,
  because every key in a window is emitted at the same boundary and shares an age.
- **One second is now the default.** Order latency falls from p50 2488ms to
  **518ms**, with the maximum inside one interval as before.
- **Checkpoints are charted** next to the latency they explain, which settled what
  the cost actually is: a checkpoint takes about 13ms against a one-second
  interval, so the interval between them is what costs.

Charting them also surfaced one aborted checkpoint per job at startup — the first
checkpoint triggers before every task is running. Harmless, but the panel had
been written to say any non-zero count meant the guarantee was being retried,
which would have raised a false alarm mid-demo. The panel now says what the
number means and the runbook has an answer ready.

Full exchange: [`docs/reviews/step-09.md`](../docs/reviews/step-09.md)

**Outcome:** approved, squash-merged to `main`, tagged `step-09`.

---

## Step 10 — Scale

- **Branch:** `step-10-scale`
- **Prompt:** The assignment's two scale cases — orders to 1000/sec without
  losing throughput, and a very high price rate without hurting order latency —
  plus the parallelism 2 to 4 comparison.

### Results

Both specified cases pass.

| | orders/s asked | prices/s | orders through | latency p50 |
|---|---|---|---|---|
| baseline | 10 | 1000 | 8/s | 519 ms |
| case 1 | 1000 | 1000 | **1000/s** | **522 ms** |
| case 2 | 10 | **20000** | 8/s | **513 ms** |

Case 1 raised throughput a hundredfold with latency unmoved. Case 2 raised the
price rate twentyfold with order latency unchanged, which settles the question
left open in step 05: broadcasting prices to every subtask was the concern, and
at twenty times the rate it does not bite. Measuring rather than pre-reducing was
the right call — pre-reducing would have traded away the exact price-at-close for
a problem that is not there.

### Forcing it to fail

Passing runs say less than a broken one, so the rate went up until it broke:
315,000 orders/sec offered at parallelism 4.

| | sustained | lag drift | latency p50 | checkpoint |
|---|---|---|---|---|
| parallelism 4 | 37,419/s | **+242,861/s** | **110,088 ms** | 270–720 ms |
| parallelism 2 | 43,300/s | +248,766/s | 107,080 ms | 200–714 ms |

Nothing crashed. No failed checkpoint, no restart, no exception — Flink
back-pressures the source and keeps producing correct output, just further behind
every second. Latency rose two hundredfold because each record now waits behind a
backlog of thirty million, so under saturation latency measures the queue rather
than the work. The failure is invisible in correctness, invisible in throughput,
and obvious only in lag.

### Why parallelism does not move it here

Parallelism 2 beat parallelism 4 in that run, because three generator JVMs were
competing with the cluster for eight cores and for the same broker. Removing the
producer entirely — filling the topics first and timing a drain of an identical
8,000,000-order backlog — gives a clean and flat answer:

| parallelism | time to drain | orders/sec |
|---|---|---|
| 1 | 58 s | 137,931/s |
| 2 | 56 s | 142,857/s |
| 4 | 61 s | 131,147/s |

The first explanation I reached for was cores, and it was wrong — recorded here
because the wrong answer was the plausible one. Each candidate was excluded by
measurement:

| Candidate | Result |
|---|---|
| TaskManager CPU | 385% at parallelism 1, 475% at 4, of 800% — **~40% of the box idle while throughput was pinned** |
| A container CPU cap | `cpu.max` is `max`, quota `-1` — none |
| Kafka reads | 1,908,570 records/sec — fourteen times the pipeline's rate |
| Partition count | 16 partitions instead of 4: 137,931/sec against 131,147 — unchanged |
| Checkpointing | a 10s interval instead of 1s: no better |

Low CPU with high back-pressure is the tell: a thread blocked on a Kafka
acknowledgement is neither busy nor idle for want of work.

The constraint is the **single broker's write throughput**. Four concurrent
producers measure its aggregate ceiling at **750,000 records/sec**, and the
pipeline writes five records per order — one symbol-side, four account-side — so
at 137,000 orders/sec it is issuing 685,000 writes/sec, **91% of the broker's
capacity**. That explains every observation at once: flat across parallelism
because the broker is shared, indifferent to partition count because the limit is
the broker process, TaskManager cores half idle because its threads are blocked
on acknowledgements, and the account writer pinned because it issues four of the
five writes.

**Flink parallelism scales the work inside the job; it cannot scale a broker
outside it.**

The earlier claim that the generator was the ceiling is also superseded twice
over. The pacer was fixed, and then gzip compression turned out to cap the
producer at 387,000 orders/sec against lz4's 879,000 — gzip having been chosen to
avoid a native-library warning that only appears on Java 24+, while this project
targets 17.

### What would prove scaling

Raise the ceiling that actually binds, then vary parallelism against it: a
three-broker cluster roughly triples write capacity, and the drain can then be
run at 1, 2 and 4 against a limit that is not the first thing hit. Worth noting
that four of every five records written come from the account branch, so reducing
that write volume moves the ceiling further than any parallelism change — and
that the same trap exists in the managed service, where KPUs and MSK brokers are
provisioned separately.

Details: [`docs/steps/step-10/scaling.md`](../docs/steps/step-10/scaling.md)

### Review

Two rounds. The first redirected the whole approach — force a failure and
evaluate it rather than re-running passing cases. The second asked what the
dashboard actually monitors, and the answer was: not enough. Back-pressure was
not charted, per-operator usage was not charted, and consumer lag — the signal
that defines saturation — had no panel at all. The account-chain diagnosis had
come from an ad-hoc PromQL query, which meant the dashboard could not have found
it. All three are now charted, with busy and back-pressure broken out per task.

It also asked why CI failures were not being caught before the push. They are
now: `scripts/precheck.sh` runs the two jobs that need no Docker stack in about
fifteen seconds, and `.githooks/pre-push` runs it on every push. Every CI failure
on this branch would have been caught by it.

Full exchange: [`docs/reviews/step-10.md`](../docs/reviews/step-10.md)
