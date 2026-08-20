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
| 03 | CI | Kafka | `step-03-ci` | not started |
| 04 | Part 1 — positions | Kafka, Flink | `step-04-part1-positions` | not started |
| 05 | Part 2 — market value | Kafka, Flink | `step-05-part2-marketvalue` | not started |
| 06 | Observability | Kafka, Flink, Prometheus, Grafana | `step-06-observability` | not started |
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
- **Ten sub-accounts per account.** Account keys go from 16 to **160**. The
  40/sec rate is unchanged since it follows from four allocations per trade.
  This differs from the 16 the assignment prints, and was raised as such.
- **Event time now comes from a `LongSupplier`.** `live()` uses the wall clock
  and is the default; `replaying()` uses a counter for byte-identical runs.
  Reproducibility splits in two as a result: the seed fixes the *content* of the
  sequence in both modes, and replay exists for when identical bytes must be
  shown. Asserted rather than assumed.
- **Key coverage is now a function of run length** — 160 of 160 at 45s, but 147
  at 10s, which is what coupon-collector predicts for 400 draws over 160 keys.
  A short demo shows the count climbing rather than settled.
- **The pinned sequence hash failed on purpose** and was re-pinned: drawing a
  sub-account consumes extra randomness, so the sequence legitimately changed.
- **Two bugs found in the verification script itself**, both false alarms rather
  than defects in the generators: a fixed-size prefix read slices unevenly
  across partitions and made an exactly-even round-robin look uneven, and a run
  ends mid-cycle so the record count is nominal ±1. Rates are now checked with a
  tolerance while the invariants stay exact.

Full exchange: [`docs/reviews/step-02.md`](../docs/reviews/step-02.md)

**Outcome:** awaiting round 2.
