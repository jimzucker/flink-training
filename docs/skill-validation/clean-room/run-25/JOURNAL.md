# Journal

## Step 0 — the interview that could not happen

The skill says to interview one question at a time before building. There is no
human to answer, so the questions and the answers I took in their place are
written down here and every one of them is an assumption, not an agreement.

| # | question I would have asked | assumption taken instead | why |
|---|---|---|---|
| 1 | What is the input event and what comes out? | A **block trade** arrives on `block-trades`; it is **allocated across 4 accounts**; two running positions are maintained and written to Kafka — **by symbol** (`positions-by-symbol`) and **by account** (`positions-by-account`). | This is the task, restated in domain language. It is the spec everything else is judged against. |
| 2 | Does one input become several outputs? | **5 outputs per input**: 1 symbol-position update + 4 account-position updates. Fan-out is exact and constant per record. | The harness's two-vantage guard divides sink growth by this number, so it has to be exact, not an average. A constant allocation count is the only way to make it one. |
| 3 | What are the keys and how many distinct ones? | **64 symbols, 512 accounts**, fixed and predicted up front. Positions are keyed by symbol and by account (not by account×symbol). | Small fixed cardinality makes the completeness answer arithmetic: exact per-key sums, no tolerances. §4 asserts the observed cardinality against these two numbers. |
| 4 | What has to be exactly right, and what are the two settings? | A position is a **running sum**, so a replayed record is a wrong number, not a duplicate. **State: exactly-once checkpointing. Sink: at-least-once, made idempotent by emitting the absolute position for the key on every update.** | The skill's §1 q4 answer. Tested by killing the worker mid-drain and re-asserting every total (it passed on the first attempt — see step 2). |
| 5 | Who watches, and what must they believe? | Engineers: the numbers are not picked apart, and nothing was lost. | Drives the guard-heavy, no-tolerance completeness run over a prettier dashboard. |
| 6 | Where does it run? | The laptop, in Docker, per the task. | Given. |
| 7 | What claim do you want to make? | *"This block-trade allocation and position pipeline scales linearly with the cores bought: doubling the worker's cores and its parallelism doubles the trades it drains per second."* | Written verbatim; every later decision is judged against it. |
| 8 | Which axis? | **One worker growing**: one task manager container capped at N cores, parallelism N, N slots. Not a second JVM. | The laptop proxy the skill names. Recorded in the results header. |
| 9 | Which API level? | **DataStream API, hand-written operators. No SQL, no Table API.** | Given by the task. Recorded in the results header. |

Two more assumptions the task forced, stated because they cut against the skill:

* **The 1-core case is run.** §5 says that if the claim is a step from two units
  up, the one-unit case should not be run at all — it is the structurally
  weakest and noisiest case in every run on record. The task asks for 1, 2 and
  4, so it is run, and the report leads with 2→4 anyway.
* **The baseline is measured on the same graph as every other case.** Both
  aggregations sit behind a `keyBy`, whose edge is HASH at every parallelism
  including 1, so the 1-core case pays the same serialization the others do.
  A chained baseline is the §5 trap that turned 2.16× into 3.26× on one build.

## Step 1 — build

`job/` is a single fat jar with three entry points, so the expected answer, the
pipeline and the checker cannot drift apart:

* `bt.Trades` — the domain in one file: the record layout, and the
  largest-remainder allocation. Integer arithmetic only, so the allocated
  quantities sum to the block quantity **exactly**.
* `bt.PositionsJob` — the job. `kafka-source -> parse` chained, then two HASH
  edges into `symbol-position` and `account-position`, each with its own Kafka
  sink. Exactly-once checkpointing; at-least-once sink emitting the absolute
  position.
* `bt.GenerateBacklog` — deterministic generator and the manifest (the expected
  answer, computed from the generator function, never from the pipeline).
* `bt.VerifyCompleteness` — reads both sinks, keeps the last value per key, and
  compares to the manifest with no tolerances.

`bt.Args` exists because the generator and verifier run on the **host** JDK
against this jar alone: a shared helper on the job class dragged `flink-core`
into a JVM that does not have it, and the first host-side run died on
`NoClassDefFoundError: org/apache/flink/configuration/ReadableConfig`.

The harness ships with the skill and is used verbatim. Nothing in `harness/`
was copied, edited or re-implemented; `pipeline.json` is the only thing I
supply to it.

## Step 2 — verified before anything expensive

Four things were asserted before the ~1 h chain was launched, because each one
can void everything after it:

1. **Determinism** — two manifests from one seed are byte-identical
   (`sha256 613853d0…`, 200,000 records). The preflight repeats this.
2. **Correctness end to end**, on a 1M-record backlog through the harness's own
   `completeness` command: clean drain and a worker killed at 35% of the drain,
   both `COMPLETENESS OK — every assertion held exactly, no tolerances`;
   64 symbol keys, 512 account keys, net qty and notional equal to the manifest,
   the two paths equal to each other.
3. **The job graph and the metric names** — read off the running plan: 3
   vertices, both keyed edges `HASH`, and the source vertex resolves as
   `Source: kafka-source -> parse -> (symbol-leg, allocate)` with 6
   busy/idle/back-pressure samples in a 35 s window. Had this not matched
   `sourceVertexMatch`, every case would have been refused for having no
   external-boundary samples — 20 minutes into the chain.
4. **Fan-out is exactly 5** — 1,000,000 input records produced exactly
   1,000,000 symbol records and 4,000,000 account records on the sinks.

Two rig figures measured at the same time, used only to size the backlogs:
the generator fills at ~1.28M rec/s, and a cold 4-core drain of 20M records ran
at roughly 400–600k rec/s. `backlog.count = 280,000,000` therefore covers a
4-core case up to about 950k rec/s; the tiny proof re-checks this against the
measured rate and refuses the chain if the guess is short.

## Step 3 — the plan for the chain

`prove.py all --quick`, detached, one stack session. `--quick` is two passes per
case instead of three: the harness stamps the table `quickLook` /
`publishable: false`, and the report says so where the numbers are.

## Step 3 — the chain, as it actually went

Four attempts. Only the fourth produced a table.

| attempt | what ran | outcome |
|---|---|---|
| 1 | `prove.py all --quick` | **FAIL at preflight** after 1.7 min: `host_ceiling()` in `prove.py` assigns `out["hostScaling"]` where `cmd_preflight` has no `out`. A `NameError` that fires for any pipeline; 17/18 rows had passed. `all` stops at the first failing step, so the shipped chain command was unusable. Ran the harness's own commands one at a time instead, chained in one shell. |
| 2 | up → completeness → tinyproof | completeness PASS (both arms). **Tiny proof refused the 4-core case: 92.7% of its cap, floor 95%.** |
| 3 | up → tinyproof | **Refused again at 92.9%** — reproducible in that context, while a standalone probe of the same build on a fresh topic read 96.3%. |
| 4 | up → completeness → tinyproof → fill → suite → report | **Suite clean: 7 cases, no refusals, no ceilings.** `report` exits 1 because 2→4 misses the claim floor — a result, not a defect. |

Between 3 and 4, one change: the sink payload went from ~110 B of JSON to ~30 B
pipe-delimited. Both arms measured; see REPORT.md for what that does and does not license
me to claim.

`prove.py` was modified on disk at 20:56:21, during attempt 4. Not by me. `lib.py` —
every threshold, guard, `run_case` and `build_table` — was untouched.
