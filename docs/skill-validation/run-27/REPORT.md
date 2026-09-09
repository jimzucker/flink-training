# Block trades → positions → market value at close, on 1, 2 and 4 cores

**Headline: 2→4 cores returned 1.85× (interval 1.824–1.881), 93% of linear —
the claim was not met.** 1→2 returned 2.43× (interval 2.402–2.451), which is
*superlinear* and says more about the one-core baseline than about the
pipeline. Every case owned its CPU cap (99.6–100.2%), the pipeline is exactly
correct on a clean drain and with the worker killed mid-drain, and the table is
valid — it just says this pipeline does not scale to the claim on this rig.

> **QUICK LOOK — not a publishable result.** This table came from
> `prove.py all --quick`: **two passes per case**, not the configured three.
> That is enough for a spread and not enough to publish a ratio; the harness
> stamps it `publishable: false`. Every per-case guard was live. Quote the
> spread with the ratio or do not quote it.

## Header

| field | value |
|---|---|
| **axis** | one worker growing: one task manager container capped at N cores, parallelism N, N slots |
| **API level** | Flink DataStream API, hand-written operators and event-time timers. No SQL, no Table API — and no window API either, because the value wanted is the state *at* the boundary, not an aggregate over the interval |
| **guarantee — state** | exactly-once checkpointing (aligned, 10 s interval, hashmap backend, source offsets committed on checkpoint) |
| **guarantee — sink** | at-least-once, idempotent by key: a running-position record carries the *absolute* position for its key, and a windowed market-value record is keyed by `(key, window end)` and carries a value that is a pure function of the input — so a replay overwrites and never adds |
| **checkpoint interval** | 10 000 ms |
| **build hash** | `8c010e5c57edcca4` — one build for the completeness gate, the tiny proof and every row of the table |
| **passes per case** | 2 (`--quick`), plus the sentinel: the baseline measured a third time at the end |
| **study** | scaling — every case configured identically; no `perCase` tuning |
| **rate source** | committed broker offsets on `block-trades` (never the engine's meter) |
| **CPU source** | cgroup `cpu.stat usage_usec` at window open and close |
| **stack** | Flink 1.20.1 / java 17, Apache Kafka 3.9.0, both arm64 native; macOS arm64, Docker Desktop, 7 838 MiB VM, 8 CPUs |
| **claim under test** | *"On one worker, this pipeline's throughput scales linearly with the cores it is given: going from two cores to four at least doubles the records it drains per second, to within 95% of linear."* |

## The table

| cores | passes | mean records/s | spread | % of cap | **CPU µs / input record** | **GC % of capacity** | broker cores | source idle | source back-pressure | headroom at close | reportable |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 1 | 2 | 108,886 | 7.8% | 99.8% | **9.18** | **5.8%** | 0.06 / 2.5 | 0.0% | 45.0% | 1330 s | yes |
| 2 | 2 | 264,183 | 6.8% | 99.6% | **7.53** | **5.9%** | 0.15 / 2.5 | 1.3% | 32.2% | 448 s | yes |
| 4 | 2 | 489,596 | 8.3% | 100.2% | **8.19** | **2.9%** | 0.24 / 2.5 | 3.0% | 22.7% | 175 s | yes |

CPU µs per input record is the task manager's cgroup CPU over the window
divided by the records its source committed in the same window. Back-pressure
here is *internal* — the source waiting on the aggregation threads that share
its capped cores — and is gated on nothing; the external-boundary figure is
"source idle", which never exceeded 3.4% against a 20% ceiling.

### Per pass

| cores | pass | records/s | tm cores | % of cap | throttled | broker | src idle | src BP | headroom | vantage | status |
|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 1 | p1-asc | 104,628 | 1.00 / 1 | 100.0% | 100% | 0.06 / 2.5 | 0.0% | 47.7% | 1382 s | 0.21% | OK |
| 2 | p1-asc | 255,216 | 1.99 / 2 | 99.7% | 100% | 0.15 / 2.5 | 1.4% | 33.0% | 479 s | 0.21% | OK |
| 4 | p1-asc | 469,146 | 4.01 / 4 | 100.4% | 100% | 0.23 / 2.5 | 2.6% | 22.5% | 185 s | 0.47% | OK |
| 4 | p2-desc | 510,046 | 4.00 / 4 | 100.0% | 100% | 0.25 / 2.5 | 3.4% | 22.8% | 166 s | 0.37% | OK |
| 2 | p2-desc | 273,149 | 1.99 / 2 | 99.5% | 100% | 0.14 / 2.5 | 1.2% | 31.5% | 418 s | 0.31% | OK |
| 1 | p2-desc | 107,712 | 0.98 / 1 | 97.9% | 100% | — | 0.0% | — | 1342 s | 0.15% | **CEILING** |
| 1 | sentinel | 113,145 | 1.00 / 1 | 99.7% | 100% | 0.06 / 2.5 | 0.0% | 42.3% | 1278 s | 0.35% | OK |

### The step ratios

| step | ratio | interval (from the spread of adjacent pairs) | range across passes | efficiency | **met the claim?** |
|---|---:|---|---|---:|---|
| **2 → 4 cores** | **1.853×** | **1.824 – 1.881** | 1.718× – 1.998× | **92.7%** | **No** — the lower bound is 91.2% of linear against a 95% floor |
| 1 → 2 cores | 2.426× | 2.402 – 2.451 | 2.256× – 2.611× | 121.3% | Yes, but superlinear — see below |

The claim is judged on the **lower bound** of the interval, not the point: a
ratio that *might* be linear has not been shown to be. Both steps also carry
their adjacent-in-time pairs, which is what actually cancels rig drift:
2→4 pairs were 1.838 and 1.867 (median 1.853, spread 1.6%), 1→2 pairs 2.414
and 2.439 (median 2.427, spread 1.0%). The adjacent medians and the
mean-of-means agree to three decimal places, so the drift discussed below did
not land in the answer.

`prove.py report` exited non-zero on this table and the chain ended
`FAIL at report`. That is the harness doing its job: the measurement is sound
and the claim is not met, which is a result about the pipeline rather than a
table to publish.

### Where it stops

- **The one-core case is where the rig stops being about the worker.** Its
  second pass was refused as a **CEILING**: the broker hit its container memory
  limit 323 times inside the window (17,195 file-page refaults) while the
  worker sat at 97.9% of its cap — so for that window the broker was reading
  the backlog off disk and was the constraint. The pass is measured, kept and
  reported, and excluded from every ratio. The refusal names the fix
  (`kafkaMemory` 3072m → about 4864m); it was **not** applied, because the VM
  has 7 838 MiB and the four-core worker plus job manager already claim about
  4 800 MiB of it, and because tuning after seeing the number turns a curve
  into a story.
- **Four cores is the top of what this VM can hold.** Worker 3072m + broker
  3072m + job manager ~1700m ≈ the whole VM. An eight-core case was not run.
- **The broker is nowhere near its CPU ceiling**: 0.24 of 2.5 cores at the
  four-core case. If this pipeline were pushed wider, memory, not broker CPU,
  is what would give first.

## Was it correct? What the completeness check asserted

Completeness is a separate 6,000,000-trade drain at the baseline case, drained
to the last record, run twice: once clean and once with the task manager
`docker kill`ed at 40% of the backlog (it was killed at 3,287,711 committed
records, and the job came back on a replacement worker). **Every expected value
comes from the input** — `blockmv.Reference` is a single-threaded restatement
of the specification that reads the generator's seed, never the pipeline — and
**nothing is compared within a tolerance.**

The sixteen assertions, all of which passed in both arms:

| # | assertion | why a miss matters |
|---|---|---|
| 1 | distinct keys on `positions-by-symbol` = 32 | a key nobody intended, or one that never arrived |
| 2 | distinct keys on `positions-by-account` = 64 | same |
| 3 | records on `positions-by-symbol` ≥ 18,000,000 allocations | the sink is at-least-once; fewer means loss |
| 4 | records on `positions-by-account` ≥ 18,000,000 | same |
| 5 | **final position exact for every one of the 32 symbols** | a lost or double-counted record |
| 6 | **final position exact for every one of the 64 accounts** | same |
| 7 | Σ symbol positions = Σ account positions = the blocks' signed quantity (760,368) | the two aggregation paths over the same input, located |
| 8 | 12 ten-second windows predicted by the input | the window set is not the input's |
| 9 | distinct `(symbol, window)` pairs on `mv-by-symbol` = 384 | a missing or extra window |
| 10 | distinct `(account, window)` pairs on `mv-by-account` = 763 | same |
| 11 | **every repeated `(symbol, window)` record byte-identical** | a replay produced a *different* answer |
| 12 | **every repeated `(account, window)` record byte-identical** | same |
| 13 | **market value exact for all 384 `(symbol, window)`** — position at the close × last price at or before the boundary, in integer cents | any arithmetic, ordering or watermark error |
| 14 | **market value exact for all 763 `(account, window)`** | same |
| 15 | for every window, Σ over accounts of quantity and of market value = Σ over symbols — **read off Kafka alone, not through the manifest** | the two windowed paths disagree |
| 16 | both windowed topics carry all 12 windows | a path stopped early |

### How the *windowed* outputs were proved, specifically

A windowed number can only be asserted exactly if the set of windows that fire
and the value in each are functions of the input. Three things make that true
here, and each is enforced rather than hoped for:

1. **Every Kafka partition carries a non-decreasing timestamp series** — trades
   are produced with an explicit partition and ascending index, prices with one
   symbol group per partition — and the producer is idempotent so a retry
   cannot reorder a partition. A source's per-split watermark is therefore
   exactly *(highest timestamp seen − 1)*, and the job's watermark is the
   minimum over every split. The last window that can fire is then fixed by the
   backlog: `Reference.finalWatermark` computes it and the verifier demands
   exactly those windows, no more and no fewer.
2. **A record is counted at the boundary it belongs to, not at the boundary
   that happens to be open.** "The position at the window close" and "the last
   price at or before the boundary" are both inclusive of the instant itself,
   so each operator keeps its settled state plus a small per-boundary pending
   bucket. A trade that arrives out of order between partitions — which happens
   constantly, since eight partitions are read at different speeds — lands in
   its own bucket. Without this, a record timestamped one millisecond *after* a
   boundary could still be folded into that boundary's close, because it is
   processed before the watermark that follows it.
3. **A boundary is closed only once the price stream has demonstrably passed
   it** — a price with a strictly later timestamp for that symbol proves no
   further price at or before the boundary can arrive. That fact lives in keyed
   state, so it survives a restore. See "what was refused", below, for why this
   is not left to the price source's watermark.

The killed arm is the proof that the guarantee configured is the guarantee
obtained. It shows exactly the shape an at-least-once idempotent sink should
have: `positions-by-symbol` came back with 18,203,554 records against
18,000,000 allocations and still gave the exact final position for all 32
symbols; `mv-by-symbol` came back with 416 records covering exactly 384
distinct `(symbol, window)` pairs, and every one of the 32 repeats was
byte-identical to the record it repeated.

## What was refused, and why

| what refused | what it said | what was done |
|---|---|---|
| **The job itself, at submit** | `key-group arithmetic drifted from Flink for AAAL: 0 vs 1` | The job asserts at startup that its own copy of Flink's key-group hash agrees with `KeyGroupRangeAssignment` for every key. The copy, written from memory, had dropped murmur3's `^ 4` length mix and got the negative branch wrong. Re-derived from the bytecode of `flink-core-1.20.1`. It refused on its first run. |
| **The completeness verifier, killed arm** | 15 assertions failed: **zero** records on both windowed topics | Two separate defects, found by measurement rather than guessed — see the next section. Fixed, and both arms then passed 16/16. |
| **The tiny proof, one-core case** | `garbage collection took 11.8% of this case's capacity (ceiling 11%)` | The one-core worker had 1536m and was collecting five times as often per record as the four-core worker. `tmMemoryBase` 1024m → 2048m with `tmMemoryPerCore` 512m → 256m: the **per-subtask** term stays constant at every case and the four-core case keeps the identical 3072m it had already run clean at, while one core goes from 1536m to 2304m. GC fell to 7.6%. |
| **The suite, one-core `p2-desc`** | `the broker hit its memory limit 323 times inside the window … the broker is the constraint here, not the worker` | Kept as a measured **CEILING** row, excluded from the ratios, reported above. Not tuned away. |
| **`prove.py report`** | `CLAIM NOT MET: 2->4 returned 1.853x of an ideal 2x — 92.7% of linear, floor 95%` | Reported as the headline. Exit code 1; the chain ends `FAIL at report`. |
| **The guard self-test** | 38 of 38 guards broken on purpose fired as expected, including the two that must *not* fire | Nothing — that is the pass condition. It includes wrong CPU cap, busy cluster, truncated backlog, dead sampler, no running job, GC ceiling, starved broker, differing graph shape, spread ceiling, sentinel drift, host-side watcher, disk projection and warm-up scatter. |
| **`prove.py replay`**, before every command | re-derived all 14 recorded suites, 22 step verdicts, 10 case verdicts and 6 configurations under the live thresholds | Nothing changed in the harness, so nothing to fix — the check ran and passed each time. |

### The two windowed-output defects, and how each was pinned down

Neither was diagnosed by argument. Both were pinned by changing one thing and
measuring both arms.

**Defect 1 — the watermark lagged by most of the backlog.** Windows fired only
in the last third of the clean drain. A source's watermark is the minimum over
its partitions, and a partition's watermark only advances as its records are
emitted, so the watermark trails by about one fetch batch per partition. At the
8 MB `max.partition.fetch.bytes` I had set, that is roughly half a million
lz4-compressed records per partition — four million across the eight, or eighty
seconds of event time. One rig, one build, one variable:

| `max.partition.fetch.bytes` | window rows on Kafka at t = 18 s / 34 s / 66 s of a 90 s drain |
|---|---|
| 8 MB | 0 / 160 / 384 — nothing until the last third |
| 1 MB | 64 / 160 / 384 — progressive |

**Defect 2 — a restarted worker never fired another window.** A source's
watermark *generator* is not checkpointed. The price topic is small and fully
consumed in the first second of a drain, so after the kill the price splits
were restored at their end, no further price record ever arrived, and a
monotonous-timestamp generator sat at `Long.MIN_VALUE` for the rest of the run
— pinning the whole pipeline's watermark, because a co-processed watermark is
the minimum of its two inputs. The fix moves the fact that has to survive a
restore out of the source and into keyed state: the price stream now emits no
watermark constraint at all (`Long.MAX_VALUE - 1`), and the symbol operator
closes a boundary only once it has seen a price for that symbol with a strictly
later timestamp. Measured with the same kill afterwards: windows fired
progressively and survived the restart — 416 symbol rows over 384 distinct
pairs, 827 account rows over 763, i.e. the replayed windows re-emitted
identically.

## What I did not explain

Three things, stated as unknowns rather than dressed in a mechanism.

1. **Why 2→4 returns 92.7% of linear and not ≥95%.** Per-core throughput falls
   7.3% from two cores to four (132,091 → 122,399 records/s/core). I ruled out,
   with evidence from the same table:
   - *the worker not being the constraint* — 99.6% and 100.2% of cap, 100% of
     CFS periods throttled in both cases;
   - *the broker* — 0.15 and 0.24 of 2.5 cores, and source idle 1.3% → 3.0%
     against a 20% ceiling, so the input side is not starving the worker;
   - *memory* — GC is **lower** at four cores (2.9%) than at two (5.9%), so the
     wider case is not the starved one; per-subtask memory was held at 256m at
     every case and the fixed base at 2048m;
   - *key skew* — 32 symbols and 64 accounts land 8/8/8/8 and 16/16/16/16 at
     parallelism 4, asserted against Flink's own key-group assignment at
     startup;
   - *graph shape* — five vertices with identical ship strategies, read off the
     running plan and compared across every case;
   - *backlog running out* — 448 s and 175 s of headroom at window close.

   What is left is bounded but not identified. This host's own cores, measured
   in the same image under the same caps by `probe/Spin.java`, give **98% of
   linear from 2→4 on a register-only loop and 75% on a memory-bound one**. The
   pipeline's 92.7% sits inside that band. That is a ceiling, not a cause — I
   have not run a controlled experiment on this pipeline that isolates memory
   traffic, so any claim that cache or memory bandwidth is *the* reason would
   be a story. **I do not know yet.**

2. **Why 1→2 is superlinear (2.43×, 121% of linear).** The measured shape is
   that CPU per input record is *worst at one core*: 9.18 µs at one core, 7.53
   at two, 8.19 at four. The one-core case spends 22% more CPU per record than
   the two-core case while sitting at 100.0% of its cap. The obvious candidate
   is that everything the JVM does off to the side — GC threads, JIT compiler
   threads — plus five vertices' worth of operators all have to share a single
   capped core, and that fixed cost is amortised by the second core. I did not
   test it (the controlled version would be one core at parallelism 2, or a
   run with compilation and GC thread counts pinned), so it stays a hypothesis.
   What the number *does* establish is the skill's own warning: **the baseline
   is a case, not a reference point**, and here it is the structurally weakest
   one. That is why the headline is the 2→4 step.

3. **Why the whole rig ran faster later in the suite.** The descending pass was
   7.0% faster than the ascending pass at two cores and 8.7% faster at four,
   and the sentinel — the baseline case measured once more at the very end —
   came back +7.8% above its first measurement. Something warms or accumulates
   across a 29-minute suite on this laptop. I did not identify it. It does not
   move the answer, because both step ratios were also computed from
   adjacent-in-time pairs and those agree with the mean-of-means to three
   decimals, but a reader is entitled to know the rig drifts by about 8% over
   half an hour.

## Wall clock

| | |
|---|---|
| **The accepted chain** (`prove.py all --quick`: up → preflight → completeness → tiny proof → fill → suite → report) | **47.1 min** — 17:08:42 to 17:55:49 |
| Everything before it — building the job, and the work that had to be redone | **76 min** — 15:52 to 17:08 |
| Teardown and assertion that nothing survives | 5 s (`fstrim` returned 7.9 GiB to the host) |
| **Total, 15:52 to 18:00** | **2 h 08 min** |

Inside the chain: preflight 99 s, completeness 434 s, tiny proof 482 s, fill of
160,000,000 records 73 s, suite 1,737 s, report 0 s.

The 76 minutes before it break down as **24 minutes of first-time work**
(reading the harness contract, writing the job, generator, reference and
verifier, first stack-up and preflight) and **52 minutes of rework**:

| rework | cost |
|---|---|
| completeness run 1, refused at submit by the job's own key-group assertion | 2 min |
| completeness run 2, killed arm lost every windowed output | 8 min |
| three diagnostic probes and the two fixes they justified | 28 min |
| completeness run 3 (passed) — a re-run of a step that had already run | 8 min |
| tiny proof run 1, one-core case refused as a GC ceiling | 8 min |
| tiny proof run 2 (passed) — a re-run | 8 min |

Note that the 47-minute chain re-ran completeness and the tiny proof from
scratch on the same build, which is 15 of its 47 minutes. That is the price of
one build hash across the gate and the table, and it is worth paying.

## What the pipeline actually does

```
block-trades (Kafka, 8 partitions)          block-trades-prices (Kafka, 8 partitions)
        │                                             │
        ▼                                             │
  Source: kafka-source ─▶ allocate                    ▼
  split each block across the 3 accounts       Source: priceticks-source
  it names, pro rata by weight, in whole
  shares, remainder by largest fraction
  and ties to the lower account id —
  the parts sum to the block exactly
        │                                             │
        ├──────────── keyBy(symbol) ⨝ keyBy(symbol) ──┤
        │                     │
        │                     ▼
        │            symbol-position-and-value
        │            · running position per symbol ──▶ positions-by-symbol
        │            · at each 10 s boundary:
        │              position at the close ×
        │              last price at or before it ──▶ mv-by-symbol
        │              and one contribution per
        │              account holding the symbol
        │                     │
        │                     ▼  keyBy(account)
        │            account-value-at-close ────────▶ mv-by-account
        ▼
  keyBy(account)
  account-position
  running position per account ──────────────────────▶ positions-by-account
```

Five vertices, three hash shuffles, identical at parallelism 1, 2 and 4 —
verified by reading the shape off the running plan on every case.

Design decisions a reader may want to argue with:

- **No window API.** The value asked for is not an aggregate over the interval;
  it is the state *at* the boundary. Event-time timers over explicitly bucketed
  state express that directly, and made the boundary semantics — inclusive of
  the instant itself — something written down rather than inherited.
- **Market value is computed in integer cents.** `qty × priceCents` is a long
  multiplication, so the expected value is exactly representable and can be
  asserted without a tolerance. No floating point appears anywhere in the
  arithmetic.
- **Account-level market value is assembled from per-symbol contributions**
  rather than by broadcasting prices to the account operator. Broadcast state
  is not ordered against the keyed stream's watermark, so a price from *after*
  the boundary could be in the state when the boundary closes. The contribution
  path keeps every value on the symbol key, where the price for that symbol
  already lives.
- **The key universe is chosen, not taken.** Flink murmur-hashes a key's
  `hashCode` into one of 128 key groups; 32 arbitrary symbols do not land
  8/8/8/8 across four subtasks, and an uneven split makes the busiest subtask
  set the pace — which reads on the table as the largest case sitting below its
  cap, indistinguishable from a real scaling shortfall. `Keys` therefore scans
  four-letter tickers and `ACCT####` ids in a fixed order and takes the first
  that fill an equal quota per subtask at parallelism 4. It is a declared
  property of the synthetic data, fixed before any measurement, asserted at
  job startup against Flink's own assignment.
- **Prices are pinned one symbol group per partition** so that no price
  partition is ever idle and each carries an ordered series — the ordering the
  "last price at or before the boundary" rule depends on.

### Shape of the load

| | |
|---|---|
| input | 160,000,000 block trades, 3.87 GB on the broker (lz4), 25.5 B/record |
| fan-out | 3 allocations per trade → **6 running-position records per input**, plus 96 windowed records per 10 s window (0.02% of the output) |
| keys | 32 symbols, 64 accounts, 2,048 (symbol, account) pairs |
| output at four cores | 489,596 input records/s → 2.94 M sink records/s |
| state | tiny — a few thousand entries per subtask; this is a shuffle-and-I/O pipeline, not a state-heavy one |

## Reproducing

```
cp -r job pipeline.json <dir> && cd <dir>
(cd job && JAVA_HOME=/opt/homebrew/opt/openjdk@17 mvn -B package)
nohup python3 ~/.claude/skills/prove-it-scales/harness/prove.py all --quick > results/all.log 2>&1 &
until [ -f results/DONE ]; do sleep 30; done
python3 ~/.claude/skills/prove-it-scales/harness/prove.py down
```

Raw results are under `results/`: `preflight.json`, `completeness.json`,
`tinyproof.json`, `selftest.json`, `suite.json`, `suite.txt`, `suite.md`,
`all.json`, `phases.log`, `harness.log`, `all.log` and the three generator
manifests. `JOURNAL.md` carries the step-by-step record, including the
interview questions there was nobody to answer and the assumption taken in
place of each.
