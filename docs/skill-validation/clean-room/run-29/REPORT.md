# Block trades → positions → market value at close

**Outcome: the harness refused the fixed input, and no throughput table was published.**
The pipeline is built and proved correct; `prove.py all --quick` stopped at the tiny
proof because a 50,000,000-record backlog is too small to measure this pipeline at four
cores on this machine. The refusal, verbatim, and the arithmetic behind it are in
*[What was refused](#what-was-refused)*.

| field | value |
|---|---|
| axis | one worker growing: one task manager container capped at N cores, parallelism N, N slots |
| API level | Flink DataStream API 1.20.1, hand-written operators — no SQL, no Table API |
| guarantee | state: **exactly-once checkpointing**; sink: **at-least-once, idempotent** by emitting the absolute position per key and the absolute market value per key per window |
| checkpoint interval | 5,000 ms |
| build hash | `6036a8f0b26de5d4` (completeness passed for this build) |
| passes per case | `--quick` = 2 per case; the suite never ran, so the numbers below are the tiny proof's **one pass per case** |
| study | scaling: every case configured identically |
| rate source | committed broker offsets on `block-trades-tiny` (never the engine's own meter) |
| CPU source | cgroup `cpu.stat usage_usec` at window open and close |

Scope, once: this is **one pipeline on one laptop**. Nothing here is a statement about
Flink, only about this job on this rig.

---

## The table

There is none. `results/DONE` reads `FAIL at tinyproof 17.0 min`, and the harness will
not start a suite behind a failed tiny proof. The rule the task set — *the input is
fixed; if the harness refuses it, say what it refused and stop rather than substituting
different sizes* — is why no table follows.

What did get measured, with **every per-case guard live** (cap read back from the
container, parallelism = cap = slots read back from the engine, job graph shape compared
across cases, window anchored on six commit boundaries, two vantage points, headroom at
close), is the tiny proof's two cases. One pass each, 30-second windows: a smoke
measurement, not a result.

| cores | records/s in | records/s out | tm cores | % of cap | throttled | broker cores | src idle | src back-pressure | GC % of capacity | **CPU µs / input record** | vantage |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 181,115 | 905,576 | 1.004 / 1 | 100.4% | 100% | 0.124 / 2.5 | 1.3% | 49.7% | 3.76% | **5.54** | 0.10% |
| 4 | 746,842 | 3,734,211 | 3.948 / 4 | 98.7% | 98% | 0.606 / 2.5 | 0.35% | 30.4% | 0.75% | **5.29** | 0.21% |

- **step ratio 1 → 4 = 4.124×** (the tiny proof's bound is 3.00–5.00, so it passed).
  One pass per case, no spread, no sentinel, 30 s windows: **not publishable**, and it is
  quoted here only because it is the only scaling measurement this run produced.
- The 2-core case never ran. The tiny proof measures only the smallest and largest case,
  and the suite that would have measured 2 never started.
- CPU per input record = worker cgroup CPU over the window ÷ records committed in the
  window. GC fraction = GC milliseconds in the window ÷ (window seconds × cores).
- Host ceiling for context, measured by the preflight probe in the same image under the
  same caps: register-only 1→2 = 96.9%, 2→4 = 97.8%; memory-bound 1→2 = 90.8%,
  2→4 = **76.1%**. No pipeline on this machine beats those.

---

## What was refused

**1. The fixed input.** Verbatim, from `results/all.log`:

```
REFUSED (rig): backlog 50,000,000 is short of the 72,817,104 records the 4-core case
needs at its measured 746,842 rec/s (warm-up + window + headroom, x1.5);
set backlog.count to at least that
```

The arithmetic, all of it measured on this rig with this build:

| quantity | value |
|---|---|
| four-core drained rate (committed broker offsets) | 746,842 rec/s at 98.7% of a four-core cap |
| the fixed backlog, in seconds of drain at that rate | 50,000,000 ÷ 746,842 = **67 s** |
| harness minimum for one measured case | warm-up to a flat trend ≥ **90 s**, then a window ≥ **60 s** spanning ≥ 6 commit boundaries, then ≥ 1 checkpoint interval of backlog still unread |
| what the tiny proof computed from its own shorter rules (44.2 s measured warm-up, 30 s window, ×1.5) | **72,817,104** records |
| what the suite's own rules would need (90 + 60 + 15 s, ×1.5) | ≈ **185,000,000** records; ≈ 124,000,000 even with no margin at all |

The four-core case cannot finish warming up before the backlog is gone: the mandatory
warm-up alone is 90 s and the whole drain is 67 s. This is not a threshold I can argue
with — it is the measurement window not fitting inside the data. Nothing about the
pipeline, the caps or the rig can be changed to fix it; only the backlog can, and the
backlog is fixed. So the run stops here, as instructed.

For reference, the harness's own shipped example pipeline is configured with a
160,000,000-record backlog for cases 2 and 4 — the same order of magnitude this
measurement says is needed, though it was not measured on this rig or this build.

**2. Broker memory (a rig parameter, fixed and re-run).** On the second chain attempt:

```
REFUSED (ceiling): the broker hit its memory limit 2,646 times inside the window
(98,560 file-page refaults): it was reading the backlog off disk, so the broker is the
constraint here, not the worker — raise caps.kafkaMemory from 4096m to about 6400m
```

Raised `caps.kafkaMemory` 4096m → 6400m exactly as the refusal named. The next attempt
recorded 0 limit hits in both cases. This is the harness's configuration, not the study's
input, so changing it does not substitute a different workload.

**3. My own tiny-proof backlog (mine to size, fixed and re-run).** The first chain attempt
refused with *"backlog drained (25,000,000 records) before the window closed"* on the
four-core tiny case. `tinyCount` is a harness parameter I choose, not part of the fixed
input; raised to 80,000,000 so the four-core case had runway, which is what let the chain
reach the guard that judges the real backlog.

---

## Completeness: what was asserted, with no tolerances

Build `6036a8f0b26de5d4`. A 1,500,000-trade backlog with its 15,000 prices, drained to the
last record at the baseline (1 core), **twice**: once clean, once with the task manager
killed mid-drain (`docker kill` at 733,463 of 1,500,000 committed, the job restored from
its checkpoint on a replacement worker). The expected answer comes from the generator's
manifest — record counts, per-key totals, and the full expected window table — never from
the pipeline. The verifier exits non-zero on any mismatch.

| | clean drain | worker killed mid-drain |
|---|---|---|
| positions-by-symbol | 1,500,000 records, 4 keys, **0 duplicates** | 1,605,804 records, 4 keys, 105,804 duplicates |
| positions-by-account | 6,000,000 records, 16 keys, **0 duplicates** | 6,445,034 records, 16 keys, 445,034 duplicates |
| mv-by-symbol | **596 records for 596 expected (key, window) pairs, 0 duplicates** | 640 records for the same 596 pairs, 44 duplicates |
| mv-by-account | **2,384 records for 2,384 expected pairs, 0 duplicates** | 2,560 records for the same 2,384 pairs, 176 duplicates |
| verdict | `COMPLETENESS OK` | `COMPLETENESS OK` |

### The windowed outputs specifically

For `mv-by-symbol` and `mv-by-account` the check asserts, with no tolerance anywhere:

1. **The exact set of (key, window end) pairs.** 149 windows of ten seconds per key for a
   1,500,000-trade backlog — the last window cannot close, because its end lies beyond the
   highest watermark either stream can produce, so the count is `N/10000 − 1` and is
   computed from the input, not observed. 149 × 4 symbol keys = 596 pairs; 149 × 16
   account keys = 2,384. A missing pair and an extra pair are both failures.
2. **Every field of every record** — not the last record per key, *every copy* — equal to
   the manifest: the position at the close, the price in cents, and the market value in
   cents. Market value is `position × price` in integer cents, so there is nothing to
   round and no tolerance to hide behind.
3. **Price at close, not an average.** The manifest's price for a window is the last price
   whose event time precedes the window end. Prices are stamped at `…+50 ms` so no price
   ever falls on a boundary and "at or before the boundary" has one reading.
4. **Duplicates counted and reported rather than tolerated.** Zero on the clean drain. After
   the kill, 44 and 176 — and each duplicate carried the *identical exact triple*, which is
   what makes an at-least-once sink safe here.

On the position topics the check asserts: the distinct key set is exactly the four symbols
and the sixteen `ACCn/SUB1|SYMBOL` keys predicted in the interview; **every block in the
input appears for each of its five keys** (tracked as a bitset over block indices, so a
lost block is caught even if the totals happened to balance); the final value per key
equals the manifest total exactly; the symbol position equals the sum of its four account
legs exactly (two paths over one input); and each record's key matches the symbol of the
block it names.

Position values *between* the first and last record are deliberately **not** asserted
record-by-record: eight partitions interleave, so the running total after an individual
block depends on arrival order and is not predictable. The order-independent statement is
the windowed one — a window's position is fixed by event time — and that is where the
per-record equality is enforced.

---

## Key skew: four symbol keys across eight partitions at four cores

**No measurable skew — but that is a property of a value chosen before the run, not luck.**

Where the keys land, computed with Flink's own `KeyGroupRangeAssignment` (`scaletest.KeySpread`):

| maximum parallelism | symbols (4) at parallelism 4 | account keys (16) at parallelism 4 |
|---|---|---|
| 128 (Flink's default) | 1 / 1 / 1 / 1 | **2 / 4 / 7 / 3** |
| 1616 (this job) | 1 / 1 / 1 / 1 | 4 / 4 / 4 / 4 |

With the default the *symbols* would have been fine and the *account keys* would not: one
subtask would have owned 44% of them where 25% is even, and that subtask carries four
records per trade against the symbol path's one. 1616 is the smallest maximum parallelism
that divides both key sets evenly over 1, 2 and 4 subtasks. It is fixed in the job, before
any measurement, and identical in every case, so nothing about it varies with cores.

Measured on a running four-core case (60,000,000-record backlog, 30 s of reporter samples,
outside the harness's table):

| operator | records/s per subtask | spread |
|---|---|---|
| `symbol-positions` (4 keys) | 117,439 / 117,432 / 117,436 / 117,435 | **0.0%** |
| `account-positions` (16 keys) | 469,732 / 469,717 / 469,743 / 469,728 | **0.0%** |
| `symbol-positions` busy time | 49.9% / 51.1% / 48.3% / 49.5% | 5.8% |
| `account-positions` idle time | 9.5% / 8.5% / 5.6% / 8.6% of each second | — |

Load per key is uniform by construction (the generator round-robins the four symbols and
splits every block across all four accounts), so an even key-group assignment gives an
even subtask load, and it did: the record rates agree to four significant figures.

---

## What I did not explain

- **The 1 → 4 ratio is 4.124×, three percent above linear.** Superlinear is a defect
  report, not a result. Its whole size is visible in one column: the pipeline spends
  **5.54 µs of CPU per input record at one core and 5.29 µs at four**, 4.6% cheaper per
  record with four cores. *I do not know why.* Things that differ between the two cases and
  that I did **not** test with a controlled comparison: the one-core case is throttled in
  100% of its cgroup periods against 98% at four; its GC takes 3.76% of capacity against
  0.75%; its source sits back-pressured 49.7% of the window against 30.4%, four tasks
  sharing one core. Each is a hypothesis. None is offered as the cause, and the ratio is a
  single pass per case in any event.
- **Why Kafka's consumer read its partitions as unevenly as it did** with large fetches. The
  effect is measured and fixed (below); the mechanism inside the consumer is not something I
  established.
- **Whether the four-core rate would hold on a 185M-record backlog.** It was measured on an
  80,000,000-record topic; a larger one changes the broker's page-cache pressure, and this
  rig has already shown that mattering (refusal 2). Untested.

---

## The pipeline

One Flink job, four vertices, identical shape at every parallelism (the harness reads the
shape off the running plan and compares):

```
Source: trades-source → allocate ─────HASH──┐
                                            ├──→ symbol-positions  → sink positions-by-symbol
Source: prices-source → price-fanout ─HASH──┘                      → sink mv-by-symbol   (side output)
                                            ├──→ account-positions → sink positions-by-account
                                            └──                    → sink mv-by-account  (side output)
```

- **Allocation.** Every block names all four accounts; the split is largest-remainder —
  `base = qty/4`, and the first `qty % 4` accounts get one more — so the four legs sum to
  the block quantity exactly, in integers, by construction.
- **Positions.** `symbol-positions` keys by symbol (4 keys), `account-positions` by
  `account|symbol` (16 keys). Each emits the *absolute* running position per key on every
  trade, which is what makes the sink idempotent without transactions.
- **Market value at close.** Each keyed operator holds an event-time timer chain on the
  ten-second boundary. A trade's quantity is added to the delta of the window its event
  time falls in — not the window that happens to be open when it arrives — so eight
  interleaving partitions cannot move a quantity between windows. Prices are filed under
  the window they belong to and read back when that window closes, one entry per window per
  key, O(1) in and O(1) out; the last price carries forward into a window with none. At the
  close the operator emits `position × price` for that key, once, on a side output.
- **Fan-out** is 5 output records per input (1 symbol position + 4 account positions), plus
  20 windowed records per ten seconds of event time — 0.002 per input. The two-vantage
  guard read 0.10% and 0.21% disagreement against that figure.

## Four defects the correctness work found before the chain

Each cost a re-run and each is a measurement, recorded here because the fix is only as
trustworthy as the evidence behind it:

1. **Kafka record timestamps stamped with the 2025 event time.** The broker's seven-day
   time retention deleted the whole backlog between the two completeness drains — earliest
   offset equal to latest, the second drain reading nothing. The harness's backlog check
   compares the *log end* to the manifest and cannot see this. Fixed by letting the producer
   stamp wall-clock time; event time lives in the payload.
2. **Windows read the current position, not the position at the boundary.** With the source
   running far ahead of its own watermark, every window closed late and reported the
   position at the moment the watermark arrived. Fixed with per-window deltas.
3. **The source ran 1.3M records ahead of its watermark** at 8 MB per-partition fetches, so
   all 149 windows fired in one burst at the very end of a drain. The watermark of a source
   subtask is the *minimum* over its splits, so the partitions must advance together: 64 KB
   fetches with a 20,000-record poll cap take all eight partitions' worth in one poll, and
   the per-partition offset spread fell to ≤ 15,000 records.
4. **A restored split parked at its end offset pins the watermark at `Long.MIN_VALUE`.**
   After a killed worker, 94 of 149 windows had closed and not one more ever did. Fixed with
   a 20 s split idle timeout — which was itself tried first *without* fix 3 and produced
   61,182 duplicate position records and eight wrong window totals, because a back-pressured
   source stalls every split at once and the first to resume carries the watermark past the
   others. The operator now throws on a record that arrives after its window has closed,
   rather than folding a wrong number in quietly.

---

## Interview: the questions, and the assumptions taken in place of answers

No human was available, so each question below was answered by assumption and the
assumption is stated.

| # | question | assumption taken |
|---|---|---|
| 1 | What is the input event and what comes out? | Given by the brief: a block trade naming a symbol, a quantity and the accounts to allocate across; out come running positions by symbol and by (account, symbol), and market value at every ten-second window close for both. |
| 2 | Does one input become several outputs? | **5 per input** — one symbol position and four account positions per trade — plus 20 windowed records per ten seconds of event time. Positions are emitted on every trade rather than on a timer, because "maintains running positions" reads that way and it makes the sink idempotent. |
| 3 | What are the keys and how many? | Given: 4 symbols, 16 (account, symbol). One sub-account per account (`SUB1`), so the account leg is `ACCn/SUB1`. |
| 4 | What has to be exactly right, and which two settings? | Everything, with no tolerance. **Exactly-once checkpointing** for the keyed state; **at-least-once idempotent sink** — absolute position per key, absolute market value per key per window. Tested by killing the worker. |
| 5 | Who watches, and what must they believe? | Engineers: correctness first, capacity second. Hence completeness gates the table, and the table was never published without it. |
| 6 | Where does it run? | Given: this laptop, Docker, everything local, every container prefixed `bt30`. |
| 7 | What claim do you want to make? | Written verbatim before building: *"On one task manager, this pipeline's drained throughput scales linearly with cores from 1 to 4, with positions and ten-second market-value windows exactly correct — including across a worker killed mid-drain."* The second half is proved; the first half was not measurable on the fixed backlog. |
| 8 | Which axis? | One worker growing — cases 1, 2, 4, with parallelism = cap = slots. |
| 9 | Which API level? | Given: DataStream, hand-written operators. |

Assumptions about the input that the brief did not pin down:

- One trade per millisecond of event time from `2025-01-01T00:00:00Z` (a multiple of ten
  seconds, so windows are aligned to it); symbols round-robin, so all four carry exactly a
  quarter of the blocks; quantity `100 + splitmix64(seed, i) % 900`.
- One price per hundred trades, at `T0 + 100j + 50 ms`, symbols round-robin, opening prices
  100.00 / 200.00 / 300.00 / 400.00 in integer cents then a deterministic ±10c walk. The
  500,000 prices span exactly the same event-time range as the 50,000,000 trades.
- Trades go to partition `(i/4) % 8` and prices to `(j/4) % 8`, so every partition carries
  all four symbols with ascending timestamps — which is what makes per-split ascending
  watermarks correct.
- Prices live on their own eight-partition topic, `<trades topic>-prices`, filled by the
  same deterministic generator.
- "Price at close" = the last price with event time strictly before the window end.
- The completeness backlog is 1,500,000 trades (mine to size): 33 s at the baseline rate,
  about seven checkpoint intervals, enough for the kill to land at 35% and still leave a
  drain to finish.

---

## Wall clock

| phase | wall | |
|---|---:|---|
| build: design, generator, job, verifier, jar (06:36–07:01) | 25 min | |
| correctness work: six completeness drains and two kill probes, four defects found and fixed (07:01–08:21) | 1 h 20 m | redone |
| chain attempt 1 (`prove.py all --quick`), plus the fix between it and the next — refused: my tiny-proof backlog too small at four cores | 18 min | redone |
| chain attempt 2, plus the fix after it — refused: broker hit its 4 GiB memory limit at one core | 19 min | redone |
| **accepted chain (attempt 3, 08:58–09:15)** — preflight 18/18, completeness both arms, tiny proof both cases, then the refusal on the fixed backlog | **17.0 min** | |
| key-skew measurement, teardown, report (09:15–09:29) | 14 min | |
| **total** | **2 h 53 m** | |

The accepted chain is 17.0 minutes of the 2 h 53 m; 37 minutes went on the two chain
attempts that were re-run, and 1 h 20 m on the correctness work before any chain started.
Almost all of the rework was correctness rework: four re-runs came from defects the
completeness check found, one from a rig parameter the harness named for me, and one from a
tiny-proof backlog I had sized by guess before anything had been measured.

Teardown asserted: no container, volume or network with the `bt30` prefix survives, no host
process watching the run survives, `fstrim` returned 16.3 GiB to the host.
