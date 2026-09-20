# A 2 KB order under the harness

What happens to the demo's `PositionsJob` when each order is about six times
larger: does it still scale from 1 to 4 cores, and why is each order slower?

## What changed

Commit `99870a2` adds optional filler fields to every allocation, `f01..fNN`
each holding `XXXX`, carried through `SplitByAllocation` into
`PositionUpdate` and across the shuffle to the account aggregation. With
`FILLER_FIELDS` unset the order is byte-identical to before (golden test in
`PayloadSizeTest`); the aggregation's output, `PositionState`, carries no
filler, so the sinks are unchanged.

| arm | `FILLER_FIELDS` | mean order, JSON bytes | on disk, lz4 | config |
|---|---:|---:|---:|---|
| base | unset | 326.5 | 27.6 B/record | [`pipeline-base.json`](payload-2k/pipeline-base.json) |
| 2 KB | 32 | 2,034.5 | 46.6 B/record | [`pipeline-f32.json`](payload-2k/pipeline-f32.json) |

Mean bytes are over 20,000 generated orders (`StageBench`); on-disk bytes are
the tiny proof's `disk.inputBytesPerRecord`.

The two configs differ in exactly four places: the project name,
`FILLER_FIELDS=32` on the generator and manifest commands, the suite backlog
(150,000,000 → 40,000,000) and the tiny-proof backlog (60,000,000 →
20,000,000). The backlogs were cut because each 2 KB record takes more disk. Everything else
is the same, and the same as arm A of the
[demo under the harness](demo-under-harness.md): 8 partitions, 5 s
checkpoints, broker capped at 2.5 cores and 4g, worker memory 1024m + 1024m
per core. All three tiny proofs below ran build `4b4039306a87`.

KB below is JSON bytes ÷ 1,000 at the mean order size.

## Result: the 2 KB order scales

| tiny proof | 1c orders/s | 1c KB/s | 4c orders/s | 4c KB/s | 1→4 | verdict |
|---|---:|---:|---:|---:|---:|---|
| 2 KB, 2026-09-14 | 24,536 | 49,918 | 38,175 | 77,667 | 1.556 | **FAIL** (bounds 3.00–5.00) |
| **2 KB, 2026-09-15** | 22,733 | 46,250 | 103,908 | 211,400 | **4.571** | **PASS**, guard self-test PASS |
| **base, 2026-09-15** | 68,203 | 22,268 | 294,691 | 96,217 | **4.321** | **PASS**, guard self-test PASS |

The base arm's rates match arm A's tiny proof of 2026-09-10 on the same config
(1c 72,214, 4c 295,132 orders/s, ratio 4.087), which ran an earlier build,
`d465a077`.

A 2 KB order is **3.0× slower** at 1 core (68,203 → 22,733 orders/s) and
**2.8× slower** at 4 cores, but moves **2.1–2.2× more bytes per second**.

## Why 2026-09-14 failed

The 4-core rate was not steady. Committed offsets per 5 s, from the tiny
proof's own ticks:

| run | 4-core orders/s, consecutive 5 s intervals |
|---|---|
| 2026-09-14 | 69,517 · 65,575 · 61,092 · 73,837 · 57,350 · 26,395 · 46,910 · 34,065 · 61,453 · 7,183 · 15,477 · 21,551 · 21,655 |
| 2026-09-15 | 98,105 · 100,407 · 103,939 · 104,283 · 104,494 · 98,325 · 104,363 · 104,018 · 104,652 · 107,602 · 105,178 |

The 1-core case was steady on both days (22,333–25,521 and 19,642–24,347).
The window on 2026-09-14 (108.2–139.3 s after submit) fell in the collapse,
so the measured 38,175 is not a ceiling.

**Cause: not proven.** The only change known between the two days is host
memory. On 2026-09-14 the host was swapping heavily with a browser holding
several GB; that was observed during the session but **no file records swap
during that run**. On 2026-09-15 the browser was closed and host swap read
1,066 MB used of 2,048 MB when the rerun started. Same rig, jars and config.

Memory was still tight on 2026-09-15, in both arms, without slowing the rate
inside the window (from `sampler.log` and `vm.log`, sampled every 2–5 s):

| 4 cores, 2026-09-15 | 2 KB | base |
|---|---|---|
| Docker VM free memory | 111–161 MB | 126–396 MB |
| broker page-cache refaults per 5 s | 2,323–7,360 | 4,483–75,717 |
| checkpoints failed | 1 | 1 |

The base 4-core rate dipped to 138,779 orders/s at 96 s, after its window
closed at 80.9 s, alongside the largest refault burst (75,717).

## Where the pipeline is busy

From the tiny proofs' back-pressure samples, fraction of time:

| 2026-09-15 | source busy | source back-pressured | account aggregation busy | symbol aggregation busy |
|---|---:|---:|---:|---:|
| base 1c | 0.73 | 0.26 | **0.98** | 0.33 |
| base 4c | 0.49 | **0.51** | 0.86 | 0.29 |
| 2 KB 1c | **0.94** | 0.05 | 0.69 | 0.12 |
| 2 KB 4c | **0.94** | 0.05 | 0.84 | 0.15 |

At 326 B the source waits on the account aggregation. At 2 KB the source
chain (Kafka read, both flatMaps, serializing the updates) is the busy
part and is rarely held back.

## Why a 2 KB order is slower

Each stage of the per-order path timed alone: one thread, its own JVM, the
rig's image and jars, `--cpus 2` to leave room for JIT and GC threads beside the
timed one, 8 s warm-up, 12 s measured, 3 repeats, median.
[`StageBench.java`](payload-2k/bench/StageBench.java),
[`stages.log`](payload-2k/bench/stages.log).

| stage | 326 B µs/order | 2 KB µs/order | added µs | share of added |
|---|---:|---:|---:|---:|
| Kafka fetch: lz4 batch decompressed, value copied | 0.11 | 0.35 | 0.23 | 1% |
| bytes → String | 0.03 | 0.20 | 0.17 | 1% |
| `ToSymbolUpdate`: parse + 1 update | 1.29 | 7.88 | 6.59 | 33% |
| `SplitByAllocation`: parse again + 4 updates | 1.45 | 8.35 | 6.90 | 35% |
| serialize the order's 5 updates | 0.35 | 2.19 | 1.84 | 9% |
| deserialize them at the aggregations | 0.70 | 4.69 | 3.99 | 20% |
| **per-order path** | **3.93** | **23.66** | **19.73** | |

Splitting the parse, same method:

| 2 KB order | µs/order |
|---|---:|
| tokenize only, nothing bound | 4.62 |
| parse, filler skipped instead of bound | 4.89 |
| parse, as the job does | 8.01 |

So **68% of the added cost is the two parses** (13.49 of 19.73 µs), and most of each parse is
reading the bytes: binding the filler into a map adds 3.12 µs over skipping
it. **About 30% (5.83 µs) is carrying the filler across the shuffle.** Kafka
fetch, including lz4, and bytes → String are 2% together. Spread across the three repeats was 1–5% for every 2 KB stage except
deserialize, 38% from one slow repeat; its median is used.

Batches were built the way the generator's producer builds them (262,144-byte
`batch.size`, lz4): 123 orders per batch at 2 KB, 714 at 326 B.

**This is not a profile of the running job.** At 1 core the pipeline spends
43.99 µs per 2 KB order and 14.66 µs per 326 B order (1 ÷ rate), 29.33 µs
more — against 19.73 µs in the stages above. What the other ~10 µs is has not
been measured; the pipeline also runs the aggregations, sinks, checkpoints and
the Kafka client on that core.

### The parse work alone scales

The job's two flatMaps plus serializing the updates, on 1, 2 and 4 threads
with `--cpus` equal to threads, 10 s warm-up, 20 s measured, 3 repeats,
median. No Kafka, no Flink runtime.
[`PayloadBench.java`](payload-2k/bench/PayloadBench.java),
[`matrix.log`](payload-2k/bench/matrix.log).

| order | 1 CPU | 2 CPUs | 4 CPUs | 1→4 |
|---|---:|---:|---:|---:|
| 326.5 B | 296,768 | 600,334 | 970,655 | 3.27 |
| 2,034.5 B | 41,892 | 111,299 | 206,696 | 4.93 |

The 1-CPU case shares its core with JIT and GC threads, which may inflate the
2 KB 1→2 step (2.66) — not tested; the 326 B 2→4 step (1.62) was not investigated.
Neither number is a model of the pipeline's ceiling.

## Parse once: measured

The first option below was tried. Commit adds `SplitOnce`, which parses the
order once and emits the symbol update on the main output and the account
updates on a side output, and `PositionsJobParseOnce`, which wires the same two
aggregations to it. Both arms ran from **one jar** (`839fcde2930d`), back to
back on 2026-09-20, differing in two lines of `pipeline.json`: the project name
and the main class.

A side output rather than two operators chained off one parsed stream, because
with object reuse off Flink copies a record per downstream consumer, which could
cost more than the parse it saves. Equivalence is proved before any rig time by
`ParseOnceEquivalenceTest`: `SplitOnce` emits field for field what
`ToSymbolUpdate` and `SplitByAllocation` emit between them, at 326 B and at 2 KB,
filler included.

| tiny proof, 2 KB order | 1c orders/s | 4c orders/s | 1→4 | verdict |
|---|---:|---:|---:|---|
| parse twice (control) | 24,934 | 104,559 | 4.193 | PASS, guard self-test 38/38 |
| **parse once** | **30,553** | **130,087** | **4.258** | PASS, guard self-test 38/38 |
| gain | **+22.5%** | **+24.4%** | — | |

**The stage bench predicted this within 7%.** It put the second parse at
6.9 µs/order; the pipeline shows 40.11 µs/order falling to 32.73, a saving of
**7.38 µs**. That is the first stage-bench figure checked against the running
job, and the check holds — which is some evidence the rest of the stage table
is worth trusting, and none at all that the unexplained ~10 µs is.

**Scaling is unchanged: 4.258 against 4.193**, inside the ±4% this rig carries.
Parsing once removes constant work per order, so it buys throughput, not
scalability.

### The constraint moved

| fraction of time | control 1c / 4c | parse once 1c / 4c |
|---|---|---|
| source busy | 0.92 / 0.95 | **0.77 / 0.84** |
| source back-pressured | 0.06 / 0.04 | **0.22 / 0.14** |
| account aggregation busy | 0.61 / 0.84 | **0.83 / 0.89** |

The source chain was the constraint and no longer is: it drops to 0.77 busy at
one core while its back-pressure rises to 0.22, and the account aggregation
climbs to 0.83–0.89. **A second round of the same optimisation would return much
less.** What is worth attacking next is the shuffle and the aggregation, which
the stage table puts at about 30% of the added cost — the two remaining options
below.

Both arms: [`results-2026-09-20-once/`](payload-2k/results-2026-09-20-once/),
[`results-2026-09-20-f32/`](payload-2k/results-2026-09-20-f32/), with the configs
beside them.

## Options, not measured

Each targets a stage measured above; none has been tried in the pipeline, and
savings measured alone need not add up there.

| option | stage it targets | cost there |
|---|---|---:|
| carry only what the aggregation uses across the shuffle | serialize + deserialize | ~5.8 µs |
| carry the filler as one string instead of 64 | mostly deserialize | part of ~5.8 µs |
| skip the filler when parsing for the symbol side | building the filler map | ~3.1 µs |
| a binary format instead of JSON at the producer | both parses | ~13.5 µs |

A fan-out of a parsed `BlockTrade` to two chained operators is still not in
the list: with object reuse off, Flink may copy the record per output, which
could cost more than the parse it saves. The measured arm above avoids the
question by using a side output; whether chaining would have been cheaper is
still unverified.

## Files

| path | what |
|---|---|
| `payload-2k/results-2026-09-20-once/` | the parse-once arm, and `results-2026-09-20-f32/` its same-day control; `pipeline-once-2026-09-20.json` and `pipeline-f32-2026-09-20.json` differ in two lines |
| `payload-2k/results-2026-09-14-f32/` | the failed tiny proof, its manifests, completeness (passed, including a worker killed mid-drain), preflight, harness log |
| `payload-2k/results-2026-09-15-f32/` | the rerun, with `sampler.log` (host swap, container CPU and memory, checkpoints) and `vm.log` (Docker VM meminfo and vmstat) |
| `payload-2k/results-2026-09-15-base/` | the same-day control, same samplers; `completeness-2026-09-14.json` is from the base stack the day before |
| `payload-2k/bench/` | both benchmarks, their scripts and logs, `sampler.sh`, and `join.py`, which lines the 5 s rates up with the samplers |

The tiny proofs are in [`docs/runs/ledger.csv`](../runs/ledger.csv) as study
`payload-2k`. These are not clean-room runs, so they have no row in the runs
status table. User paths are redacted to `~`.
