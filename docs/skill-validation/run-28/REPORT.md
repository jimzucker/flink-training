# Block-trade positions and market value at close — how far does it scale?

**2 → 4 cores: 1.91× (95.4% of linear), interval [1.78, 2.04]. It does not clear
the claim.** The claim is judged on the interval's lower bound, and that bound is
89.0% of linear, not 95%. **1 → 2 cores: 2.13× (106.6%), interval [1.98, 2.33] —
that one clears it.** Scope: this is one pipeline on one worker, not a statement
about Flink.

Everything below is a **quick look** — `prove.py all --quick`, two passes per case
instead of three. Enough for a spread, not enough to publish a ratio.

| field | value |
|---|---|
| axis | **one worker growing** — one task manager container capped at N cores, parallelism N, N slots (not workers multiplying) |
| API level | **Flink 1.20.1 DataStream, hand-written operators** — `ProcessFunction`, `KeyedCoProcessFunction`, event-time timers. No SQL, no Table API |
| guarantee, setting 1 (state) | **exactly-once checkpointing**, 10 s, hashmap backend, filesystem checkpoint storage |
| guarantee, setting 2 (sink) | **at-least-once**, made idempotent by emitting the *absolute* position per key and the *absolute* market value per key per window |
| checkpoint interval | 10,000 ms |
| build hash | `437a14a2910b1df8` — completeness passed for this same hash |
| passes per case | 2 (+ the baseline a third time as the end-of-suite sentinel) |
| study | scaling: every case configured identically |
| rate source | committed broker offsets on `block-trades`, never the engine's own meter |
| CPU source | cgroup `cpu.stat usage_usec` at window open and close |

## The table

| cores | pass | records/s | tm cores | % of cap | broker | src idle | src BP | headroom | vantage |
|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | p1-asc | 191,972 | 0.96 | 96.2% | 0.09 / 2.5 | 0.1% | 50.1% | 1430 s | 0.26% |
| 2 | p1-asc | 395,754 | 2.00 | 99.9% | 0.19 / 2.5 | 1.1% | 46.5% | 615 s | 0.34% |
| 4 | p1-asc | 729,171 | 3.99 | 99.9% | 0.45 / 2.5 | 4.7% | 23.0% | 250 s | 0.48% |
| 4 | p2-desc | 786,237 | 3.99 | 99.7% | 0.46 / 2.5 | 5.6% | 20.0% | 238 s | 0.23% |
| 2 | p2-desc | 398,209 | 1.99 | 99.4% | 0.22 / 2.5 | 1.6% | 43.3% | 609 s | 0.22% |
| 1 | p2-desc | 177,612 | 1.00 | 99.6% | 0.11 / 2.5 | 0.0% | 44.4% | 1549 s | 0.31% |
| 1 | sentinel | 189,173 | 1.00 | 99.9% | 0.11 / 2.5 | 0.1% | 48.0% | 1438 s | 0.41% |

| cores | passes | mean records/s | spread | reportable |
|---:|---:|---:|---:|---|
| 1 | 3 | 186,252 | 7.7% | yes |
| 2 | 2 | 396,982 | 0.6% | yes |
| 4 | 2 | 757,704 | 7.5% | yes |

No case was a ceiling: the worker held **96–100% of its cap** at every case, the
broker never rose above 0.46 of its 2.5 cores, source idle stayed under 6%, the
backlog still held 238 s of work at the tightest window close, and the two
vantage points (committed source offsets vs sink growth ÷ 5) never disagreed by
more than 0.48%. Sentinel drift across the whole suite: **−1.5%**.

Source back-pressure of 20–50% is *internal* — inside a capped worker the source
thread waits on the aggregation threads sharing its cores. It is reported and
gated on nothing. The external boundary is source *idle*, and that is ≤ 5.6%.

## The step ratios, with intervals

| step | ratio | efficiency | adjacent pairs | interval (95%) | meets the claim? |
|---|---:|---:|---|---|---|
| 1 → 2 | **2.131×** | 106.6% | 2.062, 2.242 | **[1.975, 2.329]** → low bound 98.8% | **yes** |
| 2 → 4 | **1.909×** | 95.4% | 1.842, 1.974 | **[1.779, 2.038]** → low bound 89.0% | **no**, short by 11.1 points on the bound |

The burden is on the claim: a ratio that *might* be linear has not been shown to
be. The 2→4 point estimate sits right on 95%, and two passes cannot separate that
from 89%. Three passes might; this run did not buy them.

For context, measured on this machine in the same image and under the same caps
(preflight `hostScaling`), before any pipeline is judged:

| this host's own cores, 2 → 4 | of linear |
|---|---:|
| register-only loop | 98.1% |
| random read-modify-write over 4 MB/thread | **76.1%** |

The pipeline's 95.4% sits between those bounds, near the top of the range the
hardware allows for work that touches memory.

## Cost per record

| cores | mean records/s | per core | **CPU µs per input record** | output records/s | **GC, harness measure** | GC, true |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 186,252 | 186,252 | **5.29** | 931,262 | **8.76%** | 4.38% |
| 2 | 396,982 | 198,491 | **5.02** | 1,984,908 | **2.87%** | 1.44% |
| 4 | 757,704 | 189,426 | **5.27** | 3,788,521 | **2.19%** | 1.09% |

CPU µs/record = task-manager cgroup cores × 10⁶ ÷ records per second. Each input
record is one block trade and produces five output records (1 symbol position +
4 account positions), so the four-core case writes 3.8 M records/s to Kafka.

Two columns for GC because Flink 1.20 publishes an `All` aggregate collector
*alongside* `G1 Young`/`G1 Old`, and the harness sums every metric whose name ends
in `.Time` — so its figure is exactly double the real one. Example, one 4-core
sample: `{All.Time: 2081, G1 Young Generation.Time: 2081, G1 Old Generation.Time: 0}`.
The harness's 11% ceiling is calibrated against its own double-counted measure, so
the left-hand column is the one to compare with it. Either way, memory was not the
constraint in the accepted table.

The one-core case is the outlier: at one CPU the JVM classifies the machine as
client-class and picks the **serial** collector, and GC there is 4× the four-core
figure. This is why worker memory is `tmMemoryBase 3072m + tmMemoryPerCore 512m`
(3,584 / 4,096 / 5,120 MB) rather than a smaller per-core figure — see the journal.

## What the completeness check asserted

A separate run on a 12,040,000-record backlog drained **to the last record**,
twice: once clean, once with `docker kill` on the task manager at 35% of the
drain (it landed at 4,445,765 records committed, three checkpoint intervals in,
and the job came back on a replacement worker). Everything expected comes from the
generator manifest — computed from the input, never from the pipeline. **No
tolerances anywhere.**

| assertion | clean | worker killed |
|---|---|---|
| distinct keys = the interview's prediction | 8 symbols, 128 (account, symbol), 8, 16 | same |
| every per-key update counter walks 1..K with **no gap** | 0 rewinds | 8 + 128 rewinds, no gap |
| every final position = manifest, exactly | ✓ | ✓ |
| every (key, window) market value present, the **exact expected set** | 1,200 + 2,400 values | 1,200 + 2,400 values |
| every market value = manifest, exactly | ✓ | ✓ |
| market value = quantity × price, on the record | ✓ | ✓ |
| repeats of one (key, window) carry identical values | ✓ | ✓ |
| positions-by-symbol = Σ positions-by-account, per symbol | ✓ | ✓ |
| Σ market value by symbol = Σ by account, per window | 150 windows | 150 windows |
| record counts | 60,200,000 positions, 3,600 market value — exact | 60,917,596 positions (717,596 replayed, all accounted for by the gap walk), 3,600 market value |

**How the windowed outputs were proved.** The generator simulates the backlog it
wrote and emits, per symbol and per account, the market value at every window
close — 150 windows × 24 keys = 3,600 numbers in the manifest. Three things make
that answer well defined rather than a race:

1. **Event-time windows.** Timestamps live in the records, so the set of closed
   windows is a function of the input, not of how fast the drain ran. The backlog
   length is chosen so `count / partitions` lands mid-window (10,000k + 5,000 ms),
   which puts the last close 5 seconds clear of the final watermark — so "which
   windows closed" is not a knife-edge question about whether
   `forMonotonousTimestamps` emits `maxTs` or `maxTs − 1`.
2. **The close is a snapshot, not a reading of running state.** A watermark says
   "nothing further at or below W", not "the reader has stopped". Quantities are
   therefore accumulated as a delta per window close and folded in when that close
   fires; prices are held by their own close instant and the last one at or before
   the close is chosen when it fires. Neither is applied on arrival.
3. **The verifier compares the exact set and every value**, and flags any
   (key, window) the manifest did not predict as well as any it did not receive.

The check earns its keep: it found two real defects that the throughput columns
were perfectly happy with. Both are in `JOURNAL.md`.

## What was refused, and why

**By the harness, during the chain (two attempts thrown away):**

1. **Completeness, killed-worker arm** — 672 market-value records written of
   3,600, every missing window at the end of the drain, positions still exact.
   Cause: the price topic is 2,000× smaller than the trade topic and is read to
   its end in the first second, so after the restart the replacement worker
   resumed that source at its end offset, had nothing to emit, and its watermark
   restarted at `Long.MIN_VALUE`. `min(trades, prices)` then never advanced again
   and no window timer ever fired. Fixed with `withIdleness(15 s)` on the price
   source: an exhausted source must stop holding the watermark. This is exactly
   the class of bug that only a kill test finds.
2. **The tiny-proof fill** — `block-trades-tiny holds 151,268,997 records, wanted
   160,040,000`. That refusal is the generator's own read-back of what it wrote.
   Nine producers in one JVM at 128 MB of buffer each is 1.15 GB against `-Xmx1g`;
   the resulting `OutOfMemoryError` is an `Error`, not an `Exception`, so
   `catch (Exception)` missed it and a producer thread died in silence. Fixed by
   bounding the buffers (32 MB), catching `Throwable`, adding an uncaught-exception
   handler, and raising the generator heap. The read-back is why this was a
   refusal and not a wrong table.

**By the harness, deliberately, before the table existed:** `prove.py tinyproof`
broke 20 guards on purpose and every one refused — cap not applied, cluster still
busy, backlog not matching its manifest, sampler dead at startup, no job running,
fewer than 3 commit boundaries, zero rate, rate from the engine, vantage points
12% apart, no headroom at close, too few reporter samples, graph shape differing
across cases, spread past 20%, a case measured once, and five ceiling
classifications (94% of cap, 90% of cap, 40% source idle, 26% GC, a broker at its
memory limit). The self-test also reaped my own log watchers mid-run, which is the
rule working: a watcher started on the host is killed by the teardown it waits on.

**By `report`, at the end:** exit non-zero, so the chain reads `FAIL`, because
2→4 misses `scalingFloor`. The measurement is sound; the claim is not met. Those
are different verdicts and the harness keeps them apart.

**Not refused, and worth saying so:** nothing was a ceiling. The 4-core case that
read 77–83% of cap during development was an artefact of a probe backlog running
out inside its own measurement window — on a backlog with headroom the same build
read 99–100%.

## What I did not explain

- **Why 1 → 2 is superlinear (2.13×, both pairs above 2.0).** The one-core case is
  the weak one — serial collector, 8.8% GC by the harness's measure against 2.9%
  at two cores, 7.7% spread against 0.6% — so the ratio is flattered by its
  denominator. That is a description of the one-core case, not a measured cause of
  the ratio. I did not run the controlled experiment that would settle it.
- **Why the descending pass read 7.8% higher than the ascending pass at four
  cores** (786,237 vs 729,171) while one core read 7.5% *lower* descending. The
  sentinel says the rig did not drift (−1.5% first to last), so this is inside the
  suite. The harness's own record notes the same thing on other builds and says
  what moves it is not known. I have nothing to add.
- **Where the remaining 4.6% of 2→4 goes.** GC, checkpointing and the broker are
  all measured and all ruled out (2.2% of capacity, 90 ms average, 0.46 of 2.5
  cores). The host probe says memory-bound work on this machine scales 2→4 at
  76.1%, so there is a hardware bound well below linear and this pipeline is
  nearer to it than to the register-only bound — but that is a bound, not a
  mechanism, and I did not measure which part of the pipeline pays it.
- **Where it stops.** Not found. Four cores is not the ceiling — the worker is
  still the constraint there and the broker is at 18% of its cap. Locating the
  ceiling needs the `ceiling` run (starve the broker in steps at the largest
  case), and this session did not have room for it. The honest statement is that
  the ceiling is above four cores and unmeasured.

## Wall clock

| | |
|---|---:|
| **Accepted chain** (`prove.py all --quick`: up → preflight → completeness → tiny proof → fill → suite → report) | **52.1 min** |
| Chain attempts thrown away (completeness refusal 10.2 min, fill refusal 12.9 min) | 23.1 min |
| Build, smoke drain and rate probes before the first chain | ~76 min |
| Teardown, disk trim and this report | ~8 min |
| **Total** | **~2 h 39 m** |

Two thirds of the elapsed time is before the accepted chain, and almost all of
that is the four defects in `JOURNAL.md` — three of which a throughput number
would have reported as a perfectly good result.

## Reproducing

```
cd flink-skill-test-29
(cd job && JAVA_HOME=/opt/homebrew/opt/openjdk@17 mvn -q -B package)
python3 ~/.claude/skills/prove-it-scales/harness/prove.py all --quick   # drop --quick to publish
python3 ~/.claude/skills/prove-it-scales/harness/prove.py down
```

Raw results are under `results/`: `preflight.json`, `completeness.json`,
`tinyproof.json`, `selftest.json`, `suite.json` / `suite.md` / `suite.txt`,
`manifest*.json`, `phases.log`, `harness.log`, `all.log`, plus the two discarded
attempts as `all-attempt1-completeness-fail.log` and
`all-attempt2-fill-fail.log`. Held still across every case: 8 partitions, a 2.5-core
broker, a 0.5-core job manager, a 10 s checkpoint interval, a 300,040,000-record
backlog, and one build hash.
