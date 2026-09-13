# Block trades → allocations → running positions: a quick look at 1, 2 and 4 cores

> **QUICK LOOK — not a result.** The harness's own words, from `results/suite.md`:
> *"one pass per case. No spread, so no table: these numbers say the rig ran clean
> and roughly how fast, and nothing about how repeatable the ratio is. The record's
> own passes read 2.04-2.27x where a suite reported 2.15x. Do not publish or quote."*
> `suite.json` carries `"quickLook": true, "publishable": false`. The step ratios
> below are **not** a proven result and no claim is being made from them.

## Headline

| step | ratio | of linear |
|---|---:|---:|
| **2 → 4 cores** | **1.81×** | 90% |
| **1 → 2 cores** | **2.06×** | 103% |

One pass per case, so neither ratio has a measured spread. The 1-core case was
measured twice (the suite's sentinel), and those two passes alone move the
1→2 ratio between 2.018× and 2.103×.

## Header fields

| field | value |
|---|---|
| axis | one worker growing: one task manager container capped at N cores, parallelism N, N slots |
| API level | Flink DataStream API, hand-written operators (no SQL, no Table API) |
| guarantee | state: **exactly-once checkpointing**; sink: **at-least-once, idempotent** by emitting the absolute position per key |
| checkpoint interval | 10,000 ms |
| build hash | `65146bb06a53b232` (completeness passed for the same build) |
| passes per case | **1** (`--quick`; the configured number is 3) |
| rate source | committed broker offsets on `block-trades` |
| CPU source | cgroup `cpu.stat usage_usec` |
| backlog | 190,000,000 trades, 8 partitions, 5 outputs per input |
| rig | MacBook, 8 cores / 16 GB; Docker Desktop VM 8 vCPU / 7.65 GiB; Flink 1.20.1, Kafka 3.9.0, both arm64-native |

**Scope:** this is one pipeline on one laptop. It supports "this pipeline
scaled this way on this rig", not "Flink scales".

## The pipeline

A block trade arrives on Kafka (`{tradeId, ts, symbol, side, qty, priceE4,
strategyId}`, 128 B on disk). The job:

```
Source: kafka-source -> parse-trade -> allocate      (one chained vertex)
        |                                  |
   keyBy(symbol)  [HASH]           keyBy(account)  [HASH]
        v                                  v
 symbol-position -> symbol-sink    account-position -> account-sink
```

* **allocate** splits each block across the 4 accounts of its strategy's basket.
  Quantities allocate by largest remainder and notional half-even to 4 dp with
  the last leg absorbing the rounding, so **the legs sum to the block exactly**.
* **symbol-position / account-position** keep the running net quantity, net
  notional and update count in keyed `ValueState`, and emit the **absolute**
  position for the key on every update, keyed by the position key so all of a
  key's updates land in one Kafka partition and the last one is its final state.
* Fan-out **5**: 1 symbol update + 4 account updates per input trade.
* Cardinality **4,608 keys**: 512 symbols, 4,096 accounts.

Three vertices and two HASH edges at every parallelism — including 1, so the
baseline is not a chained single-vertex job flattering itself. The harness read
the shape off the running plan and compared it across every case.

## The measurements

Pasted from `results/suite.md`; no number retyped.

| cores | pass | records/s | tm cores | % of cap | throttled | broker cores | src idle | src BP | headroom | vantage |
|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | p1-asc | 153,526 | 1.00 | 99.6% | 100% | 0.16 / 2.5 | 0.1% | 37.0% | 1094 s | 0.35% |
| 2 | p1-asc | 322,915 | 1.93 | 96.3% | 100% | 0.28 / 2.5 | 0.7% | 26.8% | 448 s | 0.30% |
| 4 | p1-asc | 584,031 | 3.92 | 97.9% | 95% | 0.57 / 2.5 | 2.8% | 19.7% | 165 s | 0.10% |
| 1 | sentinel | 159,993 | 1.00 | 100.2% | 100% | 0.13 / 2.5 | 0.0% | 33.2% | 1045 s | 0.30% |

| cores | passes | mean records/s | out records/s | spread | reportable |
|---:|---:|---:|---:|---:|---|
| 1 | 2 | 156,759 | 783,797 | 4.1% | yes (but one pass per case → unpublishable) |
| 2 | 1 | 322,915 | 1,614,574 | — | yes (but one pass per case → unpublishable) |
| 4 | 1 | 584,031 | 2,920,157 | — | yes (but one pass per case → unpublishable) |

Sentinel: the 1-core case first (153,526 rec/s) and last (159,993 rec/s),
**drift +4.1%** across the suite — the rig did not move under the suite.

**The worker was the thing constrained**, which is what makes the numbers about
the worker: 99.6%, 96.3%, 97.9% and 100.2% of cap, 95–100% of periods throttled,
the broker never above 0.57 of its 2.5-core cap, source idle 0.0–2.8% against a
20% ceiling, and the broker never hit its container memory limit inside a window.
The 20–37% `src BP` is *internal* back-pressure inside the capped worker — the
source waiting on the aggregation threads that share its cores — which the
harness reports in a column and gates on nothing.

Two vantage points (committed source offsets vs. sink growth ÷ 5) agreed to
within 0.35% everywhere; tolerance is 5%.

Window construction, per case: warm-up to a flat trend over 4 commit intervals
(90–105 s, scatter 2.1–11.1%), then a 60 s window opened on the tick the
committed offset advanced and closed after 6 further commit boundaries.
Backlog headroom at close was 165 s at the 4-core rate — the drain never ran out.

## Correctness, gated before any throughput number

`prove.py suite` refuses to start unless completeness and the tiny proof have
passed **for the same build hash**. Both did, for `65146bb06a53b232`:

* **Completeness**, no tolerances, on a 6,000,000-trade backlog drained to the
  last record — twice. Clean drain (70.8 s) and a drain with the **task manager
  killed at 40%** (136.9 s, killed at committed offset 3,562,565, job RUNNING
  again on a replacement worker). Both arms: 512 symbol keys and 4,096 account
  keys — exactly the cardinality the input predicts, none missing and none extra;
  every net quantity, net notional and update count exact against the manifest;
  symbol updates = 6,000,000 = trades in; account updates = 24,000,000 = trades ×
  4 legs; and the two independent paths agreed exactly on both totals
  (3,725,600 shares, 13,331,669,379,400 notional at 1e-4).
* **Tiny proof**: two cases on an 80M backlog, 1→4 = **3.438×**, inside the
  harness's 3.00–5.00 bound (superlinear is a defect report).
* **Guard self-test**: **34/34 guards fired as expected**, each broken on purpose
  — wrong cap, busy cluster, truncated backlog, dead sampler, no job running,
  bad window anchor, disagreeing vantage points, spread, single pass, sentinel
  drift, starved broker, host-side watcher, disk projection, chain stop.
* `prove.py replay` re-checked every threshold against the 14 recorded suites
  before each command: no recorded valid table would be refused, no recorded
  invalid one reported.

## What was refused, and why

**The suite refused nothing.** `results/suite.json` has `"refusals": []` and no
`stoppedEarly`; all four measurements returned OK. Three refusals are worth
recording anyway:

1. **The watcher reaper killed my own wait loops.** During the tiny proof's
   self-test, `reap_host_watchers` killed a `tail -f` on `results/phases.log` and
   two shells whose command line named the project directory (exit 144). That is
   the guard working exactly as documented — "a watcher you start on the host will
   be killed by the teardown it was waiting for". The chain was untouched; I
   re-waited with a form the reaper leaves alone.
2. **Every guard in the self-test refused on purpose** (34/34), including the two
   that must *not* fire — a valid case record and a broker that never hit its
   limit — so a passing case is a case the guards looked at and let through.
3. **Nothing was refused for the rig or for a case's data.** The one number that
   could have stopped the chain, the tiny proof's 1→4 ratio, landed at 3.438×
   inside its bound.

## What I did not explain

* **Why 1→2 reads 2.06× (above linear) and 2→4 reads 1.81×.** I have not measured
  a cause and I am not going to offer one. A mechanism needs one rig, one build,
  one variable changed and both arms measured; I ran none of that. Note also that
  with one pass per case the difference is inside the band a single pass can
  wander: the harness's own record has single passes reading 2.04–2.27× where the
  three-pass suite reported 2.15×. **I do not know yet** whether 1→2 above linear
  is real or is one pass of noise on the structurally weakest case.
* **Where it stops.** I did not run `prove.py ceiling`. What the table does say is
  that the broker was nowhere near being the constraint at 4 cores — 0.57 of a
  2.5-core cap, source idle 2.8% — so the ceiling on this rig is not the broker at
  this size. Where it actually is, I did not measure. The host has 8 cores and the
  4-core case already spends 4 + 0.57 + jobmanager of them, so the rig itself
  cannot carry the next step.
* **Why source back-pressure falls from 37% to 19.7% as cores are added.** It is
  internal to the worker and gated on nothing; I did not investigate the trend.

## The interview I could not hold

No human was available. The questions I would have asked, one at a time, and the
assumption taken in place of each answer:

| # | question | assumption taken |
|---|---|---|
| 1 | What is the input event and what comes out? | A block trade (symbol, side, quantity, price, allocation strategy) in; the running position for its symbol and for each account it is allocated to, out. |
| 2 | Does one input become several outputs? | Yes, **5**: one symbol-position update plus four account allocation legs. |
| 3 | What are the keys, and how many distinct ones? | **512 symbols, 4,096 accounts** — small and fixed, so the outputs are arithmetic and the verifier asserts exact numbers with no tolerances. |
| 4 | What has to be exactly right — and which two settings? | Positions are running sums, so a replayed record is a wrong number: **exactly-once checkpointing** for the state, **at-least-once sink** made idempotent by emitting the absolute position per key. Tested by killing a worker mid-drain. |
| 5 | Who watches, and what must they believe? | Engineers: correctness first (hence completeness as a separate gated run), capacity second. |
| 6 | Where does it run? | Given: this laptop, in Docker. |
| 7 | What claim do you want to make? | **None.** The request was explicitly the fast look, not the publishable table, so the deliverable is "the rig runs clean and roughly this fast". |
| 8 | Which axis? | **One worker growing** — one task manager container capped at N cores, parallelism N, N slots — not a second JVM. |
| 9 | Which API level? | Given: DataStream, hand-written operators, no SQL or Table API. |

Two further assumptions worth naming:

* **Cases 1, 2 and 4 with the baseline at 1.** §5 says that if the claim is a step
  from two units up, the one-unit case should not be run at all — it is the
  structurally weakest and noisiest case in every run on record. The task asked
  for both step ratios, so it was run; the graph shape was held identical at
  parallelism 1 so the baseline gets no chaining advantage, and the 1-core case is
  the one whose two passes already disagree by 4.1%.
* **Sizes were chosen from a sizing calibration, not from anyone else's numbers.**
  A throwaway drain measured 1 core at ~137k rec/s and 2 cores at ~301k rec/s on
  this rig, which set the backlogs (190M suite / 80M tiny / 6M completeness).
  No number from that calibration appears in the table.

## Wall clock

Harness-timed phases are exact (`results/phases.log`); the rest are to the minute.

| phase | span | duration |
|---|---|---:|
| read the skill + harness contract, environment checks | 18:27–18:31 | ~4 min |
| build the pipeline (pom, domain, generator, job, verifier, `mvn package`) | 18:31–18:35 | ~4 min |
| `up` + `preflight` by hand (13/13 PASS) | 18:35–18:38 | ~3 min |
| **sizing calibration** (incl. one 10-min tool timeout and a duplicated 70M fill — rework) | 18:38–19:12 | **34 min** |
| disk trim, resize the backlogs, completeness smoke test | 19:12–19:22 | 10 min |
| **`prove.py all --quick`** | 19:22:24–19:58:00 | **35.6 min** |
| — `up` | | 2 s |
| — `preflight` | | 68 s |
| — `completeness` (2 drains + 2 verifications) | | 400 s |
| — `tinyproof` (80M fill, 2 cases, 34-guard self-test) | | 507 s |
| — `fill` (190,000,000 trades at ~860k rec/s) | | 221 s |
| — `suite` (4 measurements) | | 939 s |
| — `report` | | 0 s |
| `down` + assert nothing survives + fstrim (43.8 GiB returned) | 19:58:00–19:58:37 | 37 s |
| write this report | 19:59–20:04 | ~5 min |
| **total** | **18:27–20:04** | **~1 h 37 m** |

The measurement windows are 4 minutes of that. The chain is 36. The single
largest item is the calibration, and most of that was my own rework: one
10-minute tool timeout killed a run mid-flight and the restart re-filled a 70M
topic it had been told to skip.

## Teardown

`prove.py down` asserted it, and I asserted it again afterwards: no container,
volume or network with the `bt17` prefix survives, no `prove.py` process is
running, and `fstrim` returned 43.8 GiB to the host (150.7 GB free).

Raw results are under `results/`: `suite.json` / `suite.txt` / `suite.md`,
`completeness.json`, `tinyproof.json`, `selftest.json`, `preflight.json`,
`manifest*.json`, `phases.log`, `all.json`, `all.log`, `harness.log`, `DONE`.
