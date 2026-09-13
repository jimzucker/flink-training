# Block-trade allocation and running positions — a quick look at scaling

**This is a `--quick` smoke table. It is not a result, and the two step ratios below
are not proven.** The harness stamps it `quickLook: true`, `publishable: false`, and
says so itself:

> **QUICK LOOK — not a result.** two passes per case, not the configured number:
> enough for a spread, not enough to publish. A one-pass version of this table read
> 2->4 = 1.645 where the same build measured 1.910 with the memory it needed, and
> 1.539 where three passes read 1.678. Quote the spread with the ratio, or do not
> quote it.

The harness's README adds what a quick table does and does not answer: *"Every
per-case guard stays live, so it answers 'does this rig run clean, and roughly how
fast'. It answers nothing about the ratio: replayed against the record, single passes
of the recorded suites read 2.039-2.273x where the suite reported 2.154x, and
1.837-1.859x where it reported 1.850x — a band wider than the accept line. Never quote
a quick table, and never put one in `record/`."*

So: the rig ran clean, every case was measured with every guard live, and the numbers
below are what it read. A publishable table needs the configured three passes per case
(`prove.py all`, no flag).

---

## What it read

**1→2 cores: 2.089× (104.5% of linear), range 2.025–2.132× across passes.**
**2→4 cores: 1.793× (89.7% of linear), range 1.782–1.804× across passes.**

Both ratios are provisional for the reason above.

| field | value |
|---|---|
| axis | one worker growing: one task manager container capped at N cores, parallelism N, N slots |
| API level | Flink DataStream API, hand-written operators (no SQL, no Table API) |
| guarantee — state | exactly-once checkpointing (offsets committed only on a completed checkpoint) |
| guarantee — sink | at-least-once, idempotent by emitting the absolute position per key |
| checkpoint interval | 10,000 ms |
| build hash | `1b997db5c993ca54` (completeness passed for the same hash) |
| passes per case | 2 (`--quick`; the baseline is measured a third time as the sentinel) |
| rate source | committed broker offsets on `block-trades` — never the engine's meter |
| CPU source | cgroup `cpu.stat usage_usec` at window open and close |
| backlog | 240,000,000 records, 8 partitions, 5 outputs per input |
| held still across cases | broker 2.5 cores, job manager 0.5 cores, 8 partitions, 10 s checkpoints, 2 GiB sink retention per partition, worker memory 1024m base + 768m per subtask |

### Per pass

| cores | pass | records/s | out rec/s | tm cores | % of cap | throttled | broker cores | src idle | src BP | headroom | vantage | GC |
|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | p1-asc | 176,355 | 881,775 | 1.00 | 99.6% | 100% | 0.10 / 2.5 | 0.1% | 40.5% | 1215 s | 0.23% | 8.98% |
| 2 | p1-asc | 373,500 | 1,867,502 | 2.00 | 99.9% | 100% | 0.24 / 2.5 | 2.4% | 30.8% | 494 s | 0.15% | 3.84% |
| 4 | p1-asc | 665,759 | 3,328,794 | 4.01 | 100.3% | 100% | 0.50 / 2.5 | 3.2% | 23.7% | 208 s | 0.25% | 1.67% |
| 4 | p2-desc | 670,749 | 3,353,745 | 4.01 | 100.4% | 100% | 0.48 / 2.5 | 3.1% | 20.5% | 208 s | 0.20% | 1.53% |
| 2 | p2-desc | 371,797 | 1,858,986 | 2.01 | 100.3% | 100% | 0.24 / 2.5 | 2.2% | 29.8% | 485 s | 0.45% | 4.27% |
| 1 | p2-desc | 175,176 | 875,882 | 1.00 | 99.6% | 100% | 0.12 / 2.5 | 0.0% | 41.1% | 1226 s | 0.32% | 13.20% |
| 1 | sentinel | 183,630 | 918,149 | 0.99 | 99.5% | 100% | 0.11 / 2.5 | 0.2% | 43.4% | 1167 s | 0.24% | 11.32% |

### Per case — spread and garbage collection

| cores | passes | mean records/s | **spread** | **GC, fraction of the case's CPU capacity** | worker % of cap | broker cores | src idle | src back-pressure | max vantage disagreement | reportable |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 1 | 3 | 178,387 | **4.7%** | **11.17%** | 99.6% | 0.11 / 2.5 | 0.1% | 41.7% | 0.32% | yes |
| 2 | 2 | 372,649 | **0.5%** | **4.06%** | 100.1% | 0.24 / 2.5 | 2.3% | 30.3% | 0.45% | yes |
| 4 | 2 | 668,254 | **0.8%** | **1.60%** | 100.3% | 0.49 / 2.5 | 3.2% | 22.1% | 0.25% | yes |

Every case is under the harness's 20% spread ceiling, so no case and no ratio was
voided. Order effect (descending mean / ascending mean): 1c 0.993, 2c 0.995, 4c 1.008.
Sentinel: the 1-core case read 176,355 rec/s first and 183,630 rec/s last, drift
**+4.0%** across the suite — counted inside that case's 4.7% spread.

`results/suite.md` is the harness's own copy of this table. One line in it is stale:
its ratio lines say *"one pass per case, no spread measured"*, which was true of
`--quick` before 2026-09-06; this run measured two passes and the ranges above come
from `results/suite.txt`, which prints them correctly. The numbers are the same.

---

## What is behind the numbers

**Scope.** One pipeline, one build, one laptop. This says *this block-trade pipeline
behaved this way on this rig*; it says nothing about "Flink scales".

**The pipeline.** A block trade arrives on `block-trades` as a FIX-style tag=value
record. The job parses it, allocates it pro-rata across the four accounts of its
allocation scheme (slot 0 absorbs the remainder, so quantity is conserved exactly),
and maintains two running positions — net position per **symbol** (4,096 keys) and net
position per **account** (8,192 keys) — writing the absolute current position for each
key to `positions-by-symbol` and `positions-by-account`. Fan-out is exactly 5 output
records per input.

**The baseline is the same graph as every other case.** Three vertices —
`Source: kafka-source -> allocate`, `symbol-position -> sink-symbol`,
`account-position -> sink-account` — with **HASH** edges into both keyed operators at
parallelism 1 as well as at 2 and 4. The harness read the shape off the running plan
and compared it across cases. Nothing chains into a single vertex at the baseline,
which is the difference the skill measured as 2.16× becoming 3.26×.

**Completeness passed for this build, with no tolerances**, before any throughput was
reported: a 20,000,000-record backlog drained twice — once clean, once with the task
manager `docker kill`ed at 35% of the drain and the job restarted on a replacement
worker. Both arms:

- 4,096 distinct symbol keys and 8,192 distinct account keys — exactly the cardinality
  predicted in the interview;
- every symbol's and every account's (net quantity, net notional, update count) equal
  to the generator's manifest **exactly**;
- the two paths agree exactly: net quantity 51,030,517,282 and net notional
  1,326,897,507,205,252 by symbol and by account and in the manifest;
- 20,000,000 symbol updates and 80,000,000 account updates — the fan-out, asserted.

---

## What was refused, and why

**In the suite: nothing.** `results/suite.json` has `"refusals": []`. All seven case
measurements returned OK; no case was voided and no ratio was withheld.

**In the tiny proof's self-test: 35 guards, all fired as expected.** These are refusals
on purpose — the harness breaks each guard to prove it is not a guess. Among them: a
cap that did not apply, a cluster still busy from the last case, a backlog one record
off its manifest, a monitor that died at startup, a job that never reached RUNNING, a
host-side watcher outliving the run, a spread past the ceiling, a case measured only
once, a starved broker, and a disk budget that does not fit.

**Before the chain, three things were refused or broke, and each changed the
configuration.** All were found by measuring, not by reasoning:

| what | evidence | what changed |
|---|---|---|
| The 4-core task manager was killed by the VM mid-run | job restart-looped; `No route to host` to the worker; an *idle* 4-core worker plus the broker already left `MemAvailable` at 2.9 GB, and the worker's heap grows into that under load | worker memory 768m base + 1280m/core → **1024m base + 768m/core**; broker heap 3 G → **1 G** |
| The harness's broker-page-cache guard would have refused the baseline | at `kafkaMemory: 3g` the **1-core** case hit the broker's cgroup limit **9,437 times** in a 60 s window while the **4-core** case hit it **zero** times — at 1 core the worker is small, the VM has 1.65 GB free, the cache grows to 2.0 GB and reaches the limit; at 4 cores the VM is already full and global reclaim (which the guard does not count) trims the cache first | `kafkaMemory` → **6g**, deliberately above anything this VM can hand the broker. Re-measured at 1 core: 0 hits, 0 refaults, rate unchanged (175,211 → 175,544 rec/s, 0.2%) |
| A probe case was refused: *"backlog drained before the window closed"* | my probe topic held 40M records; the 4-core case eats them in under 100 s | sized every backlog from the measured 4-core rate instead (below) |

**Backlogs sized from a measurement, not a guess.** Pre-chain probes read 667,732
rec/s at four cores and 77.5 B/record on the broker's disk, so `backlog.count` was set
to 240,000,000 (the harness's tiny proof later computed 84,160,956 as the minimum),
`tinyCount` to 120,000,000 and `smallCount` to 20,000,000 — the last chosen so it
spans eleven checkpoint intervals at the 1-core baseline rate and the kill at 35% lands
40 s in with several completed checkpoints behind it.

---

## What I did not explain

Four things. Each has a plausible story and none has the controlled experiment that
would make it a cause, so none is offered as one.

1. **1→2 is superlinear (2.089×, 104.5% of linear).** The obvious candidate is that
   the baseline is partly measuring garbage collection: the 1-core case spends
   **11.17%** of its capacity in GC against 4.06% at two cores and 1.60% at four. But
   the one piece of evidence I have points the other way — raising the worker's fixed
   memory base from 768m to 1024m (which gives the 1-core case 17% more process
   memory) moved its GC fraction from 8.8% to 8.2% and its rate not at all
   (175,211 → 175,544 rec/s). So heap size alone does not explain the GC, and I have
   not run the experiment — one rig, one build, worker memory the only variable at one
   core — that would settle it. **I do not know why 1→2 is above linear.**

2. **Why 2→4 is 1.793× and not ~2×.** The worker is at 100.3% of its cap at four
   cores, so it is the constrained component; the broker used 0.49 of its 2.5-core cap,
   so the broker is not near its own CPU limit; source idle is 3.2% and the broker hit
   its memory limit zero times, so the input side is not visibly starved. Internal
   back-pressure falls monotonically (41.7% → 30.3% → 22.1%) and source idle rises
   (0.1% → 2.3% → 3.2%), which is where I would look next — but I did not run
   `prove.py ceiling`, and **I do not know where the 10% goes.**

3. **The sentinel drifted +4.0%.** The 1-core case read 176,355 rec/s at the start of
   the suite and 183,630 rec/s at the end, 28 minutes later. That is inside its own
   4.7% spread and well under the ceiling, so nothing was voided, but something on the
   rig warmed or settled across the suite and I did not find out what.

4. **Broker file-page refaults swing 89,157–571,531 between passes** with no visible
   effect on the rate, and the 4-core pass with the most refaults was the *faster* of
   the two. Not explained.

**Where it stops** is also not measured: I did not run the ceiling command, so I cannot
name the point at which adding cores to this worker stops paying. Naming it would make
the rest more credible, and it is the obvious next thing to run.

---

## Wall clock

Total from an empty directory to a torn-down stack: **1 h 47 m** (20:09 → 21:56).

### The accepted chain — `prove.py all --quick`, 49.0 min

| phase | start | seconds | minutes |
|---|---|---:|---:|
| up | 21:05:53 | 18 | 0.3 |
| preflight | 21:06:11 | 64 | 1.1 |
| completeness (fill 20M, drain clean, drain with the worker killed, verify twice) | 21:07:14 | 577 | 9.6 |
| tinyproof (fill 120M, 1-core and 4-core cases, disk projection, backlog sizing, 35-guard self-test) | 21:16:51 | 429 | 7.2 |
| fill (240,000,000 records) | 21:24:00 | 156 | 2.6 |
| suite (7 measured cases: 3 cases × 2 passes + sentinel) | 21:26:36 | 1698 | 28.3 |
| report | 21:54:55 | 0 | 0.0 |
| **total** | | **2942** | **49.0** |

Teardown afterwards (`prove.py down`, assertions, fstrim): **~1 min**. Nothing with the
`bt24` prefix survived — 0 containers, 0 volumes, 0 networks — and fstrim returned
19.6 GiB to the host.

### Everything before the chain — 56.9 min (20:09 → 21:05:53)

| phase | minutes | first-time work | redone / lost |
|---|---:|---:|---:|
| Read the skill and the harness contract; write the interview and the design | 10 | 10 | — |
| Write the job, generator and verifier; build; determinism check | 6 | 6 | — |
| Smoke test: stack up, 2M drain at 2 cores, graph shape, reporter metrics, verifier | 3 | 3 | — |
| **4-core rate probe that died** — task manager killed by the VM | 9 | — | **9** |
| Diagnose it: VM disk throughput, VM memory with a 4-core worker up, broker cgroup counters | 4 | 4 | — |
| Generator producer-buffer fix and its verification fill | 2 | 2 | — |
| **Probe refused: backlog drained inside the window** | 3 | — | **3** |
| Fill a 160M probe backlog; 4-core probe at the new memory | 6 | 6 | — |
| Second config change (broker limit 3g → 6g, worker base 768m → 1024m) and writing it up | 5 | 5 | — |
| **Repeat both probes on the changed configuration** (1 core, then 4 cores) | 7 | — | **7** |
| Teardown before the clean chain | 1 | 1 | — |
| **total** | **56** | **37** | **19** |

**So: 49 minutes on the accepted chain, ~37 minutes of first-time build and
measurement, and ~19 minutes redone** — nine of them lost to a worker the VM killed,
three to a backlog I had sized by guess, and seven to repeating two probes after the
configuration changed. All nineteen were spent *before* the chain, which is where they
are cheapest; none of the accepted chain was re-run.

---

## Files

| path | what |
|---|---|
| `INTERVIEW.md` | the questions I would have asked and the assumptions taken instead |
| `JOURNAL.md` | what drove each step, what was decided, how it was verified |
| `pipeline.json` | the pipeline the harness was given |
| `job/` | the Flink DataStream job, the deterministic generator and the completeness verifier |
| `results/suite.txt`, `results/suite.md`, `results/suite.json` | the table as the harness wrote it |
| `results/completeness.json`, `results/tinyproof.json`, `results/selftest.json`, `results/preflight.json` | the gates |
| `results/phases.log`, `results/all.json`, `results/all.log`, `results/harness.log`, `results/DONE` | the chain's own timing and log |
| `results/prechain/` | the pre-chain probe manifests and log |
