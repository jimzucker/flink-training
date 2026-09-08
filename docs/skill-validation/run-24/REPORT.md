# Block-trade allocation and running positions — does it scale from 1 to 4 cores?

**Headline: 1→2 cores returns 2.04× (102% of linear) and meets the claim. 2→4 cores
returns 1.906× — 95.3% of linear as a point estimate, but the lower bound of its
interval is 91.1%, so it does not meet a 95% claim. The chain ends FAIL at `report`.**

The table itself is not in dispute: every case ran at 97–100% of its CPU cap with the
broker at 20% of its own, per-case spreads of 1.8–2.7%, two vantage points agreeing to
0.6%, and no case refusals. This is a result about the pipeline and the host, not a
broken measurement.

| field | value |
|---|---|
| axis | one worker growing: one task manager container capped at N cores, parallelism N, N slots |
| API level | Flink DataStream API, hand-written operators (no SQL, no Table API) |
| guarantee — state | exactly-once checkpointing |
| guarantee — sink | at-least-once, made idempotent by emitting the absolute position per key with a per-key sequence number |
| checkpoint interval | 10,000 ms |
| build hash | `ecd91ca64b6e7938` (completeness passed for the same hash) |
| passes per case | 2 (`prove.py all --quick`), plus the sentinel |
| rate source | committed broker offsets on `block-trades` |
| CPU source | cgroup `cpu.stat usage_usec` |
| harness | `prove-it-scales/harness`, used verbatim; nothing in it changed |

> **QUICK LOOK — not a publishable result.** `--quick` runs two passes per case instead
> of the configured three. Enough for a spread, not enough to publish; the harness stamps
> `publishable: false` on the table it wrote. Every per-case guard stayed live.

## The table

Scope: this is **one pipeline on one laptop**. It says what this block-trade job does on
a task manager capped at 1, 2 and 4 cores against a broker held still. It says nothing
about "Flink scales".

| cores | pass | records/s | tm cores | % of cap | throttled | broker cores | src idle | src BP | headroom | vantage |
|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | p1-asc | 129,336 | 1.00 | 100.3% | 100% | 0.11 / 2.5 | 0.0% | 49.2% | 2175 s | 0.17% |
| 2 | p1-asc | 263,214 | 1.99 | 99.2% | 100% | 0.23 / 2.5 | 0.8% | 36.0% | 999 s | 0.22% |
| 4 | p1-asc | 513,209 | 3.84 | 96.1% | 100% | 0.49 / 2.5 | 1.9% | 23.1% | 433 s | 0.56% |
| 4 | p2-desc | 499,363 | 4.01 | 100.4% | 100% | 0.49 / 2.5 | 1.9% | 25.0% | 449 s | 0.26% |
| 2 | p2-desc | 267,933 | 1.91 | 95.3% | 100% | 0.23 / 2.5 | 0.9% | 34.8% | 972 s | 0.57% |
| 1 | p2-desc | 129,132 | 1.00 | 99.7% | 100% | 0.11 / 2.5 | 0.0% | 41.0% | 2180 s | 0.28% |
| 1 | sentinel | 132,071 | 1.00 | 100.2% | 100% | 0.11 / 2.5 | 0.0% | 51.9% | 2127 s | 0.45% |

| cores | passes | mean records/s | per core | spread | **GC (% of capacity)** | reportable |
|---:|---:|---:|---:|---:|---:|---|
| 1 | 3 | 130,180 | 130,180 | 2.3% | **5.78%** | yes |
| 2 | 2 | 265,574 | 132,787 | 1.8% | **1.29%** | yes |
| 4 | 2 | 506,286 | 126,572 | 2.7% | **0.65%** | yes |

Sentinel: the 1-core case measured first (129,336) and last (132,071) in the suite, drift
+2.1%; it is counted in that case's spread, and the suite did not drift under it.

Output rate is 5× the input rate — 1 symbol position and 4 account positions per block
trade — so the 4-core case wrote **2.53 M records/s** to Kafka while consuming 506 k.

### Step ratios, and whether they met the claim

The claim: *each doubling of cores and parallelism returns at least 95% of double the
throughput*. The harness judges it on the **lower bound** of the ratio's interval, not on
the point estimate — the burden is on the claim, and a ratio that *might* be linear has
not been shown to be.

| step | adjacent pairs | ratio | interval (95%) | efficiency at the lower bound | met? |
|---|---|---:|---|---:|---|
| **1→2** | 2.035, 2.075 | **2.040×** | 2.016 – 2.094 | **100.8%** | **YES** |
| **2→4** | 1.864, 1.950 | **1.906×** | 1.822 – 1.991 | **91.1%** | **no** (shortfall 8.9 pts) |

Point efficiency is 102.0% for 1→2 and **95.3%** for 2→4 — the 2→4 *point* clears the
floor; its interval does not. The half-width comes from the two adjacent pairs' own
disagreement: 1.9% for 1→2 and **4.5%** for 2→4. With two passes, a 4.5% disagreement is
worth ±4.4% on the ratio, and that is what fails the test.

Read plainly: **1→2 is linear here. 2→4 measured 95.3% of linear and I cannot show, with
two passes on this rig, that it is above 95%.**

## What was refused, and why

| what | when | verdict |
|---|---|---|
| **the claim, at 2→4** | `report`, chain 2 | `meetsClaim: false` — 1.822 lower bound against a 1.90 floor. The chain exits FAIL. This is the whole story. |
| **the claim, at both steps** | `report`, chain 1 (superseded build) | 1→2 lower bound 1.785 (89.3%), 2→4 lower bound 1.702 (85.1%). |
| **our key-group arithmetic** | first job submit, before any measurement | The job asserts its copy of Flink's `MathUtils.murmurHash` against `KeyGroupRangeAssignment` for all 12,288 keys. It disagreed (`SYM000000`: 31 vs 87) — the copy was missing `code ^= 4` and had the wrong negative branch. Fixed. Without that read-back the "balanced key groups" would have been balanced across the wrong buckets and the study would have measured data skew. |
| nothing else | — | `preflight` 17/17 PASS. `completeness` PASS on both arms. `tinyproof` PASS (1→4 = 3.675×, inside the 3.0–5.0 bound) with its self-test firing **36 of 36 guards on purpose**. The suite recorded **zero** case refusals across both chains. |

Two scouting runs outside the harness were refused for `only 2 commit boundaries inside
the window` — my scouting script asked for a 2-boundary window and `check_case` requires
3. That is my script being wrong, not the harness; no scouting number is quoted as a
result.

## Correctness, gating the throughput

No table is published for a build that has not passed completeness, and this one did, for
`ecd91ca64b6e7938`:

- 10 M block trades drained to the last record, twice: once clean (114 s), once with the
  **task manager killed at 35% of the drain** (170 s including recovery).
- Both times: 4,096 symbol positions and 8,192 account positions — the cardinality
  predicted in the interview — matching the generator's manifest **exactly**, field by
  field, with no tolerance.
- Both times the **two independent paths agree to the share**: `net = 87,634,900`,
  `gross = 500,551,540,300`, `notional = 28,059,245,816,700` computed by symbol and by
  account. That is only possible because the allocation is integer largest-remainder and
  money is integer cents.
- The generator is deterministic: two manifests from one seed are byte-identical.

## Every change made to chase the claim, and what it did to the numbers

### 1. `caps.tmMemoryBase` 768m → 2048m (before chain 1)

One variable, all three cases measured on a 60 M-record scouting topic.

| base | 1 core | 2 cores | 4 cores | GC 1c / 2c / 4c |
|---|---:|---:|---:|---|
| 768m | 120,016 | 246,752 | 509,496 | 10.6% / 3.3% / 1.1% |
| 2048m | 122,220 | 262,465 | 503,558 | 5.0% / 1.5% / 0.8% |

It halved the baseline's GC and bought the baseline **+1.8%** — so the 1-core case was
*not* materially heap-starved, which is worth knowing because a 10% GC figure invites
that assumption. Kept anyway: it balances GC across the cases and it makes the baseline
faster, which is the direction that makes the claim *harder*.

### 2. Cut ~2.4 KB of allocation per input record (between chain 1 and chain 2)

Chain 1 came in at 2→4 = 1.861 with the worker at 99.9% of cap and the broker at 20% of
its own — so the worker *was* the constraint and was simply doing less useful work per
core at 4 cores (118,347 vs 127,190 per core, −7.0%).

Before offering a mechanism I measured what this host's cores do at all
(`results/host-cpu-probe.txt`, two arms, repeated): a register-only loop doubles at
**98%** of linear on both steps; a random read-modify-write over a 4 MB array doubles at
~99% from 1→2 but only **69%** from 2→4. Memory traffic is where this host stops scaling.

So I counted the pipeline's allocation: about **3.5 KB per input record**, of which ~2.5 KB
was the output encoder — `StringBuilder` → `char[]` → `String` → `byte[]` for a 92-byte
JSON record, five times per input. Replaced with a direct write into a per-subtask scratch
`byte[]`, copied out once at its exact length; plus two smaller cuts (do not materialise
the unused `blockId` String; reuse the account/weight arrays instead of allocating a pair
per record). Output bytes are **byte-for-byte identical** to the old encoder — checked
directly, and again by completeness.

| | chain 1 `74d31cf7…` | chain 2 `ecd91ca6…` | change |
|---|---:|---:|---|
| 1 core, mean | 129,230 | 130,180 | +0.7% |
| 2 cores, mean | 254,379 | 265,574 | +4.4% |
| 4 cores, mean | 473,390 | 506,286 | **+6.9%** |
| 4 cores, per core | 118,347 | 126,572 | +7.0% |
| **1→2 ratio (lower bound)** | 1.968 (1.785) | **2.040 (2.016)** | miss → **meets** |
| **2→4 ratio (lower bound)** | 1.861 (1.702) | **1.906 (1.822)** | miss → miss |
| per-case spread | 4.7 / 4.5 / 4.3% | 2.3 / 1.8 / 2.7% | roughly halved |
| adjacent-pair spread, 1→2 / 2→4 | 9.2% / 8.8% | 1.9% / 4.5% | much tighter |
| GC, 1c / 2c / 4c | 7.3 / 1.4 / 0.66% | 5.8 / 1.3 / 0.65% | baseline down |

**Kept.** It moved the big case most — where the host's memory result says the headroom
is — and halved the run-to-run noise as a side effect.

### 3. Things I chose *not* to change

- **The harness, and its thresholds.** Untouched. `prove.py replay` passed before every
  command: 14 recorded suites, 22 step verdicts, 6 recorded configurations.
- **The rig, to flatter the answer.** The obvious next lever was worker memory: at 4 cores
  the broker's page cache is squeezed to 0.5–1.5 GB where the 1- and 2-core cases leave it
  1.9–2.3 GB, because the 4-core worker asks the 7.8 GB VM for the most. Shifting worker
  memory from per-core to base would hand ~1.5 GB back to the broker in exactly the case
  under test. I did not do it: it changes the measurement conditions in favour of the big
  case, and with the point estimate already at 95.3% that is shopping for a configuration
  that passes rather than building a pipeline that scales.
- **Re-running the same build hoping for a friendlier pair of passes.** The 2→4 verdict
  turns on a 4.5% disagreement between two passes. Rolling again until they agree is
  p-hacking. One build, one chain, reported.

## What I did not explain

- **The residual 2→4 shortfall.** The 4-core case does 126,572 records per core against
  the 2-core case's 132,787 — 4.7% less — while sitting at 98% of its cap with the broker
  at 20% of its own and the source idle 1.9% of the window. I do not know what that 4.7%
  is. The host probe *bounds* it (a memory-bound workload loses 31% over the same step
  here, an ALU-bound one loses 2%, and this pipeline is somewhere between), but that is a
  proxy workload, not this pipeline, and no percentage of the shortfall is attributed to
  it.
- **Why the two 2→4 passes disagree by 4.5%** on one unchanged build inside one suite,
  when each case's own spread is under 3%. In chain 1 the disagreement was 8.8% and the
  per-pass rates alternated low-high-low-high-low-high by position in the suite, which
  looks systematic and which I could not account for.
- **Why the worker's cap usage wanders 95.3–100.4%** between passes of the same case, and
  whether the broker's page cache (0.49 GB in one 4-core pass, 1.51 GB in the other, the
  faster pass being the one with more cache) causes it. Two points is a coincidence, not
  evidence.
- **Where it stops.** Not located. The broker is nowhere near its CPU cap (0.49 of 2.5 at
  the largest case) and never hit its memory limit inside a window, so it is not the CPU
  ceiling; the disk-and-page-cache path is the suspect, and `prove.py ceiling` — which
  starves the broker's *CPU* — would not have found it. Naming a ceiling I did not measure
  would be worse than saying this.

## Wall clock

Total, from the first line of code to `down` asserting the stack gone: **2 h 41 min**
(14:44 → 17:25, 2026-09-08).

| segment | wall clock | kept? |
|---|---:|---|
| Interview notes, design, build of job + generator + verifier | 8 min | kept |
| Scouting: rate measurement, the memory-base comparison, the murmurHash fix | 27 min | kept — it sized the backlogs and set the memory |
| Completeness run as an early shakeout | 9 min | superseded by the chain's own |
| **Chain 1, `prove.py all --quick`** | **50.5 min** | **redone** — the build it measured was replaced |
| Diagnosis (host CPU probe), the allocation change, a scouting check | 14 min | kept |
| **Chain 2, `prove.py all --quick` — the accepted chain** | **49.5 min** | **accepted** |
| Teardown and assertion that nothing survives | 2 min | — |

**Accepted chain: 49 min 31 s. Redone or superseded: 59.5 min** (chain 1 plus the early
completeness shakeout). The remaining ~49 min is the build and the scouting that sized the
run, neither of which was repeated.

Chain 2, phase by phase: `up` 18 s · `preflight` 69 s · `completeness` 488 s ·
`tinyproof` 493 s · `fill` 231 s (300 M records) · `suite` 1,673 s · `report` 0 s.

## The pipeline, in one paragraph

`block-trades` → parse the JSON block trade → allocate it across the 4 accounts it names,
pro-rata with largest-remainder rounding so the integer shares sum to the block quantity
exactly → `keyBy(symbol)` into a running position per symbol → `positions-by-symbol`; and
`keyBy(account)` into a running position per account → `positions-by-account`. Three
vertices, two HASH edges, the same graph shape at every parallelism (read back off the
running plan and compared across cases by the harness). 4,096 symbols and 8,192 accounts,
chosen so that every one of Flink's 128 key groups holds exactly 32 symbols and 64
accounts — 128 divides by 1, 2 and 4, so every subtask of every case carries exactly the
same share of the traffic, and the table measures cores rather than skew.

Source: `pipeline.json`, `job/src/main/java/blocktrades/`, journal in `JOURNAL.md`, raw
results under `results/` (chain 1 preserved under `results/superseded-chain1/`).
