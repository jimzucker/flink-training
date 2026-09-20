# Clean-room validation, run 30 — the skill after it moved out

The first run of `scalable-flink-skill` since it was renamed, split into
[its own repository](https://github.com/jimzucker/scalable-flink-skill) and
installed globally. Fresh agent, empty directory, barred from reading this
repository, the skill's repository, and every earlier run's directory. One
prompt, no human input.

**Model: Claude Opus 5.** Working directory `~/code/GitHub/flink-skill-test-31`.

The question this run existed to answer was whether the skill still carries the
method from its new home. It does: the agent built a working pipeline, proved it
correct, measured it, and was refused three times when it deserved to be.

## What it produced

**2 → 4 cores: 1.662×, 83.1% of linear. It does not clear the claim.** The claim
is judged on the interval's lower bound, which is 78.9%. The worker held
98.9–99.6% of its cap in every pass, so the worker was the constraint and the
measurement is sound — **the claim failed, not the instrument.**

| cores | pass | records/s | % of cap | src idle | src BP |
|---:|---|---:|---:|---:|---:|
| 2 | p1-asc | 499,448 | 99.5% | 4.7% | 34.8% |
| 4 | p1-asc | 735,034 | 98.9% | 3.3% | 35.5% |
| 4 | p2-desc | 853,358 | 99.3% | 5.4% | 22.3% |
| 2 | p2-desc | 486,394 | 99.6% | 4.7% | 34.2% |
| 2 | p3-asc | 451,343 | 99.5% | 7.4% | 29.4% |
| 4 | p3-asc | 776,463 | 99.4% | 5.2% | 23.4% |
| 2 | sentinel | 460,044 | 99.2% | 5.3% | 35.2% |

| cores | passes | mean records/s | spread | reportable |
|---:|---:|---:|---:|---|
| 2 | 4 | 474,307 | 10.1% | yes |
| 4 | 3 | 788,285 | 15.0% | yes |

Interval [1.579, 1.829], adjacent pairs 1.472 / 1.688 / 1.720 / 1.754, sentinel
drift −8.2%. Build `781e8f53635502ed`, 350,000,000 trades, 8 partitions, fan-out
4, 32 symbol keys and 4,096 account keys, 10 s checkpoints, three passes plus a
sentinel.

Cases are **2 and 4, with no 1-core case.** The agent's reasoning: at
parallelism 1 the same job chains into a single vertex, so the baseline would
not share the other cases' graph. That is the rule from §5, applied without
being told.

## What the harness refused

| when | what | why |
|---|---|---|
| attempt 1, tiny proof | case refusal, chain stopped | backlog headroom at close was 0 records — the 60M tiny backlog drained inside the window |
| attempt 2, suite | two `CEILING` cases | the broker hit its memory limit inside the window |
| attempt 2, report | claim gate | a valid, complete table whose step ratio misses the 95% floor |
| final suite | nothing | 7 of 7 accepted |

Everything else passed: preflight 18/18 twice, `replay` before every command,
the guard self-test **38/38 fired as expected**, completeness twice.

**Correctness held with no tolerances.** A clean drain of 12,000,000 trades, then
a second drain with the task manager killed mid-drain. The verifier asserts
against a manifest computed from the input alone: 32 symbol keys and 4,096
account keys exactly as the interview predicted, every position exact, and update
counts of exactly 12,000,000 and 36,000,000 — which catches a record folded in
twice even when the sums still agree.

## The one-variable comparison the agent ran unprompted

The harness's ceiling refusals named the broker's page cache, so the agent tested
that claim instead of accepting it. Same jar, same backlog, same caps, same
machine; **only `caps.kafkaMemory` changed**, 4096m → 6144m, read back from the
cgroup.

| | broker 4 GiB | broker 6 GiB |
|---|---:|---:|
| broker limit hits per case | 19,854 – 45,975 | 0 |
| cases refused as ceilings | 2 of 7 | 0 of 7 |
| 2-core mean (spread) | 491,382 (5.4%) | 474,307 (10.1%) |
| 4-core mean (spread) | 821,556 (4.5%) | 788,285 (15.0%) |
| **2 → 4** | **1.672×** | **1.662×** |

Removing the page-cache pressure removed the refusals and moved the ratio by
0.6%, inside either arm's spread. **A starved broker page cache is not what held
this pipeline to 83%.** It also widened the spread, which is unexplained.

**Why the pipeline returns 83% rather than 95% is not known.** Ruled out by
measurement: the worker was the constraint, the broker was far from its CPU cap
(0.33–0.62 of 2.5), the source was not starved, GC was 2.1–3.8% against a 5.5%
ceiling, the job graph was identical across cases, the two vantage points agreed
within 0.44%, and the page cache by the comparison above. The agent stated it as
unknown rather than explaining it, which is the right answer.

## The defect this run paid for

The broker-memory guard fires on `hits > 0 AND tmCapFrac < 0.99`. On this
workload the broker hit its 4 GiB limit in **every** window, so the verdict turns
entirely on which side of 0.99 the cap fraction lands:

| case | hits | cap | records/s | verdict |
|---|---:|---:|---:|---|
| 4c p3-asc | **45,975** | 1.001 | 802,962 | OK |
| 4c p2-desc | 36,152 | 0.956 | **843,481** | CEILING |
| 2c p3-asc | 19,854 | 0.995 | 502,606 | OK |
| 2c p1-asc | 19,926 | 0.988 | **496,886** | CEILING |

The case with **more** limit hits was accepted; both discarded measurements were
**faster** than accepted passes of the same case. A 0.7-point difference in cap
fraction should not decide whether a measurement exists.

**Unfixed, and a threshold will not fix it.** Tried on 2026-09-20: a refault
floor was added, and the harness's own replay refused it — `cases.json` holds
the starvation this guard exists for, and the new rule made it invisible.
Laid side by side, the signals overlap in every direction:

| case | hits | cap | refaults | required |
|---|---:|---:|---:|---|
| 2 GiB starved, the true positive | 30,927 | 0.964 | **520,575** | fire |
| run 23, accepted | 9,437 | 0.996 | **572,000** | do not fire |
| run 30 4c p2-desc, disproved by the one-variable run | 36,152 | **0.956** | 158,499 | fires today |

The true positive does *less* disk reading than an accepted case and runs at a
*higher* cap than a proven false positive. No line separates them, so the next
attempt should be a design change — have the guard prescribe the one-variable
re-run that actually settled it — or nothing. It fires about twice in 103 cases,
discards one measurement rather than the run, and errs toward caution: it has
never let a broker-starved number into a table.

## Two bugs, found and fixed

Both confirmed in the code before being believed, and fixed in
[`scalable-flink-skill@7d37dc9`](https://github.com/jimzucker/scalable-flink-skill/commit/7d37dc9).

**Backlog sizing asked for about half what it should.** `cmd_tinyproof` read
`warmup["seconds"]`, but `warmup_verdict` returns `warmupS`. The value was always
`None`, so `size_backlog` fell back to `T["warmupMinS"]` — which at that call site
is the tiny proof's own 20 s override, not 90. Replayed against all 23 recorded
suites, the corrected guard asks **1.9–2.4× more** than the bug did. That guard is
the harness README's headline defence: *"every clean-room run from 15 to 20 lost
an attempt to a backlog sized by guess."*

Sizing on the suite's own 60 s window looked obviously right and was **rejected on
the evidence**: replayed, it refuses run-11, run-17 and phase3, all of which
produced tables we accepted. The ×1.5 margin absorbs the difference.

**A `CEILING` case rendered as `REFUSED (None): None`** with every column an
em-dash. Both renderers took the refusal branch, which reads `refusalScope` and
`refusal` — keys a ceiling never sets. The record keeps its rate, so the rate
existed and the table dropped it, against the README's *"measured, kept, reported
with its rate … never deleted."*

A third change makes the first unrepeatable: `replay_sizing` over
`record/sizing.json` holds the rate, warm-up, checkpoint interval and backlog of
all 23 recorded suites, and refuses any sizing rule that would have blocked a
suite whose table we accepted. Sizing decides whether a suite may run at all, and
nothing replayed it — which is why a key-name slip could stand.

## What the agent reported, and what came of it

Seven more findings, all acted on. Every one was verified in the code before it
was believed.

| finding | outcome |
|---|---|
| **§7 asks for a dashboard §1 and §10 forbid building** — `compose_text()` had no extension point, so the agent read all three rules and correctly built nothing | fixed: `extraServices` splices services into the generated stack, with the project prefix and a CPU cap required, and what was added is recorded in the results header |
| **Preflight contradicted itself on worker memory** — "uncapped" on one line, a 4096m budget three lines later, and that phantom written into `suite.json` as `heldStill.tmProcessMemory` | fixed: one `tm_memory_capped()` test answers it in both places; uncapped reports uncapped and records null |
| **The host probe was quoted as a bound after one reading** — 76.4%, 83% and 91% across three runs on an idle machine | fixed: three repeats, median with its range. Measured while fixing it, memory-bound 2→4 reads 79% [76%–83%] against register-only's 98% [98%–99%], so the noise is real and specific to the arm that matters |
| **The reaper is wider than documented** — it kills any process naming the project path, and took a plain `tail` of the log | fixed in the harness README, which had warned only about commands naming `prove.py` |
| **The interview has no unattended mode**, the clean-room case exactly | fixed: answer all nine yourself and write each down as a stated assumption |
| **Sizing before the first measurement is circular** — size from a rate the tiny proof exists to measure | fixed: guess high, measure, re-size; and "tiny" is not small for a fast pipeline, 150,000,000 here |
| **`pipeline.example.json` shipped `outputsPerInput: 8`** with two output topics and no derivation; `killAtFraction: 0.35` landed at 62.7% | fixed: the derivation ships with it, and the kill point is documented as approximate to the next commit |

In [`scalable-flink-skill@067c21f`](https://github.com/jimzucker/scalable-flink-skill/commit/067c21f)
and [`@874ab56`](https://github.com/jimzucker/scalable-flink-skill/commit/874ab56).

## Cost

| | |
|---|---:|
| wall, empty directory to teardown | **2.15 h** |
| tool calls | 83 |
| `results/DONE` | `FAIL at report 51.2 min` (attempt 2) |
| phases | up 2.1 s, preflight 96.8 s, completeness 393.5 s, tiny proof 549.2 s, fill 431.9 s, suite 1598.3 s |
| attempts | 3 — refused at the tiny proof, a valid table missing the claim, then the 6 GiB arm |

The final arm ran as `up` + `suite` rather than `all`, so **no `DONE` describes
it**: `results/DONE` holds attempt 2's verdict while `results/suite.json` is the
6 GiB arm. Worth knowing when reading the directory.

Teardown clean, nothing with the run's prefix surviving, 46.7 GiB returned by
`fstrim`.
