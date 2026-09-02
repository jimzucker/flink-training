# Clean-room validation, run 5 — DataStream, and the table that had to be withdrawn

The first run with the API fixed rather than chosen: **Flink DataStream, no SQL
or Table API**, so it could be set against [run 4](clean-room-run-4.md), the only
run that chose SQL. Same conditions as [1](clean-room-run-1.md),
[2](clean-room-run-2.md), [3](clean-room-run-3.md) and 4: fresh agent, empty
directory, barred from this repository and from every earlier test directory, one
prompt, no human input.

**Model: Claude Opus 5.**

It is also the run that failed twice — once on the machine, once on the
measurement — and both failures were worth more than the table would have been.

## What it cost

| | |
|---|---:|
| Human time | **0 h** |
| Human prompts | **1** |
| Wall clock | 1.47 h |
| Tool calls | 84 |
| Assistant turns | 169 |
| Tokens | 19,965,149 |
| Cost, metered-API equivalent | $30.24 |

The cheapest and fastest of the five, continuing a monotone trend: 2.52 h and
$166.58 at run 1, against 1.47 h and $30.24 here, with the problem unchanged and
only the skill text between them.

## The first failure: a full disk

An earlier attempt at this run filled the host and died. It had sized its backlog
— 13 GB — and never sized what the backlog would *produce*. The two sink topics
carry ten times the input rate with no retention set, so four cases wrote 47 GB
on top of it.

The harness behaved correctly: the probe died, a guard refused, the suite stopped
rather than continuing, and it flushed its three good rows first. What failed was
arithmetic nobody had asked it to do. The loss was total anyway, because a full
disk leaves nothing to recover *with* — the tooling could not write its own
output files, and the cleanup that would have freed the space needed the shell
that the missing space had killed.

That produced the disk budget, the retention rule, and the host-versus-VM
distinction now in the preflight, plus a floor guard that fires *before* each
case. On the re-run the budget was computed before anything was filled —
predicted ~10.5 GB, actual **12 GB net** — and free space never fell below
115 GB against a 25 GB floor.

It also corrected the rule that had just been written for it: **`retention.bytes`
is a steady-state cap swept periodically, not a bound.** The sink log reached
27 GB against a 4 GB cap between sweeps. It worked around that by dropping and
recreating the sink topics per case, re-verifying the input backlog against its
recorded count after every destructive command — the same operation that silently
destroyed run 2's backlog, done safely.

## The second failure: the table

| case | TM cap | used | % of cap | orders/s | ratio | Kafka cap | Kafka used | busy | BP |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| p1 | 1 | 1.00 | 100% | 71,278 | 1.00× | 8.0 | 0.92 | 93.4% | **37.6%** |
| p2 | 2 | 2.00 | 100% | 139,113 | 1.95× | 8.0 | 1.07 | 93.7% | 36.5% |
| p3 | 3 | 2.91 | 97% | 167,804 | 2.35× | 8.0 | 1.06 | 97.0% | 46.5% |
| p4 | 4 | **3.78** | **94%** | 201,033 | 2.82× | 8.0 | 1.19 | 95.4% | 43.6% |

Descending agreed within +2.2 / −4.5 / +3.1 / +11.6%.

**The pipeline stopped being the constraint after two cores.** By p4 the task
manager could not consume its own cap and spent 43.6% of its time
back-pressured. The skill's own central rule — *you can only show that something
scales when it is the thing constrained* — was violated at p3 and p4, and those
rows were reported as scaling data.

The honest result is **1.95× at two cores, and this rig cannot demonstrate
scaling past that.** The 2.82× is a measurement of the sink path wearing a
scaling label.

There is a second error underneath it. The run concluded *the ceiling is the
transport* — but the broker held **8 cores and used 1.19**. It was nowhere near
CPU-limited. The back-pressure is the produce path at 10× fan-out, not broker
CPU. Its squeeze table found a genuine handover at 0.5 cores:

| broker cap | orders/s | TM cores used | BP |
|---|---:|---|---:|
| 2.0 | 234,809 | 3.79 of 4 | 35.4% |
| 1.0 | 198,670 | 3.78 | 42.7% |
| **0.5** | 119,828 | **3.21** | **68.1%** |

— but that is a different experiment, one where the broker was *artificially*
starved. It does not license a claim about the table above it.

By contrast run 4, at 5× fan-out, ran 0.0% back-pressure and 100% busy
throughout: pipeline-constrained the whole way, which is why its 3.78× means
something and this 2.82× does not.

## What it got right

- **Preflight caught three live traps** in 5 minutes: JDK not on `java_home`, a
  root-owned state volume, and the metrics reporter already bundled as a plugin.
- **Every guard broken on purpose and confirmed**, including the new disk floor.
- **Completeness as a separate gating run**: 6/6 assertions, no tolerances. The
  idempotence test killed the task manager mid-drain, genuinely replayed 452,821
  records, and all 4,160 per-key values still matched the manifest.
- **Two defects found by looking, not by guarding**: the generator was
  non-deterministic (wall clock in the record), and the first idempotence test
  proved nothing because the kill landed after the drain had finished.
- **The dashboard's first render found two defects invisible in the JSON**: the
  input series was flat zero — during a *drain* nothing produces to the input
  topic, so it now charts offsets consumed — and 4096 rendered as "4 K".

## What DataStream cost and bought

~300 lines where SQL would take ~20: hand-written parser, fan-out flatMap, POJOs,
key extractors, two `ValueState` descriptors, a Kafka record serializer.

What it bought: custom metrics *inside* its own operators, which is what makes a
live cardinality panel possible at all; and emitting the **absolute** position per
key rather than an increment, which it argued makes replay idempotent and used to
decline exactly-once as unnecessary. Its own verdict on the measurement: neither
easier nor harder — *"the broker doesn't know which API wrote the records."*

> **Correction, from [run 6](clean-room-run-6.md).** That idempotence argument is
> half wrong, and this run never tested it. Absolute emission does make the
> *sink* idempotent. It says nothing about the keyed state the value is computed
> from: at-least-once *checkpointing* does not align barriers on recovery, so
> records already folded into a snapshot are replayed into it and the sum itself
> is wrong. Run 6 built the same design, killed the task manager mid-drain, and
> got 1,604,176 and 1,605,242 against an expected 1,600,000 with the two
> aggregations disagreeing. The fix is to split the settings — exactly-once
> checkpointing, at-least-once sink — after which the same kill replayed 455,888
> rows with every per-key value still exactly right. The skill carried this run's
> reasoning as an exemplar until run 6 disproved it.

**The DS-versus-SQL comparison this run existed to make is still unmade.** Run 4
used a 5× fan-out against this run's 10×, so the two did different amounts of
downstream work, and this run's table is withdrawn regardless.

## What it sent back into the skill

- **a disk budget with the arithmetic written out**, and retention on any topic
  the harness writes but never drains
- **`retention.bytes` is a sweep, not a bound** — its own correction
- **the host-versus-VM distinction**, and the `fstrim` that returns the blocks
- **constraint ownership becomes a guard**: refuse the suite at the baseline if
  the component under test is under ~98% of its cap or back-pressured; treat a
  fall below ~95% at any case as the ceiling rather than a data point
- **the tiny proof runs two cases, not one**, so constraint ownership is
  established for two minutes at small scale instead of forty at full scale

And the finding that outranks the rest: **it obeyed every rule written as a guard
and broke a rule written as prose, in the same hour, with the violating number
printed in its own results table.** Across five runs that pattern holds without
exception. An audit then counted eighty rules and seven guards, and fourteen
mechanisable rules became [the mandatory guard
set](../../.claude/skills/prove-it-scales/SKILL.md) — with a promotion rule
attached to the skill itself: *when a run breaks a rule, that rule becomes a
guard with a self-test, or it gets deleted.*
