# Every change made to chase a number

Each entry names what changed, which row of section 6a it is, and **what I
expected it to move, written before it was measured**. A change that made the
step worse is reverted, and the entry stays.

---

## Fix 1 — `pipeline.max-parallelism: 577`

**Section 6a row:** spread the keys evenly.
**When:** before the chain, after the first preflight and before the stack was
brought up for the chain. `flinkProperties` is read when the stack comes up, so
this had to be in the file first.

**What preflight found, quoted:**

> `FAIL  keys divide evenly across subtasks   the position-by-symbol stage at 4
> cores would get 1/1/0/2 keys across its subtasks, so subtask 2 gets no keys at
> all. That core cannot reach a cap with no work to do, and the case would be
> measuring the key layout rather than cores. Set pipeline.max-parallelism to
> 577 in flinkProperties, which divides every key set evenly at every case.`

**Prediction, written before measuring:** at four cores one of the four
subtasks of the symbol aggregation would have had nothing to do at all. That is
not a percentage loss, it is a quarter of the stage idle, so the 2-to-4 step
would have been bounded well below linear no matter what the pipeline did.
With 577 key groups every case gets 1/1/1/1 symbol keys and 4/4/4/4 account
keys, and the 2-to-4 step is no longer bounded by the layout.

This is arithmetic, not a measurement — a subtask with no keys does no work —
so it is not an A/B and the two arms were not run. There is no "before" number
because the run had not measured anything yet.

**Outcome:** preflight passes the key row. Recorded here because it is a change
made to move a number, and the run should not be able to hide it.

---

## Measure before you guess — the two cheap measurements, run before any lever

The suite came back with every case thrown out for spread and no step
reportable, and the scorecard said *Pipeline CPU* on all three rows. Section 6
says neither of these is optional in that situation, and that they come before
any change. **No lever was applied before them, and none after: the run was
stopped here.**

### Measurement 1 — `prove.py probe --repeats 9` (nothing starts, no pipeline involved)

**Prediction, written before running it:** the pipeline parses JSON and
allocates per record, so the memory-heavy arm is the relevant one. I expected
it to come back under 2x but could not guess by how much.

**Result:**

```
  simple arithmetic    per core: 1c 628,600,967  2c 608,742,495  4c 597,558,372
  simple arithmetic    1->2: 1.92x  [1.89-2.35x]
  simple arithmetic    2->4: 1.96x  [1.86-2.11x]
  memory-heavy work    per core: 1c 519,319,546  2c 463,585,724  4c 347,590,060
  memory-heavy work    1->2: 1.81x  [1.72-2.24x]
  memory-heavy work    2->4: 1.57x  [1.21-2.00x]

  simple arithmetic    full range 23%, middle half 1%
  memory-heavy work    full range 40%, middle half 11%
```

**What it settles, and what it does not.** On this machine, with no pipeline in
the way at all, memory-heavy work returns **1.57x on a doubling from 2 to 4
cores**, against a claim floor of 1.90x. The tiny proof measured this
pipeline's 2-to-4 at **1.763x** — which is *above* the bare machine's
memory-heavy arm and below its register-only arm.

The middle half of the memory-heavy arm is **11%**, and the gap I would be
explaining (1.763 against 1.90) is **7%**. The bound moves more than the thing
it is meant to explain. **So the machine cannot be ruled in or out here, and I
do not know whether this pipeline has a scaling problem of its own.** That is
the complete answer and it costs one line.

### Measurement 2 — `prove.py ceiling` (hold the 4-core case, squeeze Kafka in steps)

**Prediction, written after launching it and before reading a single line of
its output:** Kafka's container CPU peaked at **1.897 of its 2.5-core cap**
across the whole suite, read off the dashboard's own exporter after the suite
had ended. So I expect the 4-core rate to hold roughly flat as Kafka is
squeezed from 2.5 down to about 1.5 cores, and to fall off sharply below that.
If it holds, the broker's **CPU** is not what the largest case is waiting on —
which leaves the broker's memory and the disk underneath it as the open
question, and this command does not test either of those.

**Result:**

```
[21:00:19] ---- ceiling: broker capped at 2.5 cores, pipeline at 4 ----
[21:06:04]   broker 0.99/2.5 (40%)  cores 99.8%  234,146 rec/s  srcIdle 1.5%
[21:06:05] ---- ceiling: broker capped at 1.0 cores, pipeline at 4 ----
[21:11:36]   at broker cap 1: the rate never settled down within 240s. ... The last
             readings were [849490, 134956, 144070, 145648] records a second,
             drifting 1.98 with scatter 2.2431.
[21:11:36] ---- ceiling: broker capped at 0.5 cores, pipeline at 4 ----
[21:17:26]   at broker cap 0.5: the pipeline only used 81.5% of the 4 cores it was
             given, and it needs 95% to count.
```

**What it settles, and what it does not.** At its normal 2.5-core cap the broker
used **0.99 cores — 40% of what it was allowed** — while the pipeline held
**99.8% of its four**. Squeezing the broker to half a core does take the
pipeline off its cap, to 81.5%, so the handover is real and it is below one
core. **The broker's CPU is not what the four-core case is waiting on.** My
prediction was right about the shape and wrong to expect a clean handover at
1.5 cores: the one-core step could not settle at all, so where it gives out is
located only between 0.5 and 2.5 cores.

It says nothing about the broker's **memory**, which is what the guard actually
blamed on the one-core case, and nothing about the disk underneath it. Those
remain open.

### And the thing neither of them explains

The four-core case produced five readings this afternoon, on **one build, one
backlog, one rig**:

| when | records a second | pipeline CPU |
|---|---:|---:|
| tiny proof | 422,894 | 97.3% of cap |
| suite, pass 1 ascending | 115,234 | 98.7% |
| suite, pass 2 descending | 473,467 | 99.4% |
| suite, pass 3 ascending | 140,069 | 97.8% |
| ceiling, broker at 2.5 | 234,146 | 99.8% |

Every one of them sat at or near its CPU cap. That is the shape section 5
describes: *"a cgroup cap is a share of what the host has left, so the case
still pins at its cap and simply does less work for it — the one failure mode
every resource column in the table is blind to."* The harness caught it on one
pass and said so: **"the machine was busy with something else: load reached
27.4 on 8 cores while measuring."** At rest, after the suite, this machine's
load average was 14.08 with `mobileassetd` at 42% of a core and
`modelcatalogd` at 15%, neither of which belongs to this run and neither of
which I have any business killing.

**No lever from section 6a was applied.** Applying one now would be measuring a
change against a rig whose four-core case moves by a factor of four on its own.
