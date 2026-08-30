---
name: prove-it-scales
description: Use when the user wants to build a data pipeline or service AND demonstrate that it scales — a training project, a capacity study, a proof for a demo or talk. Interviews one question at a time before building, then enforces a measurement discipline that produces numbers the audience cannot pick apart.
---

# Prove it scales

Building the thing is the easy half. Producing a number that survives a skeptical
reader is the hard half, and it is where these projects usually fail — not by
being wrong, but by being unattributable.

## Rule zero: interview before building

**Ask one question at a time. Wait for the answer. Do not batch them, and do not
start building until the shape is known.** A list of eight questions gets three
answered; one question gets a real answer and often changes the next question.

Keep each one short and concrete. Offer a default so the user can say "yes" and
move on.

### The questions, in order

1. **What is the input event, and what comes out the other end?** One sentence,
   in domain language. This is the whole spec; everything else is detail.
2. **Does one input become several outputs?** The fan-out ratio decides where the
   load actually lands. If one order becomes five records, the write side is five
   times the read side and that is usually what saturates first.
3. **How is it aggregated — what are the keys, and how many distinct ones?**
   Small fixed key counts make outputs *arithmetic* rather than statistical,
   which is what lets you assert exact numbers instead of tolerances.
4. **What has to be exactly right?** If any output is a running sum, a replayed
   record is a wrong number rather than a harmless duplicate, and the delivery
   guarantee is not optional. Say so now, because it puts a floor under latency
   that gets discovered embarrassingly late otherwise.
5. **Who watches the demo, and what do they need to believe at the end?** A
   capacity claim for managers and a correctness claim for engineers produce
   different builds. Ask before writing code, not before writing slides.
6. **Where does it run?** Prefer "a laptop" and make the user argue you out of
   it — see *Shrink the thing under test* below.
7. **What claim do you want to make when this is done?** Write it down verbatim.
   Every measurement decision afterwards is judged against whether it supports
   that sentence.

**Stop when you can state the claim and the fan-out.** That is usually four to
six questions, not all seven — skip any the user has already answered. If you are
past eight you are gathering rather than deciding, and the user has stopped
reading. Anything still unknown becomes a stated assumption in the plan, not
another question.

## Build in reviewable steps

One branch per step, squash-merged, one commit per step on the main line. Each
step ends with the system **running and measured**, not with code that compiles.
Pause for review between steps; run without prompting inside one.

Keep a journal: what drove the step, what was decided, how it was verified, what
its review changed. It costs minutes and it is the only thing that makes a
six-week-old decision explicable.

## The measurement discipline

### Shrink the thing under test, do not overwhelm everything else

The instinct is to add load until the component saturates. That needs a cluster
and it teaches the audience the wrong lesson before you speak: *this needs
expensive infrastructure to be interesting.*

**Invert it. Cap the component under test and hold everything else still.**
Capping a task manager at 1, 2 and 4 cores on one laptop reproduces the same
curve that took 88 million records, 32 vCPUs and three brokers the expensive way.

### You can only show that something scales when it is the thing constrained

This is the rule that gets violated most often, and it produces a flat line that
looks like a real finding. Raising parallelism while the engine already has every
core it can use spreads the same work over more threads and measures nothing.

**Buy the resource and the parallelism together** — one core *and* one degree of
parallelism per step. That is also what a cloud vendor sells (a KPU, a vCPU-unit),
so the number means something when someone goes to price it.

### Put the resource columns next to the throughput

Throughput alone cannot be attributed. Record, for every case:

- **what the component under test actually used** (1.00 of 1, 2.00 of 2, 3.94 of
  4 — this is what proves the units were real)
- **what every other component used** (the one that is in the way is often nearly
  idle — a broker at 0.48 cores can still be the ceiling because it has run out
  of *write throughput*, not CPU)
- **back-pressure or queueing**, so "waiting" is distinguishable from "working"

Without these, "it got slower" is not a diagnosis and the flat step at the end of
the curve is unexplainable.

### Measure a drain, not a live generator

Fill a backlog larger than the machine's page cache, **stop the producer**, then
measure the drain. A live generator measures the generator, and a backlog that
fits in RAM measures memory.

Hold partitions, checkpoint interval and backlog size constant across every case.
Nothing varies but the one thing under test.

**Carry the delivery guarantee into the plan, not just the design.** If question 4
said the outputs must be exactly right, then nothing is readable until the
checkpoint that produced it commits — which puts a floor under visible latency
equal to roughly the checkpoint interval, and makes that interval a number you
must fix, report, and never quietly change between cases. Decide it once, write
it in the results header, and expect someone to ask why the consumer-visible
latency is far larger than the processing time. The answer is the guarantee, not
the pipeline.

### Repeat the cheap measurement, not just the expensive one

The natural instinct is to repeat the run that cost money and trust the free one.
That is backwards: repeated cloud cases agreed within 2% while laptop cases
varied ~12%, and the laptop number was the one quoted as exact.

Quote a ratio when the point is that it scales. Quote an absolute only from a run
you repeated.

## Build the harness to refuse

**A benchmark that prints a number for every input will eventually print a wrong
one.** Make it stop instead. At minimum, refuse to report when:

- the resource limit was not actually applied (check the container, do not trust
  the environment variable)
- no job is actually running
- the backlog ran out inside the measurement window
- a previous run still owns the cluster
- the measured rate is zero or negative

Every guard should exist because it caught something. Add one each time a
confident wrong answer gets through.

## Generate every artifact that carries a number

Decks, charts and diagrams that quote figures are built by a script that reads
the figures. Two hand-maintained copies drift the first time a measurement
changes.

Then check the generated output against the source of truth anyway — generation
guarantees the copies agree with each other, not that they agree with the repo.
**Render the image and look at it.** Layout collisions, overflowing text and
silently-failed edits are found by looking, never by reading the code.

## Reporting: you are not writing a thesis

- **Lead with the outcome, not the road to it.** The story of how the measurement
  went wrong before it went right makes the work sound hard instead of the result
  sound cheap.
- **Stop after the evidence.** A section that explains a result to someone who has
  already read it gets cut.
- **Match the register to the audience.** Managers want what it means and what it
  cost. Engineers want the method. Do not serve both in one paragraph — put the
  rigour in an appendix, a comment, or the repo.
- **State the scope limit once, where the technical reader will meet it.** "X
  scales linearly" is a claim about the engine; one pipeline supports "this
  pipeline scaled linearly". The narrow claim survives contact with an expert;
  the broad one invites a correction in public.
- **Say where it stops.** Naming the ceiling makes the rest credible. Volunteering
  it is stronger than being asked.

## When something is blocked

Try twice, maybe three times. Then stop, say what was tried and why it failed,
and route around it. An agent that keeps retrying a blocked tool burns the
session without converging.
