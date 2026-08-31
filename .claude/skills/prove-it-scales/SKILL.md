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

**And ask whether the guarantee is actually needed.** "It is a running sum, so a
replay is a wrong number" is the usual answer, and it is often right. But it turns
on what you *emit*, not what you compute: if each output row carries the
**absolute** value for its key rather than a delta, a replay restates the position
instead of double-counting it, and the sink is idempotent whatever the engine
does. One run reasoned exactly this way, chose at-least-once deliberately, and
kept an exactly-once flag available in case the choice was challenged. That is a
better answer than buying a guarantee reflexively, because exactly-once is not
free — it puts a floor under visible latency equal to the commit interval.

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

## Environment preflight: check these before you build anything

A measured hour of a clean-room run went to three environment traps, all
deterministic, none of them interesting. Check them first; each is one command.

**Does the JDK the engine needs actually resolve?** A machine can have several,
and the obvious lookup may not find the one a package manager installed. Resolve
it explicitly, print the version you resolved, and pin it — do not assume the
default `java` on PATH is the one the engine supports.

**Will the engine be able to write to its own state directory?** A named container
volume is created root-owned, and an image whose entrypoint drops to an unprivileged
user then cannot write to it. Overriding the container's user does not help,
because the entrypoint drops privileges anyway. The failure arrives as every job
being rejected for a directory it cannot create, which reads like a code problem
and is not. Fix the ownership when the volume is created, and confirm by writing a
file as the runtime user before submitting anything.

**Is the metrics reporter already present?** Recent images often ship the exporter
as a bundled plugin. The widely-copied instruction to copy the jar out of an
`opt/` directory into `lib/` then produces a duplicate class and the container
dies at startup. Look before you copy.

**Then prove the loop end to end on a tiny input before you scale anything.** Fill
a few thousand records, run one case, assert the outputs, tear it down. A rig that
cannot measure a thousand records will not measure fifty million, and finding that
out at the small size costs minutes instead of an hour.

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

### To find the ceiling, starve the other component

The obvious way to find where the curve stops is to keep buying units until it
flattens. That is expensive, and on one machine you may not be able to buy enough.

**Squeeze instead.** Hold the component under test at its largest size and cap
*the thing beside it* — the broker, the disk, the network — in steps. Watch for
the handover: the constrained component pins at ~100% while the component under
test **falls off its own cap**, which is the unambiguous signal that the roles
have swapped.

A test run found this without being told: at a 2.0-core broker the pipeline ran
at 269,263/s with the broker at 33% util; halving to 1.0 cost 2%; halving again
to 0.5 cost 19%, drove the broker to 98%, and pulled the task manager down from
4.02 to 3.84 of its 4-core cap. That located the ceiling on a laptop in three
short runs, and it extrapolates: a component idling at a third of its capacity
tells you roughly how many more units it would take to saturate it.

### Read the numbers from the transport, not the engine

**The engine's own metrics are least trustworthy exactly when you need them.** At
100% CPU its metric-reporting service is starved along with everything else; one
measured case had Flink under-reporting its own output by about 3×.

Take throughput from the thing on the other side of the boundary — committed
broker offsets, rows in the sink table, files closed.

**Anchor the measurement window on commit boundaries, not on wall clock.** These
two rules interact and the interaction bites: a source commits its offsets *at*
checkpoints, so reading the input side at an arbitrary wall-clock instant
quantises it to the last checkpoint, and your two vantage points then disagree by
up to one checkpoint interval for no real reason. Open and close the window when
both sides have advanced. One run cut a 25% disagreement between vantage points
to 0.5% by doing this, and could then tighten its own cross-check guard to 12%. And when the delivery
guarantee is exactly-once, **read those outputs as committed only**: the raw log
end offset includes records inside open transactions that no consumer can see, so
it will tell you the pipeline is faster than it is. One harness correctly refused
a perfectly good case because its own measurement was reading uncommitted
offsets.

**The metric you need is often not on the documented endpoint.** Engines
deprecate and empty out APIs faster than their docs and blog posts admit. One run
lost ten minutes to two back-pressure endpoints that returned empty or
`deprecated` on a current version, when the usable counters were sitting on a
different endpoint entirely. Dump the endpoint and read what is actually there
before trusting a path you found in a search result.

**Every number in the table comes from one build.** If you change the job — a
metric, a serializer, a key — the earlier cases were measured against different
code. Re-run them. Two runs did exactly this and said so; a table whose rows come
from different jars is not a curve, it is a collection.

**Size the backlog for the longest thing you will do with it, not the longest
measurement.** The window guard stops you measuring silence, but a backlog that
comfortably outlasts a 60-second case can still drain halfway through the
five-minute load you wanted for a dashboard image — and half that picture is then
an idle pipeline. Count every use before you fill.

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

### A refusal stops the suite

Refusing one case is only half of it. **When a case refuses, stop — do not run the
rest.**

Refusals are almost always systemic. If the resource cap did not apply at one
core it will not apply at two; if the slot count is wrong, or a previous run still
holds the cluster, or two vantage points disagree about the rate, then the rig is
wrong rather than the case. Every later number is suspect even if it prints, and
running them costs a warm-up each to learn nothing.

Say so and exit: *"stopping here: the remaining cases would fail the same way."*

The one thing worth doing instead of stopping is retrying the same case once, when
the failure is obviously transient — a submission that did not take, a container
that had not finished starting. If the retry refuses too, stop.

A suite that logs "REFUSED — continuing" for each guard will spend an hour
producing a table with holes in it and no account of the holes. That is worse than
no table, because the holes look like data points that happened to be missing
rather than a rig that was broken the whole time.

## Traps that only appear because you run it many times

A scaling suite restarts the job dozens of times against one cluster. That is not
how the system runs in production, and it surfaces failures a normal deployment
never sees. These cost real days here:

**Accumulated transactional state.** An exactly-once sink registers transactional
IDs with the broker. If the ID prefix is a fixed string, every run leaves its IDs
behind, and each subsequent startup must fence *all* of them before emitting a
single record. Ten runs was enough to reach 98,677 IDs and a **470-second** delay
before first output — which reads exactly like an inherent cold start that grows
mysteriously as the day goes on. Scope the prefix per run and it drops to 31
seconds. Symptom to recognise: sinks stuck initialising, first output minutes
late, zero checkpoints completing, and the delay getting worse the more you
measure.

**Memory arithmetic under exactly-once.** Transactional sinks allocate a producer
buffer *per producer*, and the producer count scales with parallelism times the
number of sinks. Those buffers come out of the same heap as the work. Get the
split wrong and the case does not fail — it thrashes: one measured run reported
3,085 records/sec that was pure garbage collection, against ~54,000 once the heap
split was fixed. A throughput-only harness prints the 3,085 as a fact. The
resource columns are what expose it.

**Infrastructure commands that quietly destroy the backlog.** Bringing up one
service brings up its dependency chain, and a changed config file makes the tool
recreate containers you did not name — one run lost a 15.6 GB backlog to a
`compose up` of a single service. Another, which had avoided that exact trap
entirely, then lost a 90-million-record backlog because `--delete --topic a
--topic b` silently deletes only one of them.

**So take the general rule, not the example: verify the backlog against a
recorded manifest immediately after any destructive or recreating infrastructure
command, not only before a measurement.** The specific commands that will bite
you are not enumerable in advance; the check after them is.

**Commands that succeed without doing anything.** `docker update --cpus 0` does
not clear an existing CPU quota — only `--cpu-quota=-1` does — and it returns
success either way, so the "cap not applied" guard is the only thing standing
between you and a case measured at the previous case's limit. Assume any
clear-the-setting command is a no-op until you have read the setting back.

**Slot arithmetic that fails silently.** Every job needs its own slots. Two jobs
at parallelism 4 need eight, not four. Get it wrong and the job does not fail — it
sits waiting for resources while the harness cheerfully times an empty pipeline.
Assert the slot count against the case before submitting.

**A cluster that is still busy from the last case.** An orphaned run holds slots
and starves the next submission. Worse, process-matching to kill it is unreliable
— a wrapper that `exec`s replaces the command line the pattern was matching. Verify
the cluster is idle by asking it, not by killing what you think is there.

**Warm-up hiding a defect.** If the first N seconds of every run are unusable,
the tempting fix is a longer warm-up. Do that only after finding out *why*. A
240-second warm-up here was hiding the transactional-ID bug for weeks; the
warm-up worked, which is what made it dangerous.

**Order effects.** Run the cases ascending and descending. If the curve differs,
something is warming up or accumulating between cases and the shape is partly an
artifact of the order.

## The dashboard is for explaining, not for measuring

A scaling result needs two different things and they are not the same artifact.
**The harness measures. The dashboard explains.** Keep the boundary sharp: the
moment a number in your report comes off a dashboard panel, you have inherited
every sampling and starvation problem the engine's metric pipeline has, at
exactly the load where it has them.

### Build it, and build it with the stack

A dashboard that exists only in somebody's browser is not part of the system.
Provision it from a file that ships alongside the compose stack, so `up` produces
the same dashboard on any machine and a panel someone improves during a demo
survives the next restart.

### Every panel answers a question someone will ask

Do not add a panel because the metric exists. Add it because you can name the
question. In practice, three earn their place immediately:

- **Rate per stage**, so the fan-out is visible rather than asserted — one input
  becoming N outputs shows up as parallel lines with a constant ratio.
- **Unique keys and totals**, so the cardinality you predicted before the run is
  visibly the cardinality you got. This is what turns a demo into a verification.
- **The saturation signal**: busiest task and most back-pressured task, side by
  side. Busy near 100% with no back-pressure is a system working at its limit and
  keeping up. Sustained back-pressure is a system that is not. **A throughput
  graph will never tell you which of those you are looking at**, and it is the one
  panel worth alerting on.

Group them in the order the talk covers them. A dashboard laid out like the
narrative is a dashboard the presenter can follow under pressure.

### A panel you cannot explain is a liability

If a number on the screen cannot be explained, it will be asked about, and the
honest answer costs more than the panel was worth. One project shipped an
"aborted checkpoints" panel whose count was always 1 — a harmless artifact of the
first checkpoint firing before every task was running. As labelled, it implied the
delivery guarantee was being retried, and it would have raised a false alarm
mid-demo. Either the panel says what the number means, or the panel goes.

### Render images server-side

For anything that ends up in a document, have the dashboard server render it
rather than screenshotting a browser. A browser extension on the presenting
machine was enough to stop panels drawing entirely while the dashboard itself was
perfectly correct — and a rendered image can be regenerated for a past time range,
so the picture in your write-up can be *the window the number came from* rather
than one taken at a convenient moment.

### One vocabulary across environments

If the same pipeline runs on a laptop and in the cloud, the metric names will not
match — different exporters, different labels. Define recording rules that map
both onto one set of names, and point the dashboard at those. Otherwise you
maintain two dashboards, and the second one is always the stale one.

### What bites when you actually build one

Five things that cost a run real time, none of them guessable from the JSON:

**The renderer is a headless browser with no timezone.** A dashboard set to
`browser` time renders in UTC and disagrees with the clock the presenter is
reading off the wall. Pass the timezone explicitly in the render URL.

**Verify the provisioned artifact actually loaded — the success code lies.** A
reload endpoint returned 200 over a half-written rules file on a bind mount, and
two panels read "No data" for a run with perfectly good data behind them.
Provisioning can silently half-apply; check that the rule or dashboard you just
pushed is really there.

**Engine rate meters are smoothed.** Per-second meters are typically ~60-second
moving averages, so a stage-rate panel ramps for a minute after start and decays
for a minute after stop. That shape reads as a slow pipeline to an audience, and
it is a second concrete reason the harness must never take its numbers off the
dashboard.

**Layout defects are invisible in the JSON — render and look for two in
particular.** A legend with more series than rows **clips** — legend clipping makes a series
look missing when it is merely below the fold. And a series on a different scale
plotted against a percent axis is a dual-axis panel in disguise, which reads as a
flat line at the top.

**Per-run identifiers pollute the dashboard too.** The transactional-ID trap has
a twin: consumer groups. After a dozen cases a backlog panel was twenty-two
frozen step functions, none of them the run being watched. Delete the group at the
end of each case, the same way you scope the transactional prefix.

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

## Log stderr from the very first version

Anything that runs unattended — a probe, a fill, a case — writes its stderr
somewhere you can read afterwards. Not `DEVNULL`, not swallowed.

A guard will catch the *consequence* of a silent failure, which is what guards are
for. But the cause stays invisible, and you will spend the time you saved on
diagnosis instead: one run lost 25 minutes to a probe dying quietly inside a call
with no timeout, and found it in seconds once stderr went to a file.

## Kill what you started to watch something

A long unattended run spawns watchers — a monitor waiting for a suite to finish,
a poll waiting for a probe to return, a loop waiting for a backlog to drain.

**When you abandon or supersede the thing being watched, kill the watcher.** A
case that refuses, a suite that stops, a run you replace with a better one: each
leaves its monitor behind, still waiting on something that will never happen.

The cost is not just a stray process. Orphaned watchers keep firing completions
long after the work is done, each one reporting elapsed time measured from the
*run's* start rather than its own — so a finished job looks like it is still
going, for hours. One run left four, and the last expired 90 minutes after the
last real measurement.

Track what you spawn and tear it down on every exit path, not only the happy one.

## When something is blocked

Try twice, maybe three times. Then stop, say what was tried and why it failed,
and route around it. An agent that keeps retrying a blocked tool burns the
session without converging.
