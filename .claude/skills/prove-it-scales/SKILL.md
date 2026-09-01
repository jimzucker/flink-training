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

8. **Which API or abstraction level?** Most engines offer more than one — a
   declarative or SQL layer, and a lower-level one you write operators against.
   Ask, and record the answer, because it changes what your measurement means. A
   declarative layer plans the job for you: the operator graph, the state layout
   and the serialization are chosen by an optimiser and can change between
   versions. That is often the better engineering choice, and it makes "I added a
   core and throughput scaled" a claim about *the plan the optimiser produced
   today*. With hand-written operators you know what the graph is because you
   wrote it, and you own the state layout that goes with it. Neither is more
   valid. But someone who knows the difference will ask which you used, and "I did
   not think about it" is a poor answer to a question about your own measurement.
   Default to whichever the audience actually runs in production.

**Stop when you can state the claim and the fan-out.** That is usually four to
six questions, not all eight — skip any the user has already answered. If you are
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

**Is every image native to the host architecture?** This is the one that can
silently invalidate the whole study. Some projects publish only `amd64`; on an
`arm64` host those run under emulation with no crash, no warning and no error —
just CPU numbers that mean nothing, because you are measuring a translation
layer. One run caught it on a `docker pull` platform warning and switched to a
multi-arch image. **For a CPU-scaling study, check the architecture of every image
against the host before you measure anything**, and prefer the multi-arch
publication where one exists.

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

### Start the longest-running thing first

Nothing except the cases depends on the backlog, so fill it the moment the tiny
end-to-end proof passes, and build the harness and the dashboard while it fills.
Runs that discovered this by accident overlapped roughly an hour of measurement
with other work; runs that filled late waited for it twice.

The same applies within the suite. A case is warm-up plus window plus the
recreate-and-submit around it, and the agent has nothing to do during all of it —
that is the time to write the next thing, not to watch a progress bar.

**Choose the checkpoint interval partly for what it costs you to measure.** The
window guard needs at least three commit boundaries inside it, so a 30-second
interval forces a 90-second window and a 10-second interval allows about 40. Over
a dozen cases that is a quarter of an hour. The interval still has to be constant
across cases and reported in the header — pick it deliberately, then leave it
alone.

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

## Prove nothing was lost, separately from proving it is fast

A throughput number says how quickly records moved. It says nothing about whether
they all arrived, or arrived once. **A fast pipeline that drops one record in ten
thousand is worthless, and every measurement in your table will look fine.**

These are two different runs and you need both.

### The completeness run drains to the end; the measurement run does not

Every rule about windows — hold the backlog, refuse if it drains inside the
window, sample the middle — exists because a *measurement* looks at a slice of a
steady state. Completeness cannot be checked on a slice. Take a backlog small
enough to drain to the last record, let it finish, and then assert against it.
Small and complete, not large and sampled.

### Derive the expected answer from the input, not from the pipeline

The check is worthless if it asks the pipeline what it produced and then agrees
with itself. Have the generator write a manifest as it produces — totals per key,
record count, whatever the aggregation is supposed to sum to — and compare the
sinks against *that*. One run precomputed every expected output row before it ran
anything; another had its generator emit a manifest and matched both aggregations
against it exactly.

### Assert four things, with no tolerances

- **Cardinality.** You predicted the number of distinct keys during the interview.
  Assert it. A key count that is one too high means a key you did not intend
  exists; one too low means a key never arrived.
- **Totals.** Every aggregation sums to what the manifest says it should. Exactly.
  Fixed reference data is what makes this arithmetic rather than statistics — if
  the answer needs a tolerance, the test is too vague to catch anything.
- **The paths agree.** If the same quantity is computed two ways — two
  aggregations over the same input, or a count from the source and a count from
  the sink — they must be equal, and a difference is a lost or duplicated record
  rather than noise.
- **Idempotence, if you claim it.** Exactly-once is a claim about failure. Without
  a failure the setting is untested and at-least-once looks identical. Kill a
  worker mid-run, let it recover, and assert the totals again.

### The generator has to be deterministic, or none of this works

Everything above assumes you can state the expected answer before the run. That
requires the input to be reproducible: same seed, same records, same order, in the
same partitions. Seed it explicitly and derive per-partition seeds from that one
seed, so parallel generation stays reproducible.

Without it you cannot precompute expectations, cannot compare two runs, and cannot
tell a regression from a different random draw. It is a precondition, not a nicety
— check early that two fills with the same seed produce byte-identical output.

### Record which build produced each number

Put the artifact's identity — a jar hash, an image digest — in the result file
next to the throughput. When a table looks wrong three days later, the first
question is whether its rows came from the same code, and it is unanswerable
afterwards unless you wrote it down at the time.

### Then gate the throughput on it

Run the completeness check first, and **refuse to report any performance number
if it fails**. It belongs in the same harness as the other guards: a case whose
outputs do not reconcile is not a slow case, it is a broken one, and its
throughput is meaningless rather than merely disappointing.

Prefer a monotonic quantity for anything you also chart. A signed running total is
a random walk, so a fraction of a second of scrape skew between two series renders
as a divergence that is not real — one run built exactly that panel, saw an
alarming 8% gap between two identical paths, and replaced it with a count where
the same skew is invisible and a genuinely lost record is not.

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

**"The backlog did not run out" is not the guard you want.** A case that consumed
39,852,303 of 40,000,000 records passed a `remaining > 0` check while the source
was starved for the tail of the window — the component under test fell off its cap
and the harness reported the resulting number as a fact. **Require headroom, not
non-emptiness**: at the rate you just measured, at least a full checkpoint
interval of records must remain at window close. Not-quite-empty and
never-the-constraint are different things.

### A guard that has never fired is a guess

You will write guards for failures you have not seen yet. Some of them will be
wrong: checking the wrong thing, reading a field that is always null, comparing
against a threshold no real failure crosses.

**Break something on purpose and confirm the guard catches it.** Set the CPU cap
to the wrong value and check the run refuses. Point the harness at a stopped
cluster. Truncate the backlog mid-window. Each takes a minute, and the alternative
is discovering during a real failure that your safety net has a hole — which is
the moment it costs the most, because you will trust the number it printed.

Every guard in a mature harness should be traceable to something it caught. Guards
that have never fired are unverified code in the one component whose whole job is
to be trustworthy.

## Make the verification a gate, not an event

Verifying once proves the pipeline was correct once. The interesting question is
whether it is correct *now*, after the change someone made this morning.

**Put the completeness check in CI and let it fail the build.** Stand the stack up
from nothing, run the generator bounded by record count, assert the expected
outputs, tear it down. It costs minutes per push and it converts every one of the
assertions above from a thing you did into a thing that stays true.

Two properties matter more than coverage. It must run **the same script you run
locally**, or the two drift and the CI one becomes decorative. And it must start
**from nothing** — a cold start on a clean machine is the path every new person
takes, and it is the one most likely to be quietly broken by a change that works
fine on a machine with warm caches and leftover state.

A demo that fails in the room usually fails for something CI could have caught and
nobody had asked it to.

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
not clear an existing CPU quota, and it returns success either way, so the "cap
not applied" guard is the only thing standing between you and a case measured at
the previous case's limit.

**And the documented fix has its own trap.** Clearing it with `--cpu-quota=-1`
sets a CPU *period* on the container, after which the daemon rejects every later
`--cpus` with *"Nano CPUs cannot be updated as CPU Period has already been set"*.
**Pick one mechanism and use it exclusively** — `--cpus` throughout, or
quota/period throughout. Mixing them fails on the second case rather than the
first, which is exactly late enough to have cost you a fill. Assume any
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

**Back-pressure does not detect starvation, and the pair has three shapes, not
two.** Busy near 100% with no back-pressure is a component at its limit and
keeping up. Sustained back-pressure is one that cannot keep up. But a component
whose *input* is starved shows **busy falling while back-pressure stays at zero**
— it is not overwhelmed, it is waiting. One run squeezed its transport to the
point of handover and saw busy drop to 76.5% with back-pressure at 0.2%: the
broker was starving the source, not back-pressuring the sink. A panel that names
only the first two shapes reads that as "everything is fine".

**A ratio between two counters that advance on different clocks cannot be fixed by
widening the window.** If one side advances only at commit boundaries and the
other continuously, their ratio lags, and averaging over longer changes the number
without removing the lag. One run measured a true 5.00 fan-out as 5.98 at one
minute, 5.83 at three, 5.90 at five and 6.12 cumulative — then deleted the panel
rather than pick the flattering window. Chart both quantities and let the reader
see the constant factor; state the ratio only where you can compute it exactly.

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

### A starting panel set, and what to check on the first render

Independent runs converge on roughly the same panels, so start here rather than
discovering it:

| panel | the question |
|---|---|
| Rate per stage | is the fan-out real? Lines a constant factor apart |
| Distinct keys per aggregation | is the cardinality you predicted the one you got? |
| The two paths, overlaid | do two independent aggregations of the same input agree? |
| Busiest vs most back-pressured task | at the limit, falling behind, or **starved**? |
| CPU per component | which one is actually in the way — including the idle one |
| Backlog remaining | is this a drain, and did it run out? |
| Checkpoint duration | what does the guarantee cost? |

**Then render once and check these five before iterating**, because each has cost
a run a whole render cycle:

1. **Does the legend fit?** More series than rows clips, and a series below the
   fold looks missing.
2. **Is anything secretly on a second scale?** A count plotted against a percent
   axis reads as a flat line at the top.
3. **Is the timezone right?** The renderer is a headless browser with no timezone;
   an unset one comes back in UTC.
4. **Does every panel have data?** A "No data" panel usually means the
   provisioning half-applied, not that the metric is missing.
5. **Is any panel a ratio or a signed sum?** Both are unstable for reasons that
   have nothing to do with your pipeline. Prefer two lines and a monotonic count.

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
- **Say which API or abstraction level you used**, next to the numbers rather
  than in a footnote. A curve from a declarative engine and a curve from
  hand-written operators are both real and are not the same claim.
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

## Apply the rule to your own process, not only to the system

The central rule here — **you can only show that something scales when it is the
thing that is constrained** — is about the pipeline. It applies just as well to
how you are working, and it is easy to forget in that direction.

A worked example, from these runs. One run lost about 50 minutes to environment
traps, so those traps were written down and the next run lost only 19. Thirty-one
minutes genuinely removed — and the total run time fell by eight.

The rest went nowhere, because environment setup was never on the critical path.
Fills, warm-ups and measurement windows are **waiting**, and an agent writes the
dashboard while it waits. Roughly 64 minutes of that run's measurement overlapped
with other work. Removing 31 minutes of setup removed 31 minutes of something
that was already partly hidden behind the waiting.

Two things follow.

**Before optimising your own loop, find what actually gates it.** Time spent is
not the same as time on the critical path. The expensive-looking phase is often
slack, and the real gate is usually the thing you cannot parallelise — here, the
measurement floor, which is fills plus warm-ups plus windows and no skill text can
shorten it.

**Expect new work to generate new friction.** The same run that saved 31 minutes
on known traps hit four unknown ones — an image architecture mismatch, a
clear-the-setting command that created a second trap, a client library's silent
metadata-refresh delay, and a reserved word in a dialect. Writing down yesterday's
traps does not prevent tomorrow's, and a process improvement that assumes it will
is measuring the wrong thing.

## When something is blocked

Try twice, maybe three times. Then stop, say what was tried and why it failed,
and route around it. An agent that keeps retrying a blocked tool burns the
session without converging.
