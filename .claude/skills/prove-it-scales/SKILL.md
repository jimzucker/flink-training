---
name: prove-it-scales
description: Use when the user wants to build a data pipeline or service AND demonstrate that it scales — a training project, a capacity study, a proof for a demo or talk. Interviews one question at a time before building, then enforces a measurement discipline that produces numbers the audience cannot pick apart.
---

# Prove it scales

Building the thing is the easy half. Producing a number that survives a skeptical
reader is the hard half. Everything below is either a **question** to ask before
building, a **preflight** check that prints PASS/FAIL, a **guard** the harness
refuses on, or a **rule of judgment** kept short enough to read. The
validation record behind each rule lives with the project that wrote it, not
here.

## 1. Interview before building

**Ask one question at a time. Wait for the answer. Do not start building until
the claim and the fan-out are known.** Offer a default with each question so the
user can say "yes". Stop as soon as you can state the claim — usually four to
six questions. Anything still unknown becomes a stated assumption in the plan.

1. **What is the input event, and what comes out?** One sentence, in domain
   language. This is the spec.
2. **Does one input become several outputs?** The fan-out ratio decides where
   the load lands: at 5× the write side is five times the read side and is
   usually what saturates first — and what fills the disk.
3. **What are the keys, and how many distinct ones?** Small fixed key counts
   make outputs arithmetic, so you can assert exact numbers instead of
   tolerances.
4. **What has to be exactly right?** If an output is a running sum, a replayed
   record is a wrong number, not a duplicate. Then decide **two settings, not
   one**: emitting the *absolute* value per key makes the **sink** idempotent
   (no transactions, no commit-interval latency floor); it does nothing for the
   keyed **state** the value is computed from, which needs exactly-once
   *checkpointing* or replayed records are folded into the snapshot twice. The
   usual answer is *exactly-once checkpointing, at-least-once sink*. Name both
   in the report, and expect to test it by killing a worker (§4).
5. **Who watches the demo, and what must they believe at the end?** Capacity for
   managers and correctness for engineers are different builds.
6. **Where does it run?** Default to a laptop; make the user argue you out of it.
7. **What claim do you want to make?** Write it verbatim. Every later decision
   is judged against that sentence.
8. **Which axis is the claim about?** *One worker growing* (cap one container's
   CPU and raise its parallelism — the laptop proxy) or *workers multiplying* (a
   second JVM with its own heap, GC and network — what a vendor sells as a
   unit). They are not the same measurement: a growing worker amortises its
   fixed cost, a multiplied one pays it again. Record the axis as a field in
   the results header, and on every case **parallelism = CPU cap = allocated
   slots**, read back from the engine.
9. **Which API level?** A declarative/SQL layer plans the graph for you and the
   plan can change between versions; hand-written operators are a graph you
   own. Neither is more valid, but the claim differs. Default to what the
   audience runs in production, and say which next to the numbers.

## 2. Build in reviewable steps

One branch per step, squash-merged. Each step ends with the system **running and
measured**, not compiling. Pause for review between steps; run without prompting
inside one. Keep a journal: what drove the step, what was decided, how it was
verified.

## 3. Preflight: assertions that print PASS or FAIL before anything is built

Each is one command. The cost of a violation scales with how late it is found —
a wrong architecture voids the study, a contaminated baseline voids the suite, a
bad window voids one case — so anything checkable now is checked now.

| check | how | fails as |
|---|---|---|
| every image is native to the host arch | `docker image inspect` `.Architecture` vs `uname -m` | emulated CPU numbers with no warning |
| the JDK the engine needs resolves | print the resolved version and pin it | wrong `java` on PATH |
| the engine can write its state directory | write a file as the runtime user | every job rejected for a directory it cannot create |
| the metrics reporter is not duplicated | look in the plugins dir before copying a jar into `lib/` | container dies at startup |
| disk budget on the **host**, not the container | `backlog + backlog × fan-out × undrained cases + checkpoint state` against host `df` | full disk with no shell to recover in |
| retention on every topic written but never drained | `retention.bytes` set; it is a periodic sweep, not a bound | sink log 7× its cap between sweeps |
| the generator is deterministic | two fills with one seed are byte-identical | no expected answer can be computed |
| CPU cap mechanism chosen once | `--cpus` throughout **or** quota/period throughout; `--cpus 0` is a no-op, `--cpu-quota=-1` sets a period the daemon then refuses to change | second case measured at the first case's cap |
| slots ≥ parallelism × jobs | compare before submitting | job waits for resources while the harness times an empty pipeline |
| transactional-ID prefix and consumer group are scoped per run | include the run id | 470-second cold start after ten runs; 22 dead series on the backlog panel |
| back-pressure counters exist on the endpoint you will read | dump the endpoint and read what is there | ten minutes on a deprecated path |
| the VM trim command is known | `docker run --rm --privileged --pid=host alpine nsenter -t 1 -m -u -n -i -- fstrim -v /var/lib/docker` | space freed inside a Docker Desktop VM never returns to the host |

**Then the tiny proof, before any fill.** A few thousand records, end to end:

- run **two** cases, one unit and two, and assert cap consumption in both;
- **bound the ratio to 1.5×–2.5×**. Superlinear is a defect report — it has
  been an artefact every time (a baseline time-sharing six unchained tasks on
  one core passed the cap guard at 99.9% and reported 3.73×);
- **kill a worker mid-drain and re-assert the totals** — a guarantee is a
  claim about failure and is untested until something has failed; finding out
  after the suite discards the suite;
- run one full case with a 10–15 s window so every line of the harness
  executes, **and fire one refusal on purpose** so you know refusals refuse.

Anything that can void the whole table is tested before the table exists.

## 4. Prove nothing was lost, separately from proving it is fast

A pipeline that drops one record in ten thousand looks fine in every throughput
column. Completeness is a separate run on a backlog small enough to **drain to
the last record**; measurement is a slice of a steady state and cannot check it.

The expected answer comes from the **input**, never from the pipeline: the
generator writes a manifest (record count, totals per key) and the sinks are
compared to that. Assert, with no tolerances:

| assertion | meaning of a miss |
|---|---|
| distinct keys = the number predicted in the interview | a key you did not intend, or one that never arrived |
| every aggregation sums to the manifest exactly | a lost or duplicated record |
| two paths over the same input agree exactly | same, located |
| after killing a worker mid-drain, all of the above still hold | the guarantee you configured is not the one you have |

Record the build hash beside every number, and **gate throughput on this**: no
table is published for a build that has not passed. Put the same script in CI
from a cold start, so it stays true after this morning's change.

## 5. Measurement discipline

**Cap the component under test; hold everything else still.** Capping a task
manager at 1, 2 and 4 cores on one laptop reproduces the curve that took 32
vCPUs and three brokers the expensive way, and it does not teach the audience
that the result needs a cluster.

**Buy the resource and the parallelism together**, one core and one degree per
step — that is what a vendor sells, so the number prices.

**You can only show that something scales when it is the thing constrained.**
This is the rule runs break most, always with the evidence in their own table,
so the harness owns it (§6): ≥98% of cap at the baseline, ≥95% at every case,
and no material back-pressure **at the boundary to the external component**.
Internal back-pressure inside a capped single-slot worker is expected — the
source waits on the aggregation threads sharing its core — so it is reported in
a column and gated on nothing.

**The baseline is a case, not a reference point.** Anything true of the other
cases and not of it — a shuffle, a network hop, a second JVM — is a difference
you are attributing to scaling. At parallelism 1 the same job can be written to
chain into one vertex with no serialization; the cases above it cannot. Measured
on one build, changing only that:

| baseline | throughput | 1→4 |
|---|---:|---:|
| chained, no shuffle | 211,533 | 2.16× |
| same graph as every other case | 140,308 | **3.26×** |

So **read the job graph back off the running plan and refuse any row whose
shape differs from the others**, and **lead with step ratios** — 2→4, not 1→4.
A step ratio has no privileged case in it, a faster single-thread
implementation cannot be punished by it, and it is the step someone will
actually buy. Quote the baseline ratio second, and say what it is measured
against.

**Measure a drain, not a live generator.** Fill a backlog larger than the page
cache, stop the producer, measure the drain. Hold partitions, checkpoint
interval and backlog constant across cases; nothing varies but the one thing
under test. Do not shorten the interval to save time once cases have run — it
changes the number.

**Read throughput from the transport, not the engine.** At 100% CPU the
engine's metric service is starved with everything else; one case
under-reported itself by 3×. Use committed broker offsets (committed only under
exactly-once — the log end includes open transactions), rows in the sink, files
closed.

**Anchor the window on the committed offset advancing**, not on wall clock and
not on checkpoint completion — the commit lands asynchronously after the
checkpoint, and a window opened on completion reads a number that has not moved
yet. Open on the tick the offset changes, close after at least three commit
boundaries. This took one run's vantage-point disagreement from 25% to 0.5%.

**Read CPU from the cumulative cgroup counter** (`cpu.stat usage_usec` at open
and close, divided by elapsed), not `docker stats`, which samples: a valid case
was refused at 94.4% sampled while the counter said 97.8%. The same file gives
`throttled_usec`, which is direct evidence the cap is what binds.

**Put the resource columns next to the throughput** for every case: what the
component under test used (3.94 of 4), what every other component used (a
broker at 0.48 cores can still be the ceiling — it ran out of write throughput,
not CPU), and back-pressure, so waiting is distinguishable from working.

**Warm up to a flat trend, not a round number.** Fit four intervals and require
a flat slope; two neighbours agreeing is a coin flip against ±10% noise.

**Every case at least twice, and report the spread.** The same case measured
three times spread 10–17% — wider than a step ratio's effect. A case whose
spread exceeds 10% is **unreportable on its own and voids every ratio it is
part of**; it does not void the suite. One run's 1-core case spread 14–42% in
seven consecutive suites while its 2- and 4-core cases held under 6%, and a
suite-wide refusal threw away six valid 2→4 measurements.

**Ascending then descending.** If the curve differs, something warms or
accumulates between cases and the shape is partly the order.

**To find the ceiling, starve the other component.** Hold the component under
test at its largest size and cap the thing beside it in steps. The handover is
unambiguous: the squeezed component pins at ~100% while the component under
test falls off its own cap. Three short runs locate it on a laptop, and the idle
fraction says roughly how many more units it would take. A starved input shows
as **busy falling with back-pressure at zero** — not overwhelmed, waiting.

**Every number in the table comes from one build.** Change the job, re-run the
rows. A table from different jars is a collection, not a curve.

### Budget the suite before running it

A case is warm-up + window + submit-and-settle, and across one suite the gaps
between windows equalled the windows. Count cases like money: the window is a
floor set by the checkpoint interval (three boundaries; four is prudent — 40 s
at a 10 s interval, not 60); the descending pass detects order effects and is
not a second table; size the backlog for the longest single *use* — including a
dashboard image — plus headroom, not for the suite. Record submit, steady,
open and close timestamps per case so the next budget is measured.

**Start the fill the moment the tiny proof passes** and build the harness and
dashboard while it runs. Nothing but the cases depends on it.

## 6. The harness refuses

**A benchmark that prints a number for every input will eventually print a
wrong one.** A harness that does not implement and self-test every guard below
is not finished. Each one exists because a run paid for it.

| the harness refuses when | how it checks |
|---|---|
| the resource cap was not applied | read it back from the container, never the environment variable |
| parallelism ≠ cap ≠ allocated slots | all three read back from the engine on every case |
| the job graph differs from the other cases | vertex count and edge ship strategies read off the running plan |
| the component under test is not the constraint | ≥98% of cap at baseline, ≥95% per case; external-boundary back-pressure not material |
| a refused case still owns the cluster | job torn down on **every** exit path |
| no job is actually running | engine reports RUNNING with the expected parallelism |
| the cluster is still busy from the last case | assert idle by asking the engine, not by killing what you think is there |
| the backlog lacks headroom at window close | a full checkpoint interval of records remains **at the measured rate** — not merely `remaining > 0` |
| the window is not anchored on commit boundaries | ≥3 boundaries inside the window |
| two vantage points disagree | transport and manifest agree within a stated tolerance |
| a rate came from the engine | the rate source is the transport's committed offsets |
| the measured rate is zero or negative | — |
| a case's passes spread >10% | every case run ≥2×; that case and every ratio it is part of are marked unreportable — the other cases still report |
| rows came from different builds | one build hash across the table |
| observed cardinality ≠ predicted | distinct keys vs the interview's answer |
| completeness has not passed for this build | §4, with no tolerances |
| host free disk is below the next case's write | checked **before** the case — a full disk takes the shell down with it |
| a monitor outlived the thing it watched | at teardown, no child the run started survives |

**A refusal stops the suite — when it is about the rig.** A cap that did not
apply at one core will not apply at two; a busy cluster, a bad window anchor,
disagreeing vantage points are the same at every case. A guard about one
case's *data* — spread, headroom at close — marks that case and moves on. Retry the same case once if the failure is
plainly transient; if it refuses again, say *"stopping here: the remaining
cases would fail the same way"* and exit. "REFUSED — continuing" produces a
table with holes that look like data.

**Assert the effect, never the exit code.** `docker update --cpus 0` reports
success and does nothing; a metrics reload returned 200 over a half-written
file; a topic delete removed one of two topics and said nothing. Every
state-changing command gets a read-back, and the read-back is the guard. After
any destructive or recreating infrastructure command — a `compose up` of one
service recreates its dependencies — **re-verify the backlog against its
recorded manifest** before trusting it.

**A guard that has never fired is a guess.** Break each one on purpose — wrong
cap, stopped cluster, truncated backlog — and confirm it refuses. Assert that
anything launched unattended is alive before waiting on it, and log its stderr
to a file from the first version; `DEVNULL` turns a one-line diagnosis into a
half-hour one.

**Anything that watches the stack runs as part of the stack.** Four
consecutive runs left a host-side sampler running with the teardown assertion
passing honestly, because the harness cannot see a process it did not start.
Put samplers in the compose file; `compose down` reaps them.

**The promotion rule:** when a run breaks a rule, that rule becomes a guard
with a self-test, or it is deleted. Sort every new rule into one of three piles
— *checkable while running* (a guard above), *checkable before running* (a
preflight row in §3), *judgment* (prose, and keep that pile small).

## 7. The dashboard explains; the harness measures

Build it, provision it from a file that ships with the stack, and never take a
reported number off it — engine meters are ~60 s moving averages and its
metric service starves at exactly the load you care about. Render images
server-side, with the timezone passed explicitly, so the picture in the
write-up is the window the number came from.

| panel | the question it answers |
|---|---|
| rate per stage | is the fan-out real? lines a constant factor apart |
| distinct keys per aggregation | is the predicted cardinality the one you got? |
| the two paths, overlaid | do two independent aggregations agree? |
| busiest vs most back-pressured task | at the limit, falling behind, or **starved**? |
| CPU per component | which one is in the way — including the idle one |
| backlog remaining | is this a drain, and did it run out? |
| checkpoint duration | what does the guarantee cost? |

On the first render check five things: the legend fits; nothing is secretly on
a second axis; the timezone is right; every panel has data (a "No data" panel
usually means provisioning half-applied — verify the artifact loaded, the
success code lies); and no panel is a ratio of counters on different clocks or
a signed sum, because both are unstable for reasons unrelated to the pipeline.
**A panel you cannot explain is a liability** — either it says what the number
means or it goes.

## 8. An explanation is a measurement, not a story

A number short of expectation invites a reason, and a plausible reason is cheap
to produce and expensive to be wrong about. A mechanism is a claim about cause,
so it needs the same evidence as a throughput claim: **one rig, one build, one
variable changed, both arms measured.** Anything less is a hypothesis and is
labelled as one or left out.

Two failures are easy and expensive: **explaining your number with someone
else's run** — absolute throughput is not comparable across implementations,
and neither are explanations of it; one investigation spent hours on a
ten-percent shortfall that belonged to a different pipeline entirely — and
**arithmetic offered as evidence**, which runs backwards easily: fixed
per-worker cost *flatters* wider cases, and was offered to explain a
sub-linear one.

When a result is short: say what you measured, say what you have ruled out and
with what evidence, and say the cause is unknown. *"I do not know yet"* costs
one line.

## 9. Reporting

- **Lead with the step ratio** the reader would buy — two units to four — with
  its efficiency and spread. Baseline ratio second, stating what it is against.
- **Lead with the outcome, not the road to it**, and stop after the evidence.
- **Header fields:** axis (§1 q8), API level, guarantee as two settings,
  checkpoint interval, build hash, passes per case.
- **State the scope once**, where the technical reader will meet it: one
  pipeline supports "this pipeline scaled linearly", not "the engine scales".
- **Say where it stops.** Naming the ceiling makes the rest credible.
- **Generate every artifact that carries a number** from the results file, then
  render it and look — layout collisions are found by looking, never by
  reading the code.

## 10. When something is blocked

Try twice, maybe three times. Then stop, say what was tried and why it failed,
and route around it. Kill every watcher you started for the thing you abandoned.
