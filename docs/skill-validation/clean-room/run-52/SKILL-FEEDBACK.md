# Skill feedback — clean-room run 52

Every place the skill or the harness was wrong, unclear, out of date, or cost
time. Each item quotes the exact wording, says what happened, and says what it
should say instead. "Code-read" means I found it by reading the harness, not by
tripping over it in a run.

## F1. The design diff forbids the duplicates that section 4 asks for (cost: one completeness run, about 5 minutes)

SKILL.md §4 asks for repeats on purpose and says they are compared loosely:

> **the input repeats a small share of its records on purpose**, the way a producer that retries would, and every total still matches the manifest, which counts each record once ... Keep it small: the dropped repeats make the output count trail the input count by that share, and the two are compared within 5%

The completeness design diff (`lib.design_diff`, fed from `prove.py cmd_completeness`) compares them exactly, as strings:

```
ok = str(want) == str(got)
```

where `got` is `round(sink_rows / c.small, 3)`. My build dropped every repeat
correctly (the verifier passed the clean arm), and the run stopped with:

> COMPLETENESS STOPPED: the job does not match the design the run wrote down: constraint outputsPerInput = 5.0, read back 4.987.

So "compared within 5%" is not true: any repeat share at all forces
`outputsPerInput` below the job's real fan-out, to the third decimal. That also
contradicts §5's own rule:

> **A fan-out is a property of the job, not of the test data.**

To get through I changed the *data* (1 repeat in 200 instead of 1 in 400) so
that 5 × 199/200 = 4.975 exactly, and declared `outputsPerInput: 4.975`. A run
with 1 in 400 would have had to declare 4.987, which is not the true 4.9875
either. I would guess run 48's "repeated 0.5% and passed" was the same
workaround, but I have not seen run 48's files, so that is a guess.

**Should say / do:** the constraint should compare against
`outputsPerInput × (records − duplicateRecords) / records`, using the manifest's
`duplicateRecords` that the harness already reads two lines later, and allow
the same 5% that §4 promises — or compare per unique record. Then
`outputsPerInput` stays 5, the job's real property.

## F2. Section 7 gives the panel list but not one metric name (cost: a smoke completeness run and a Prometheus query, about 10 minutes)

§7's table says what each panel shows, but nothing says what the series are
called on `flink:1.20.1` with the Kafka connector. My first draft had one panel
that would have been empty ("orders waiting in Kafka") and one line missing
from another (orders read), both for the first reason below; the chain stops
after completeness on any empty panel. What I had to find out by running the
job and listing Prometheus's series:

- the Kafka source's operator is labelled `Source: orders-source`, not the name given in `fromSource(..., "orders-source")`;
- sink writers are `sink-x: Writer` and `sink-x: Committer`;
- garbage collection has an `All` collector (`flink_taskmanager_Status_JVM_GarbageCollector_All_Time`) — the harness README says so, §7 does not;
- the Kafka source does publish `pendingRecords` (`flink_taskmanager_job_task_operator_pendingRecords`), which is what "records waiting in Kafka" wants.

**Should say:** add a column to §7's table with the PromQL that worked on the
pinned images, e.g. `sum(flink_taskmanager_job_task_operator_pendingRecords{operator_name="Source: <name>"})`,
`flink_jobmanager_job_numRestarts`, `flink_jobmanager_job_numberOfFailedCheckpoints`,
`flink_jobmanager_job_lastCheckpointDuration`, `flink_jobmanager_job_lastCheckpointSize`,
`flink_taskmanager_job_task_busyTimeMsPerSecond` (and the back-pressured / idle twins).
Also say that a user metric registered in an operator appears as
`flink_taskmanager_job_task_operator_<name>`, which is how the "distinct keys"
and "two paths" panels can be fed at all — §7 asks for them and never says how.

## F3. Section 7 cross-references a question that no longer exists

> | | network bytes and buffer use — below saturation | the network between workers becomes a constraint | when workers multiply (§1, question 8) |

§1 has six questions. Question 8 (the axis) was removed. **Should say:** "never
on this harness: it measures one worker growing" — or drop the row.

## F4. The top-level README contradicts SKILL.md three times

`README.md`:

> | `pipeline.json` | describes the three, plus cases, passes, backlog and caps; start from [`harness/pipeline.example.json`](harness/pipeline.example.json) |

SKILL.md: "**Two worked configurations ship with it, and neither is a template.**" and harness/README.md: "**Do not copy an example.**"
The windowed example goes further: `"_project": "COPY THIS FILE AND CHANGE THIS FIRST."`

`README.md` also says the interview asks "who is watching, and — written down
verbatim — the claim you want to make", which are not among SKILL.md's six
questions, and "About an hour later you have the pipeline", "A full run takes
about an hour on a laptop", where SKILL.md §1a says "Clean-room runs have taken
two to three".

**Should say:** "write your own from the field table in harness/README.md";
list the six questions SKILL.md asks; give the two-to-three-hour figure. In the
windowed example, delete "COPY THIS FILE".

## F5. The `caps` field row leaves out the two memory keys the harness requires

harness/README.md field table:

> | `caps` | `kafka`, `jobmanager` CPU caps; `tmMemory` (Flink process size), `tmMemoryLimit`, `kafkaMemory`, `kafkaHeap` |

Preflight stops the run unless `tmMemoryBase` and `tmMemoryPerCore` are set
(the row "memory is per subtask, not per container"), and `tmMemory` on its own
is refused. The keys are explained in prose 200 lines earlier, but the table
that §1 says to write `pipeline.json` from lists only the configuration that
is refused. **Should say:** `tmMemoryBase`, `tmMemoryPerCore` (required),
`tmMemoryLimitPerCore` (optional); `tmMemory` only with `perCase`.

## F6. `job.args` invites `--key=value`, and Flink's own argument parser cannot read it (cost: one submit, 2 minutes)

Both examples write `"args": "--bootstrap={bootstrap} --in={in} ..."`. The
obvious way to read those in a Flink job is `ParameterTool.fromArgs`, which
reads `--key value` and not `--key=value`. The first submit stopped with
"No data for required key 'bootstrap'". **Should say**, in the `job.*` row:
"the template is passed as written; if you use `ParameterTool`, write
`--bootstrap {bootstrap}`, not `--bootstrap={bootstrap}`".

## F7. Code-read: the retention preflight row can print PASS with a FAIL in it

`prove.py cmd_preflight.retention()` *returns* a string instead of raising:

```
if missing:
    return f"FAIL: no retention.bytes on {missing}, which nothing ever drains"
```

`check()` marks anything returned as PASS, so this row would print
`PASS  retention on every topic ...  FAIL: no retention.bytes on [...]`. It did
not fire on this run (my generator creates the price and market-value topics
with `retention.bytes`). **Should do:** `raise Exception(...)`.

## F8. Code-read: the memory budget row understates what the containers may take

Preflight printed:

> memory 3072m at 4 cores + broker 4352m + job manager 1600m = 9024m of 9937m VM

but `lib.start_tm` gives the task manager container a limit of 1.25 × its
process size (3,840m at 4 cores), the job manager's container is `mem_limit:
2g`, and `extraServices` (Grafana, Prometheus, exporter: 768m of limits here)
are not counted. Limits are not usage, so this may be fine, but the row reads
as if 900 MiB were spare. **Should say** both figures: process sizes, and
container limits including `extraServices`.

## F10. The tiny proof's self-test writes "finished: STOPPED" into the live progress file

At 22:10:21, in the middle of the live tiny proof, `results/PROGRESS.txt` read:

> 22:10:21  [####################] 100%  finished: STOPPED at c 0.0 min

That is the self-test's fake chain ("all: the chain stops at the first failing
step -> FIRED | chain stopped at c, d never ran"). `cmd_all` guards its other
progress writes with `if results == c.results`, but not the last one:

```
try:
    L.progress(f"finished: {verdict} {out['seconds']/60:.1f} min", pct=1.0)
```

The skill tells whoever relays a detached run to read exactly this file. A
person told "read PROGRESS.txt" would have seen the run as finished and
stopped, an hour before the suite. `DONE` was written to the fake chain's own
directory, so the watcher was not fooled. **Should do:** guard that line the
same way as the others, and add the case to the self-test.

## F11. The scorecard's "the passes put it anywhere from" is not where the passes put it

The scorecard's sentence for the 4-core case:

> 4 cores: doubling gave 1.86x. The passes put it anywhere from 1.77x to 1.90x, which spans the 1.80x target

The table in the same `suite.txt`:

> STEP 2->4 cores: 1.858x  (ideal 2x, target 1.80x, range across passes 1.697x-1.982x)

Two different intervals, both described as what "the passes" or "range across
passes" give. 1.77–1.90 is the interval the claim is judged on (`low`/`high`
in `unsettledSteps`, from 8 adjacent pairs); 1.697–1.982 is the lowest and
highest pair. A reader checking the verdict against the table finds numbers
that do not match and no word saying why. **Should say:** "the step is judged
on 1.77x to 1.90x, an interval from its 8 pairs of neighbouring passes (single
pairs ran from 1.70x to 1.98x)".

## F12. Not settled, with nobody to ask: the skill says stop, and says nothing about what next

§6: "if it is still open the chain stops saying so, and nothing in the pipeline
is changed on it." The DONE line says "More passes would decide it". The
harness had already run all six extra cases (`settleExtraCases`). For an
unattended agent there is no instruction: run the whole chain again (another
90 minutes, and a different session's numbers are not supposed to be
combined), run `prove.py suite` alone with more passes, or stop. I stopped,
because "nothing in the pipeline is changed on it" was the only rule that
applied. **Should say**, for no human: "stop and report it as not settled; do
not re-run" — or name the command that adds passes to the same table.

## F13. The dashboard check counts a panel's points across all its queries

`lib.panel_points` adds up points from every query in a panel, and
`empty_panels` flags a panel only when the total is zero. A panel with two
queries where one is wrong still passes. My first draft of "Records per second
by stage" had exactly that: the "orders read" line named the operator
`orders-source` (no data) beside the sink lines (data). It would have passed the
harness's check with its most important line missing. Found by querying
Prometheus myself, not by the harness. **Should do:** judge each query (each
`refId`) separately.

## F9. Small things (listed last)

- `PROGRESS.txt` counts the extra settling cases by moving the denominator
  ("case 13 of 13", then "case 15 of 15"), so the bar went from 90% to 92% to
  93% over 25 minutes and "about 4m left" was printed at 23:02 and again at 23:10. Say "extra
  case 3 of up to 6, to settle 2→4".
- `watch.sh` reads progress with `cat "$R/PROGRESS.txt" 2>/dev/null`, in a
  skill whose own rule is "Never send output to /dev/null while you are still
  finding out whether it works". Harmless here, but it is the one place a
  reader is shown the opposite of the rule.
- The example's `_topics` says "Create and fill the price topic yourself (the
  generator is the natural place)". The generator template has no
  placeholder for a second input topic, so its name is typed by hand in the
  generator and job commands. A `{alsoIn0}`-style placeholder, or a
  `topicsAlsoRead` list with the same courtesy `topicsAlsoWritten` gets,
  would remove one typo risk.

## How well section 7 supported building the dashboard

**Well on plumbing, poorly on content.** About 25 minutes in all: 10 to write
the provisioning files and a generator for the dashboard JSON, 10 for a smoke
completeness run, 5 to list Prometheus's series and fix two queries.

What worked first time:
- `extraServices`, and the shipped `docker_cpu_exporter.py` with the README's
  compose body: copied, ran, the CPU and broker-memory panels had data at once.
- The rules about mounting the provisioning *directory*, the data-source uid,
  and anonymous or `admin:admin` access were exact. Preflight's two dashboard
  rows passed and meant it.
- The report wrote the suite's range into the dashboard file and turned
  auto-refresh off: "dashboard : opens on the suite, 22:12–23:18; every panel
  shows data over the suite".

What did not:
- No PromQL and no metric names anywhere (F2). The panel list says *what* to
  show; finding *what it is called* took a smoke run.
- Two of the "always" panels — distinct keys per aggregation, and the two
  paths' totals — cannot be built from Flink's own metrics at all. The job has
  to publish gauges for them. §7 never says so. I registered `distinctKeys`
  and `absQuantityApplied` gauges in the aggregation; they read 4,096 / 16,384
  and 4,991,230,512 on both paths, equal to the manifest's total, on the smoke
  run (read back from Prometheus).
- The stale "question 8" reference (F3).

Were the harness's checks on it right? **Right as far as they go, and they go
only to "is there data".** Preflight would have caught a wrong data-source
name; the post-completeness check would have caught a fully empty panel and
stopped the chain minutes in, not after the suite. It did not catch, and
could not: a panel with one of two queries empty (F13), or a panel with the
wrong number in it — "distinct keys" reading 4,095 would pass. On this run
every check passed, and I confirmed the values by hand, not through the
harness.
