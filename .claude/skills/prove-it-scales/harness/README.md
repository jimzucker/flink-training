# The harness

This directory is the skill's guards as code. **Use it verbatim.** An agent
following the skill supplies a pipeline and a `pipeline.json`; it does not
write a sampler, a suite runner, a spread rule, a warm-up rule or a report
table. Ten clean-room runs each rewrote those from prose, and every one
re-decided something the rule had already decided — what a refusal does, what
the window is anchored on, what counts as flat — and paid for it in hours.

```
cp ~/.claude/skills/prove-it-scales/harness/pipeline.example.json pipeline.json   # edit
H=~/.claude/skills/prove-it-scales/harness/prove.py
nohup python3 $H all > results/all.log 2>&1 &        # the whole chain below, one stack session;
                                                     # wait on results/DONE, read results/phases.log
```

`all` is `up → preflight → completeness → tinyproof → fill → suite → report`,
stopping at the first step that does not pass; `results/DONE` holds the
verdict and the wall time, `results/phases.log` the timestamps the harness
wrote (run 11 wrote its own by hand and spent 20 minutes between commands).
Wait with `until [ -f results/DONE ]; do sleep 30; done` and nothing more: a
shell whose command line names `prove.py` from inside the project is a
watcher by the reaper's rule and is killed with the rest (it took the
author's own `pgrep -f 'prove.py all'` loop).
**`--quick` is a smoke run, not a result.** `prove.py all --quick` runs two
passes per case instead of the configured number (the sentinel still follows,
so the baseline is measured three times). It measured *one* pass until
2026-09-06, when a one-pass ratio was found to wander: run 18's build read
1.539x from a single pass against 1.678x from three, because a ratio compounds
the error of both cases. Two passes reproduce the three-pass answer to 0.2% and stamps `quickLook`/`publishable:false`
on the table it writes, with a banner in `suite.md` and `suite.txt`. Every
per-case guard stays live, so it answers "does this rig run clean, and
roughly how fast" — about 48 min here against 60, because the gates
(completeness, tiny proof, fill) do not shrink. It answers nothing about the
ratio: replayed against the record, single passes of the recorded suites read
2.039-2.273x where the suite reported 2.154x, and 1.837-1.859x where it
reported 1.850x — a band wider than the accept line. Never quote a quick
table, and never put one in `record/`.

The flag reaches exactly one place — the number of passes for this run — and
is carried on the table it produced (`quickLook` in `suite.json`), never on
the record or the guards. The first version of it was a process-wide global
that reached `build_table`, and on 2026-09-05 that stopped a run twice, both
times correctly: `replay` re-derived the record with `minPasses` bypassed and
found three recorded-invalid suites it would now report, and the live
self-test's "a case measured only once" guard stopped firing. A guard that
does not fire, or a record that changes under a flag, is a broken harness.

**Give the broker enough memory to hold the working set.** The harness now
refuses any case where the broker hit its container memory limit inside the
window. Measured 2026-09-05, one rig, one build, one backlog, one variable:
at a 2 GiB limit the broker hit it 310,423 times with 6.3M file-page refaults
and 649 MB of cache, and all three 4-core passes were refused at 93.1-93.8%
of cap; at 4 GiB the same case held 99.6-100.1% of cap at 651,653 rec/s and
the cache grew to 2.14 GB. Back-to-back single cases minutes apart: 2 GiB
refused with 30,927 hits at 562,907 rec/s and **96.4% of cap** — above the
cap floor, so nothing else would have caught it — and 4 GiB clean with zero
hits at 646,423 rec/s. A 264M-record backlog wanted 4 GiB here.

**Worker memory is a fixed base plus a per-subtask share.**
`caps.tmMemoryBase` covers what does not scale with cores — metaspace, JVM
overhead, the network buffer floor — and `caps.tmMemoryPerCore` (with optional
`tmMemoryLimitPerCore`) is multiplied by the case's core count, so every case
gives each subtask the same memory; a flat `tmMemory` is refused when there is
more than one case. Measured 2026-09-07 on one build, cap == parallelism,
cases interleaved: flat 2048m gave 2c 558,059 and 4c 917,807 rec/s — 2→4 =
1.645, GC 9.3% at four cores — and per-core memory gave 2c 549,380 (unchanged)
and 4c 1,049,130 — 2→4 = 1.910, GC 2.3%. The fourth core was starved of heap,
not short of CPU, and every case before this change shared that flaw.

Scaling the *whole* figure by cores then starves the other end: at 1280m per
core with no base, the rig read GC 17.4% at one core against 3.4% at two and
1.1% at four, because Flink's fixed overheads are most of a small process
size. Hence the base term.

**The tiny proof sizes the backlog.** It measures the largest case's rate and
refuses the chain if `backlog.count` is short of what that case needs to
survive warm-up, the window and more than one checkpoint interval of headroom
(x1.5). Every clean-room run from 15 to 20 lost an attempt to a backlog sized
by guess before anything ran — run 18 sized for 500k rec/s against an actual
930k, run 20 drained 50M records mid-window. Preflight states the ceiling the
current guess covers, and the refusal names the number to use.

**Broker memory is named, not guessed.** When the broker hits its cgroup
limit inside a window the refusal now carries the figure to use — the step that
worked on this host was x1.6 (3,840 MiB gave 995 hits, 6,144 gave none) — and
preflight refuses a configuration where the worker at its largest case plus the
broker plus the job manager do not leave the VM a spare gigabyte. Runs 20 and
21 lost five tiny proofs between them discovering both by trial.

**The claim is gated separately from the measurement.** A table can be beyond
reproach and still say the pipeline does not scale. `report` marks each step
`meetsClaim` against `scalingFloor` (95% of linear) and exits non-zero when a
step misses, so `all` ends FAIL rather than PASS. The floor comes from this
repository's own demo, which reads 1.99x from 2 to 4 cores on the same laptop,
less the +-3% a two-pass ratio carries. Replayed against the record before it
shipped: 1->2 meets it in 11 of 12 recorded runs, 2->4 in three.

When a step misses, the report prints both cases side by side — per-core rate,
cap, source idle, GC, back-pressure — and what this rig has already shown costs
what: worker memory that does not scale per subtask, about 14%; a broker
starved of page cache, about 13%; four subtasks instead of two on the same
cores, about 8%, of which roughly 3 points is the source idling. Partition
count (8 against 16) and network buffer fraction (0.15 against 0.30) were each
tested with the cases interleaved and changed nothing: 16 partitions refused
every parallelism-4 case for an unstable warm-up, and the buffers moved the
per-core rate 0.6%.

Type the steps yourself only when one of them needs re-running:

```
python3 $H replay          # thresholds vs the recorded runs — seconds, no stack
python3 $H up              # stack/compose.yml generated, broker + job manager up, sampler compiled
python3 $H preflight       # §3, one PASS/FAIL row per check
python3 $H tinyproof       # two cases on a small backlog, ratio bounded, every guard broken on purpose
nohup python3 $H fill > results/fill.log 2>&1 &        # the full backlog; build the dashboard meanwhile
python3 $H completeness    # drain small backlog twice (clean, worker killed), verify, no tolerances
python3 $H suite           # the table
python3 $H ceiling         # optional: starve the broker in steps at the largest case
python3 $H down            # everything this project started, gone; asserted; fstrim
```

Every command writes to `results/` next to `pipeline.json` and appends to
`results/harness.log`. `suite` refuses to start unless `tinyproof` (with its
self-test) and `completeness` have passed **for the same build hash**.

## What the pipeline supplies

| field | what |
|---|---|
| `project` | short lowercase token; every container, volume and network is prefixed with it, and `down` asserts nothing with the prefix survives — nor any host process holding a file under `results/` open, naming the project directory on its command line, or naming `prove.py` while running from inside the project (those are killed and listed; a survivor is a refusal — another project's harness, or a shell merely sitting in the directory, is left alone) |
| `topics.in`, `topics.out[]` | the input topic the job consumes and every topic it writes. The harness sets retention on the outputs and recreates them per case |
| `outputsPerInput` | records written to all outputs per input record. The two-vantage guard divides sink growth by this and compares to committed source offsets |
| `job.jar`, `job.mainClass`, `job.args` | the job. `args` is a template: `{bootstrap}` `{in}` `{out0}` `{out1}`… `{group}` `{par}` `{ckptMs}`. The job **must** consume `{in}` with consumer group `{group}`, commit offsets on checkpoint, and run at parallelism `{par}` |
| `job.sourceVertexMatch` | substring of the source vertex name in the running plan (busy/idle/back-pressure are read for it) |
| `generator.cmd` | fills `{topic}` with `{count}` records from `{seed}` and writes `{manifest}` (JSON) — deterministic, bootstrap `{bootstrapExt}` |
| `generator.manifestCmd` | same without producing (the determinism preflight runs it twice) |
| `generator.manifestCountField` | the manifest field holding the record count |
| `verifier.cmd` | reads the outputs and `{manifest}`; exits 0 iff every completeness assertion holds with no tolerance |
| `cases`, `baseline`, `passes` | the cases, which one is the baseline, passes per case (≥2; odd numbers alternate asc/desc/asc). The suite then measures the baseline once more as a **sentinel** — the first and last measurements of the suite are the same case, so a rig that drifts across the suite shows up as baseline spread rather than hiding inside the alternation. No threshold of its own: the 20% ceiling counts it. `suite.md` reports the first→last drift |
| `backlog.count`, `.seed`, `.smallCount`, `.tinyCount`, `.killAtFraction` | the drain backlog; the completeness backlog (must drain to the last record); the tiny-proof backlog; where the worker is killed. **Size the backlogs for the largest case's rate × (warm-up ceiling + window + two checkpoint intervals)**: the suite at up to 240 + 70 + 20 s, the tiny proof at 120 + 40 + 20 s. The completeness backlog must span **several checkpoint intervals** at the baseline rate, or the kill cannot land where `killAtFraction` says (offsets commit once per interval). A backlog that drains under the job is a refusal, and the refusal says so |
| `caps` | `kafka`, `jobmanager` CPU caps; `tmMemory` (Flink process size), `tmMemoryLimit`, `kafkaMemory`, `kafkaHeap` |
| `images.flink`, `images.kafka` | pinned tags; preflight checks they are native to the host |
| `jdk` | the host JDK home; preflight checks its major version matches the engine image |
| `axis`, `apiLevel`, `guarantee.state`, `guarantee.sink`, `checkpointMs` | the header fields of §9, verbatim into the report |

## What the harness owns, and the agent does not change

The thresholds in `lib.py` (`T`), each with the measurement it was set from.
Config guards are replayed too: `record/configs.json` holds configurations
whose verdict is already known — run 20's and run 21's, the rig's, plus two
that must be refused (flat worker memory, six partitions with a four-core
case) — and `replay` builds each one and checks it still gets that verdict. It
exists because #68 shipped a rule that refused two configurations which had
already produced accepted runs, and run 22 spent two chains and produced no
ratios finding out. With this in place that rule fails replay in a second.

`prove.py replay` re-derives every recorded suite in `record/` with the current
thresholds before any command that touches a stack, and refuses to run if a
threshold would void a table the record marks valid or report one it marks
invalid. **To change a threshold: change it, run `replay`, and if it fails, the
threshold is wrong — not the record.** A new suite worth remembering goes into
`record/` as per-pass rates per case plus a `validSteps` verdict.

What the harness measures and how:

- rate = committed source offsets over a window opened on the tick the offset
  advances and closed after ≥ `minBoundaries` further commit boundaries and ≥
  `minWindowS`; never the engine's own meter
- CPU = cgroup `cpu.stat usage_usec` at open and close; throttled periods from
  the same file
- busy / idle / back-pressured per vertex from the worker's slf4j reporter,
  averaged over samples inside the window; internal back-pressure is a column,
  source idle is the external-boundary guard
- the cap is read back from `NanoCpus` and `cpu.max`; slots from the engine;
  vertex parallelism and graph shape from the running plan
- every case tears the job, sampler and worker down on every exit path
- a refusal about the **rig** stops the suite; a refusal about a **case's data**
  marks the case, voids the ratios it is in, and moves on

## Results files

| file | written by |
|---|---|
| `preflight.json` | preflight |
| `tinyproof.json`, `selftest.json` | tinyproof |
| `manifest.json`, `manifest-small.json`, `manifest-tiny.json` | fill, completeness, tinyproof |
| `completeness.json` | completeness |
| `suite.json`, `suite.txt`, `suite.md` | suite (and `report`, which regenerates the last two) |
| `ceiling.json` | ceiling |
| `all.json`, `phases.log`, `DONE` | all (per-step rc and seconds; timestamps; the verdict to wait on) |
| `harness.log` | everything |

`suite.md` is the table for the report: header fields, step ratios with their
range across passes, per-pass rows, per-case means with spread and
reportability. Paste it; do not retype a number.
