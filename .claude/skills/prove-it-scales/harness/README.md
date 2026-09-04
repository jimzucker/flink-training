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
