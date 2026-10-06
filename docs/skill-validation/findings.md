# Findings not recorded elsewhere

These were kept only in Claude Code's per-machine working memory between
2026-09-02 and 2026-09-14. They are copied here so the repository, not a
laptop, holds them. Each is recorded as it was measured or observed at the
time; nothing below was re-measured when it was moved.

The rules they led to are in the repository's `CLAUDE.md`.

## 1. The demo's own code scaled linearly — 2026-09-02

One rig, one build, two passes, alternating order:

| units | orders/s |
|---|---:|
| 1 | 33,390 |
| 2 | 74,574 |
| 4 | 157,114 |

**2→4 = 2.11×, 105% efficiency**, spread under 4%. The deck's 1.96× is the same
result measured more conservatively. The 1→4 figure reads 4.71× (118%) because
the one-unit case runs two jobs at parallelism 1 on a single core — a weak
baseline, which is why the step quoted is 2→4.

The 88–97% figure that started the investigation came from the clean-room
pipelines — independently written code — and was wrongly treated as a property
of this one.

Ruled out on that rig, with evidence: host cores (4.6 of 8 used); the broker
(13× headroom); GC (falls per core); CPU throttling (falls per core); JVM-internal
contention (four 1-core workers were 20% *worse*, at 74.5% of cap); an exchange
penalty (fixed parallelism 108% against paired 105% — indistinguishable).

The later, harness-measured result for the same code is in
[`demo-under-harness.md`](demo-under-harness.md): 2→4 = 1.988 at 4,096 keys.

## 2. Four explanations, all wrong — 2026-09-02

For that apparent shortfall, four mechanisms were offered in one session:
back-pressure scoping, what a `keyBy` does at parallelism 1, fixed-cost
amortisation, and an exchange penalty. **All four were wrong.** Two were merged
into the skill before being tested and had to be corrected in later PRs. The
amortisation argument predicted the *opposite* of the effect it was offered to
explain, and survived review because prose hides arithmetic. The fourth died
within twenty minutes of a controlled experiment, which also showed the
shortfall belonged to a different codebase.

## 3. Identical settings, different results — 2026-09-08

The 2→4 step ratio scattered from 1.35× to 2.15× across clean-room runs. The
response was a chain of one-variable experiments, 25–50 minutes of rig time
each: broker CPU cap, broker memory, checkpoint interval, GC and heap,
partition count, network buffer fraction, parallelism against cores. Several
were dead ends — 16 partitions made it worse, network buffers moved the rate
0.6%, the checkpoint interval could not be tested at all.

A table of every component's configuration against the result, which took five
minutes, showed that everything the harness owns — Flink runtime settings,
broker settings, partitions, checkpoint interval, container caps — was
**constant** across runs reading 1.35× and 2.15×, so none of it could be the
cause. What varied was the part not being compared: each agent's own Java.
Diffing two builds found:

| run | 2→4 | producer / consumer settings |
|---|---:|---|
| 20 | 1.930× | lz4 compression, no fetch floor |
| 23 | 1.793× | no compression, `fetch.min.bytes` = 1 MB |

— a direct mechanism for the source idle that had been measured but not
explained.

## 4. Launches reported as running that were not

| when | what happened | cost |
|---|---|---|
| 2026-09-08 07:44 → 09:31 | an experiment finished and nothing was queued behind it | 1 h 47 m idle |
| 2026-09-10 12:03 | a 70-minute rig arm launched with `project: "duhA"`; the harness refuses a project token with a capital letter and it died in under a second. Reported as "running, ETA 12:55"; checked at 13:07 | 1 h idle |

Both times the status line was written from the fact that a command was
*issued*, not that it was *working*.

## 5. Docker Desktop and host disk

Docker Desktop on this machine did not return freed space to macOS when
containers, volumes or build cache were pruned: `Docker.raw` sat at 68 GB
holding 28 GB of live data with the host at 97% full. A full engine restart
trimmed it to 28 GB — but after run 5 a restart recovered only 5 GB of 80.
Trimming inside the VM took the image from 103 GB to 30 GB in seconds:

```
docker run --rm --privileged --pid=host alpine nsenter -t 1 -m -u -n -i -- fstrim -v /var/lib/docker
```

`osascript -e 'quit app "Docker"'` alone leaves the backend half-running and
`open -a Docker` then brings up only the tray, not the VM. The restart that
works:

```
osascript -e 'quit app "Docker Desktop"'; osascript -e 'quit app "Docker"'
pkill -f "Docker.app/Contents/MacOS/com.docker"; pkill -f "Docker Desktop.app"
open -a Docker      # the engine answers about 10 s later
```

**Measured exception, 2026-09-04:** deleting a Kafka *topic* inside a running
container returned its blocks to macOS on its own — the 29.6 GB tiny-proof
topic's deletion took host free space from 92.4 to 121.3 GB and `Docker.raw`
from 54.8 to 25.9 GB within about three minutes, with no trim and no restart.
Which paths discard and which do not is not known; measure `df` on the host
before assuming either way. Run 5 filled the host disk to zero, which killed
the tooling's ability to write its own output.

## 6. Where run 6's time went

Run 6 took 1.94 h. The measurement itself was not the waste:

| segment | minutes |
|---|---:|
| preflight, build, fill, proof, self-test | 43 |
| first scaling suite, **discarded** | 25 |
| diagnose, fix, rebuild | 12 |
| the suite that counted (11.5 windows, 11.1 gaps) | 22.7 |
| dashboard render and report | 13 |

37 minutes went to a suite thrown away because the worker-kill test ran *after*
it. Moving that test into the two-case tiny proof catches a wrong guarantee at
minute 20 instead of minute 90. The same tiny proof caught a 3.73× fake speedup
from `disableOperatorChaining` in about two minutes, with cap use at 99.9% in
both cases — only the implausible ratio gave it away.

## 7. Why the same case reads differently from pass to pass — 2026-09-28/29

Five reference runs of one build on this MacBook Pro (Apple M1 Pro: 6
performance and 2 efficiency cores; Docker Desktop VM with 8 vCPUs) read the
same case up to about 10% apart from pass to pass, while the pipeline's own
figures held flat. Step ratios averaged 1.87× (1→2) and 1.85× (2→4), above the
1.80× target, yet three runs printed "missed". Those three were not
shortfalls: each step's range spanned 1.80× (scalable-flink-skill #112 now
says "not settled"). The question left was where the scatter comes from. Each
experiment below used the reference pipeline's stack, the 2-core case, one
build and one backlog, and changed one thing.

| # | experiment | result |
|---|---|---|
| 1 | the same 2-core case 11 times, every Mac process sampled every 5 s | 1.48–1.58M records/s, 6.8% lowest to highest. The pipeline's own figures were flat (99–100.5% of its cores, garbage collection 0.59–0.77%); so was Docker's VM (197–203% CPU). macOS background services came and went (Spotlight, Photos analysis, up to about 60% of one core); correlation with the rate −0.40 over 11 passes |
| 2 | the same, with about 55% of one core of background-priority load (`taskpolicy -b`) on alternate passes | load on 1.532M, off 1.528M: **+0.2%, 0.1 standard errors**. Background services ruled out |
| 3 | two fresh starts, each measured in eight 60 s windows without restarting | start A 1.592M (0.6% within, after its first window), start B 1.511M (2.3% within): **−5.1% between starts**. Most of the scatter is set when a pass starts |
| 4 | one worker with the job restarted 4 times; then 4 fresh workers pinned to vCPUs 0–1 | same worker: **1.470 → 1.567 → 1.598 → 1.608M**, 0.7% within a start and no mid-run drops. Pinned: 1.594M (1.68M for two minutes, then 1.51M), 1.720M, 1.686M, 1.689M. The arms ran one after the other, so the pinned result was not yet a comparison |
| 5 | fresh workers alternated, unpinned and pinned, 6 each | unpinned 1.544M, pinned 1.626M: **pinning +5.3% (2.2 standard errors)** but not steadier — between starts 4.4% against 3.7%, within a start 3.0% against 3.7%, and runs that fell more than 4% midway: 2 of 6 against 3 of 6 |
| 6 | fresh workers alternated, cold (idle 3 minutes) and pre-warmed (a 3-minute throwaway job first), 6 each | cold 1.494M, pre-warmed 1.528M: **+2.3%, 0.7 standard errors — no clear effect**. Between starts 4.7% against **6.0%**: pre-warming did not steady it |

What this establishes, and does not:

- **The pipeline is not what varies.** Its CPU, memory and garbage collection
  are flat while its rate moves.
- **macOS background services are not the cause** (experiment 2, controlled).
- **Where the worker's threads sit inside the VM is not the cause of the
  scatter.** Pinning them makes the job faster, not steadier (experiment 5,
  alternated). Experiment 4's apparent steadiness came from running the arms
  one after the other.
- **A fresh worker is slower than a warmed one**, but that is not the scatter.
  The same worker got 9% faster over its first three jobs and then held
  (experiment 4); a 3-minute throwaway job before measuring changed the
  average by +2.3% and left starts as far apart as before (experiment 6).
- **Open:** fresh starts of the same job differ by about 5% (standard
  deviation) whatever was changed, and some ran fast for one to two minutes
  and then held 8–10% lower. What is left is how macOS runs the Docker VM on
  its performance and efficiency cores, and at what clock. That needs
  `powermetrics`, which needs an administrator — not measured.
- **A caveat on experiments 3–6.** Their 60 s windows were not aligned to the
  committed-offset boundaries (about every 10 s), so a single window can read
  one commit high or low, about ±16%; one cold start in experiment 6 read
  1.29M then 1.87M. Each start's average is unaffected, because consecutive
  windows cancel, but the within-a-start figures are overstated. The harness
  aligns its windows to those boundaries for this reason (SKILL.md §5).

What to do with it: on this Mac a pass-to-pass spread of about 5% is the
floor. The harness reports a step whose range spans the target as "not
settled" and runs more passes (scalable-flink-skill #112); numbers meant for
publication belong on a quieter machine.

Raw results, with the scripts that produced them:
[pass-scatter/](pass-scatter/). The figures above are copied from those logs.

## 8. What one CFU buys on Confluent Cloud, and what held 20 CFU back — 2026-10-03/04

Nineteen probe runs (some in several attempts) on Confluent Cloud (GCP us-east1, a Basic Kafka cluster,
Flink SQL), each on a stack it created and deleted itself, set out to answer
one question before the harness measures anything there: when a compute pool
goes from 10 to 20 CFU, does the job get twice the workers, the way 2 to 4
cores gives it twice the slots on the laptop? Rates below compare only within
one run unless a row says otherwise; separate stacks read the same per-key
sum at 10 CFU at 18.6 and 16.3 million records a minute (runs 06 and 07).

| # | what was tested | result |
|---|---|---|
| 01–05 | filling a backlog with Confluent's generator, which has no seed | one job used 1 CFU and wrote 12,740–18,370 records/s on three stacks, whatever the pool size; two side by side 27,099 (2.03× one, same stack); eight 74,962 and 74,950; sixteen 187,320 |
| 03 | lowering a pool's size between cases | Confluent answered "Reducing the max_cfu of a compute pool is currently unsupported". Every case now gets a new pool (scalable-flink-skill #123) |
| 04 | where the window can be anchored | Kafka's tools listed no consumer group for a statement's reads in 30 readings out of 30: the laptop's committed-offset anchor does not exist here |
| 04, 05 | drains with no baseline, pools of 5, 10 and 20 CFU; 24 and 48 partitions | each statement started at 1 CFU and grew in about three minutes; in 20 CFU pools it stopped at **10 CFU** three times out of three, at 24 and at 48 partitions, with tens of millions of records still waiting |
| 06 | the same drain, pool 20, once as before and once with `baseline_cfu` 20 (one stack) | Confluent's scaling status said "OK" in 22 readings out of 22, at 10 CFU with 62 million or more waiting. With the baseline the statement reached 20 CFU: **1.54×** by records read (18.60 → 28.73 M/min), **1.62×** by Kafka bytes sent (1,419 → 2,295 MB/min) |
| 04, 05 | Confluent's per-minute "records read" against the backlog | added up to 72.0%, 72.7%, 75.7%, 93.0% and 93.6% of the input topic's log end. **Corrected by run 16:** the log end, not records read, was wrong — see below |
| 07 | per-key sum and pass-through copy at 10 and 20 CFU, baseline = pool size (one stack) | sum **1.66×** (records read), **1.69×** (Kafka bytes); copy **1.23×** and **1.31×**. The copy was never held back (0 ms/s) and busy 1,000 ms/s. The cluster sat at its 10 eCKU limit in all four cases |
| 08 | the copy at 20 CFU with the cluster allowed 50 eCKU | the cluster grew to 50; the copy read 31.56 M/min against 30.59 in run 07. eCKU was not the limit **while watermark alignment was still pausing the input** — see run 18 |
| 09 | the copy at 20 CFU with 96 partitions in and out, 50 eCKU | **43.73 M/min, 1.38×** run 08 (Kafka bytes 3,440 against 2,499 MB/min). Input and output were changed together, so which one mattered is not separated |
| 10, 11 | reading parallelism from the metrics API | `operator/current_parallelism` and `operator/max_parallelism` returned no data in six query shapes, while `operator/num_records_in` returned data; the API offers no operator or subtask label. Confluent's documentation says parallelism cannot be set ("Autopilot manages parallelism") |
| 13 | the Console's Query Profiler, read through Chrome | the copy runs as one chained task. **1 subtask at 1 CFU, 20 subtasks at 20 CFU.** At 20 CFU the subtasks read 1.04 to 3.40 million messages a minute each. 18 of the 24 input partitions were "Blocked" 20–66% of the time: paused by **watermark alignment**, which Confluent Cloud turns on by default |
| 14 | the same copy with `sql.tables.scan.watermark-alignment.max-allowed-drift` = `1 d` (read back from the statement) | no partition blocked. **46.0 and 47.1 M/min** in the first two minutes at 20 CFU, against 30.6–31.6 with alignment on in runs 07 and 08 (other stacks). Then 35, 16, 8 and 5 M/min in the next four minutes, with 47, 23, 12 and 5 million still waiting: the partitions ran dry one by one |
| 15 | the copy at 20 CFU on 40 partitions through the harness's own `start_job`, `wait_at_size` and `check_partitions` (scalable-flink-skill #125, #126) | attempt a: the stack ran its first statement in 5 s and the new readiness check found its table unusable and tore it down before the fill. Attempt b stopped at the job's baseline: `confluent organization list` rejects `--environment` (fixed, #126), after a 40-minute fill. Attempt c ran: 40 partitions accepted, alignment off and baseline 20 read back, the whole pool in use 348 s after the start, held back 0 ms/s. Per minute: 34.6, 46.5, 47.6, **36.8, 23.9**, 59.0, 55.4, 39.3, 24.0 M/min; Kafka's bytes show the same dip. Cause of the dip not found |
| 16 | the same case again on a new stack, traced every 30 s: the output topic's log end (one record per input), the statement's phase and scaling status, its exception list | attempt a: first statement in 13 s, table unusable, torn down. Attempt b: **steady** — 46.8, 44.8, 43.4, 42.8, 40.9, 43.0 M/min (average 43.6), 30-second rates 653,000–791,000 records/s, running, scaling "OK" and no exceptions throughout; no dip. Then the tail as partitions ran dry. **The copy's output ended at 392,070,704 records and records read added up to 392,287,149 (0.06% apart), while the input topic's log end was 423,077,286**: the log end counts 7.3% more records than any reader got. Run 15c shows the same: records read 396,827,976 against a log end of 431,916,232 |
| 17 | the copy at **10 then 20 CFU on one stack and one fill**: 40 partitions, alignment off and baseline = pool size read back, traced every 30 s | 17a stopped at the 20 CFU case: a minute after the new pool was created the metrics API answered 403 "Query must filter by at least one of your authorized resources" (`wait_at_size` now waits through it, scalable-flink-skill #127; 17b met the same answer for about 4 minutes). 17b: both cases at their whole pool from 350 and 341 s, held back 0 ms/s, no exceptions, scaling "OK". 10 CFU **28.55 M/min** (04:53–05:04), 20 CFU **40.14 M/min** (05:17–05:25): **1.41×** by records read, **1.38×** by Kafka bytes (2,355 → 3,240 MB/min). Leaving out the tail as the backlog ran out (10 CFU 04:53–05:01, 32.0; 20 CFU 05:17–05:23, 43.4) gives 1.36×. The 10 CFU rate rose from 29 to 35 M/min over its case; the 20 CFU rate fell from 48.6 to 39.7. The cluster was allowed 10 eCKU; its eCKU count was not recorded |
| 18 | run 17's step again, one variable changed: the cluster allowed **50 eCKU** instead of 10, its eCKU count recorded every minute | 18a: first statement in 8 s, table unusable, torn down by the readiness check. 18b: the cluster sat at 50 eCKU through both cases. On the minutes at full size with at least 100 million records waiting: 10 CFU **29.96 M/min** (09:54–10:03), 20 CFU **52.99 M/min** (10:19–10:23): **1.77×** by records read, **1.75×** by Kafka bytes. Run 17, the same way: 31.87 and 43.97 M/min, **1.38×**. With at least 50 million waiting: 1.71× against 1.34×; by the usual rule, tails included: 1.59× against 1.41×. At 10 CFU the eCKU limit changed little (29.96 against 31.87 M/min); at 20 CFU it took the rate from 44 to 53 M/min |
| 19 | run 18 again, unchanged, on a new stack | the cluster sat at 50 eCKU in both cases. On the minutes with at least 100 million waiting: 10 CFU **25.61 M/min** (00:27–00:37), 20 CFU **51.41 M/min** (00:52–00:56): **2.01×** by records read, **1.99×** by Kafka bytes; with at least 50 million waiting, 1.97× and 1.98×. The 20 CFU rate repeated within 3% of run 18 (51.41 against 52.99); the 10 CFU rate did not (25.61 against 29.96, 15% apart) |

What this establishes, and does not:

- **One CFU ran one subtask** in the one statement read this way (run 13), so
  a CFU step can be a worker step like the laptop's. Nothing but the Console
  shows it; the harness cannot read it back on every case yet.
- **Confluent's autoscaler decides how much of a pool to use.** Left alone it
  stopped at 10 CFU of 20 and reported "OK". `baseline_cfu` set to the pool's
  size made the statement use all of it (run 06). Without it, a case measures
  the autoscaler, not the pool.
- **Watermark alignment is on by default and paused most of the input.** The
  laptop's jobs have no alignment, so the two platforms were not measuring the
  same job. Raising the allowed drift removed every pause and the copy read
  about half as much again (run 14) — but over two minutes, compared across
  stacks; a full case has not been measured.
- **The 24-partition topic broke the laptop's own rule** that the partition
  count divides by every parallelism under test (SKILL.md §6). 24 over 20
  subtasks cannot be even; the subtasks' readings (1.04–3.40 M/min) and run
  14's tail are consistent with that, but which subtask read which partitions
  was not recorded. 96 partitions read 1.38× faster (run 09).
- **Ruled out:** the input partition count while the autoscaler capped the
  job (run 05). Run 08's "eCKU is not the limit" held only while alignment
  was pausing the input; with alignment off it was the limit (run 18).
- **The input topic's log end is not the number of records a reader can
  get.** On the two runs with both counts it was 7–8% higher than what the
  copy wrote and what Confluent's "records read" added up to, while those two
  agreed to 0.06% (run 16). So records read is a sound count, and the
  earlier 72–94% (runs 04, 05) measured the log end's surplus, not a gap in
  records read. Why the log end is higher is not measured. The fill's jobs
  write in transactions and are deleted while running, so records of
  transactions that never committed would stay in the log and be skipped by
  readers — a hypothesis that fits, not a measurement. A backlog has to be
  counted as what a reader gets, not as the log end.
- **The fourth limit was our own cost cap on the Kafka cluster.** With the
  three fixes and the cluster capped at 10 eCKU the copy's 10→20 step read
  about 1.4× (run 17); capped at 50, where the cluster sat for both cases,
  1.77× by records read and 1.75× by Kafka bytes (run 18), against the
  skill's 1.80× target, and 2.01× when run again unchanged (run 19): two
  readings of the same step, 1.77× and 2.01×, averaging about 1.89×. They
  straddle the target, so by the skill's own rule the step is not settled
  yet; the difference between them comes from the 10 CFU side (29.96 against
  25.61 M/min), while the 20 CFU side repeated within 3%. Each run is one
  stack; run 17 against runs 18 and 19 is compared across stacks, and that
  difference (1.38× against 1.77–2.01×) is far larger than the 15% stacks
  have differed by. Each 20 CFU figure rests on five minutes. The
  harness now records the cluster's eCKU in every cloud case and makes a
  case a ceiling when the cluster sat at its limit throughout
  (scalable-flink-skill #131). Kafka capacity was the largest line on the
  bill, so the limit is a cost choice per study.
- **Run 15's dip did not come back** in runs 16 and 17, so whether it recurs
  is not known.
- **Readiness.** Four stacks ran their first statement 5–13 seconds after
  creation, and in all four a table created afterwards stayed invisible
  through five tries over two and a half minutes (runs 10, 12, 15a, 16a).
  In the thirteen other stacks with a readiness reading, the first statement
  took 102–108 seconds and every table appeared. A pattern over seventeen
  stacks, not a measured cause. Since scalable-flink-skill #125 the harness's
  readiness check writes a row to a new table and reads it back from Kafka;
  it caught runs 15a and 16a and tore those stacks down before any fill.
- **Run 13 was lost by hand.** The statement it was watching was deleted to
  start run 14's test on the same stack; the probe's next status read stopped
  it and the stack was torn down. Run 14 repeated that test on its own stack.

Raw results, scripts and the Query Profiler readings:
[confluent-cloud/](confluent-cloud/). The figures above are recomputed from
those files with `analyse.py`.
