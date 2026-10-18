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

Twenty-one probe runs (some in several attempts) on Confluent Cloud (GCP us-east1, a Basic Kafka cluster,
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
| 20 | one 20 CFU case through **the harness's own** `run_case_cloud` (`submit`, `measure_window`, `check_case_cloud`), 40 partitions, 50 eCKU; then a bounded `COUNT(*)` read over the Flink REST results API | 20a stopped at readiness: Confluent could not register the readiness table's schema ("failed registering schemas ... connection reset by peer"); readiness now waits through that. 20b: the rate from the output's log end **857,092 records/s** (51.4 M/min, as run 19 at 20 CFU), Confluent's records read 840,631, **1.9% apart**; CFU in use 20.0 of 20; busy 100%, held back 0%, idle 0.3%; 196.8 million records left at the window's end. The eCKU check of scalable-flink-skill #131 made this case a ceiling because the cluster sat at its 50 eCKU limit — wrongly: runs 18 and 19 had it there in both cases, so the check would have thrown out their tables. It now reports eCKU only (#134). The bounded `COUNT(*)`, polled for its status but not read while it ran, ended STOPPED and its results answered 409 "not ready" |
| 21 | six stacks one after another, the reused environment name `flink-training` against a fresh name each time, `up` and `down` only; on the first usable stack, a bounded GROUP BY over a three-row table read through `statement_rows`, which now reads results while the statement runs | reused name: 110, 102, 102 s to the first statement, all usable. Fresh names: **8 s and 9 s, both unusable**, then 102 s, usable. The bounded GROUP BY returned exactly the three rows' totals (values come back as strings) |
| 22 | run 18 again, unchanged (run 19's probe with only its import path changed), on a new stack | first statement 102 s, usable on the first try. The cluster sat at 50 eCKU in both cases. On the minutes with at least 100 million waiting: 10 CFU **28.51 M/min** (04:07–04:15), 20 CFU **50.87 M/min** (04:31–04:35): **1.78×** by records read, **1.78×** by Kafka bytes. `step-100m.py` computes runs 18, 19 and 22 the same way and gives runs 18's and 19's published figures exactly |
| 23 | why the first full cloud chain (the SQL app in `cloud-sql-app/`) stopped at completeness: its manifest count, a plain bounded `GROUP BY account` over the ~65M-record small backlog, had no complete answer after 30 minutes. Three probes, each on its own stack: (a) the same count, logged every minute for 40 minutes; (b) how far through the input that count gets, and a count in a tumbling window; (c) three count forms on a three-row table with a known answer, then the form that gave it on the backlog | (a) still running after 40 minutes, 17,488,616 distinct updated rows read over 714 pages. (b) **two change-log rows per input record** (5,896,266 for 2,948,135 records counted in 6 minutes), about 500,000 records a minute: about 140 minutes for 70M. The window did not run: a hint inside `TUMBLE` does not parse, and `ALTER TABLE` with the window after it ended in "Internal error". (c) the plain count right in 27 s (4 rows); as a **snapshot query** (`sql.snapshot.mode` = `now`) right in 16 s (2 rows); a view feeding the window failed validation at `$rowtime`. On the backlog the snapshot query gave its 4 rows in **72 s**, totalling 33,594,701 against a log end of 54,097,172, **38% below**; earlier runs measured the log end 7–8% above what readers get. Not explained. In all three fills the log end grew by about 4M records in the minute after the fill jobs were deleted. Counts now run as snapshot queries (scalable-flink-skill #139) |
| 24 | why the third cloud chain's completeness did not match: its input counted 44,992,903 by the snapshot query and its output 52,206,970, each output row read three times. One stack: the same fill, the input counted four times over 35 minutes; Kafka's own consumer (`kafka-consumer-perf-test.sh`) counting the input committed-only and everything; the completeness drain repeated through the harness's `set_size` and `submit` at 5 CFU; the output counted the same three ways | the input: **37,996,160** by the snapshot query at +74 s, +344 s, +2044 s and +2128 s; **37,996,160** by Kafka's committed-only consumer; 66,089,568 reading everything; log end 66,092,231. The output after the drain: **37,996,160** by the snapshot query, by Kafka's committed-only consumer and by Kafka reading everything; Confluent's records read 38,005,960 (0.03% over). Exact, per account. The last results page came while the statement still said RUNNING (counts 2 and 3), which is how the chain read each row three times (fixed, scalable-flink-skill #140). Why that chain's output held 16% more than its input is **not explained**: the same path on this stack was exact |
| 25 | the SQL app (`cloud-sql-app/`) through the harness's whole chain, `prove.py all`, fourth attempt: local first, up, preflight, completeness, tiny proof, an 800M fill, a suite of 5, 10 and 20 CFU in two passes and a sentinel | completeness **PASS**: 45,001,150 records in, the same out, exact per account. Tiny proof **PASS**: 5 CFU 205,407 records a second, 10 CFU 478,021, 5→10 2.33×. Suite: 10 CFU 480,640 and 496,044 records a second, 20 CFU 830,015 and 859,839: **10→20 1.73× in both passes, missed** against 1.80×. 5→10 1.789×, undecided (the stored `suite.json` says met: the bug scalable-flink-skill #142 fixed). The first 5 CFU case was thrown out twice, its two counts 12.8% and 8.3% apart against a 5% limit; why is not known. The report then crashed on a laptop-only reading and the stack stayed up for nine hours (fixed in #141). Results in `cloud-sql-app/results-cloud/` |

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
  records read. Why the log end is higher was measured in run 24: Kafka's own
  consumer read 37,996,160 records as a committed-only reader and 66,089,568
  reading everything, against a log end of 66,092,231. The surplus is records
  of transactions that never committed — the fill's jobs are deleted while
  running, and their open transactions are aborted. On a short fill that was
  42% of the log. A backlog has to be counted as what a reader gets, not as
  the log end.
- **The fourth limit was our own cost cap on the Kafka cluster.** With the
  three fixes and the cluster capped at 10 eCKU the copy's 10→20 step read
  about 1.4× (run 17); capped at 50, where the cluster sat for both cases,
  1.77× by records read and 1.75× by Kafka bytes (run 18), against the
  skill's 1.80× target, 2.01× when run again unchanged (run 19), and 1.78×
  a third time (run 22): three readings of the same step averaging 1.85×,
  two of them just under the target (by 1.7% and 0.9%) and one above it.
  The SQL app — a different pipeline, a projection with one computed
  column — read its own 10→20 step at 1.73× in both passes of the
  harness's full chain (run 25), short of 1.80×; that is the only 10→20
  step on Confluent Cloud the harness itself has judged. The copy's three
  readings come from probes the harness did not judge, and the two
  pipelines' ratios are not one series. The cloud work stopped there on
  2026-10-07: the $350 budget was spent.
  The difference between them comes from the 10 CFU side (25.61–29.96
  M/min, 15% apart), while the 20 CFU side repeated within 4% (50.87–52.99
  M/min). Each run is one stack; run 17 against runs 18, 19 and 22 is
  compared across stacks, and that difference (1.38× against 1.77–2.01×) is
  far larger than the 15% stacks have differed by. Each 20 CFU figure rests
  on five minutes. The harness records the cluster's eCKU in every cloud
  case and reports it; it does not decide a case (scalable-flink-skill
  #134, after #131 wrongly made such cases ceilings). Kafka capacity was the
  largest line on the bill, so the limit is a cost choice per study.
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
- **Readiness: the first statement's time, not the environment's name, goes
  with an unusable stack.** Every stack whose first statement ran in 5–13
  seconds — seven of them, under both the reused and fresh names — had
  tables no later statement could see; every one that took about 102–110
  seconds worked (run 21 and the runs before it). Why is not known. The
  harness's readiness check writes a row and reads it back, catches each such
  stack before any fill and tears it down; it costs about three minutes.
- **The harness's cloud case works on the real service** (run 20): the
  transport rate and Confluent's records read agreed to 1.9%, the pool was
  fully used, and the record had the laptop's shape. A **bounded statement's
  rows can be read over the REST API only while it runs** (runs 20 and 21);
  the harness now reads them that way and applies the change log.
- **Run 13 was lost by hand.** The statement it was watching was deleted to
  start run 14's test on the same stack; the probe's next status read stopped
  it and the stack was torn down. Run 14 repeated that test on its own stack.

Raw results, scripts and the Query Profiler readings:
[confluent-cloud/](confluent-cloud/). The figures above are recomputed from
those files with `analyse.py`.

## 9. A busy laptop, an out-of-date skill, and what a run leaves behind — 2026-10-10/11

One test project (`skill-test`, the default business case in DataStream, build
`078dc8aaf0caa40c`), one reference run, and six skill changes
(scalable-flink-skill #148–#153). Rates compare only within one run.

| # | what happened | what it showed |
|---|---|---|
| 1 | Preflight said "quiet" (other programs on 0.50 of 8 cores). Later that night Photos analysis and Spotlight were busy while the short proof ran | Its 2→4 was out of bounds twice (1.49×, then 1.482×) and stopped the chain. The next morning, with the Mac quiet, the same build read 4 cores at 609,550 and 615,001/s and 2 cores at 297,733 and 304,937/s: 2→4 of 2.00–2.07×. **What made the night's readings slow is not measured.** Both the machine and the backlog topic changed between night and morning |
| 2 | The short proof measured 1, 2 and 4 cores, then all three again, to stop on 2→4 | The two 1-core cases (about 7 minutes) could not change a 2→4 verdict. Now it runs the largest case first and stops on the step that fails (#148). Across 27 recorded suites, the 4-core case read a median 1.1% higher when it ran first in its pass: inside the ±4% noise, but not the same as running first after the fill |
| 3 | The project ended with no README, `results/` ignored in git, and no diagram, because the suite never ran | The numbers survived only because the agent copied them into its journal. The harness now checks the README at preflight (#150), writes `results/.gitignore` itself (#151), and draws the job during completeness (#152). Only 2 of 39 recorded projects had a README, and both were missing names the new check looks for (fixed in #131) |
| 4 | What `results/` keeps | Goes by file name, not size: a suite file reached 1.26 MB (run 52), larger than any manifest (0.84 MB). On the 2026-10-10 run the rule keeps 13 of 30 files, 1.07 MB |
| 5 | The diagram drew 5 boxes for a job of 4 tasks and 13 operators, and showed as a code block in viewers without Mermaid | #153 draws every operator, one box per task, and renders a PNG. Mermaid's default layout ran run 51's lines behind boxes they do not connect to; ELK did not. At the default width, run 51's picture was 1568 × 230 pixels and unreadable; at `-w 1600` it is 3168 × 462 |
| 6 | **The skill Claude Code loads was 89 commits behind GitHub** (`~/.claude/skills/scalable-flink-skill` at #61, its files closest to #115) | Every skill change since then had reached the repository but not the agents. Brought up to date on 2026-10-11; the old files are kept in `git stash` and in a tar backup |
| 7 | The reference run on the updated skill ([`reference-pipeline/run-2026-10-11/`](reference-pipeline/run-2026-10-11/)) | Every new check worked: README PASS, the short proof ran 4, 2, 1 and passed (1→2 1.979×, 2→4 1.735×), cases waited 34 s for a busy Mac, `graph.png`, `records.md` and `headline.md` were written, and 20 kept files were committed. The suite **stopped at the report**: the 2-core readings were 23% apart (1,441,040, 1,149,816 and 1,192,266/s), so neither step was reported. 8 of 10 cases kept, against run 44's 10 of 10 |
| 8 | What the reference run's own records say about the Mac | The slow 2-core passes ran with 7.1–9.2 GB swapped out, the fast one with 3.5–5.8 GB. Other programs used up to **4.46 of 8 cores at window open**, after the case had waited to start: the wait checks only before a case starts. The 1-core sentinel read 41% of its cap; across the 1-core cases Kafka's memory filled 235 times. **Whether the harness changes played any part is not measured** |
| 9 | macOS upgraded itself to 27.0.1 and restarted after the reference run | Load average 23 / 50 / 45 seventeen minutes after the restart, from re-indexing. Runs before and after the upgrade are not on the same machine and should not be compared |
| 10 | `prove.py selftest-pure` took 98 s instead of 8 s | The same on main before any of today's changes (358d582), at 1% CPU, in the dashboard self-tests. **Cause not measured** |
| 11 | `scripts/build-runs-ledger.py` stopped with an error | Broken since cloud-sql-app's Confluent Cloud short proof (#127), which keeps its case records under `runs`. Unnoticed because the ledger was last rebuilt on 2026-10-01. Fixed; it now has 650 rows |

## 10. Clean-room run 53 and the fixes it led to — 2026-10-11/12

The first clean-room run on the skill after #148–#153
([clean-room-run-53.md](clean-room/clean-room-run-53.md)), the two skill
changes made from it (scalable-flink-skill #154, #155), and the reference run
that tested them. Rates compare only within one run.

| # | what happened | what it showed |
|---|---|---|
| 1 | Run 53 met 1→2 at 1.88× (within the 1% margin) and stopped with 2→4 not met at 1.80×, the low end of its range 1.725× | The harness's verdict and its advice ("more passes could change that; change nothing in the pipeline") were followed. Kafka was ruled out (a squeeze from 2.5 to 0.5 cores moved the 4-core rate 3%); this Mac's memory-heavy 2→4 reads 1.65×, so the machine was not |
| 2 | Run 53's 4-core passes rose late in the suite on an unchanged build: 410,837–429,480/s in the first four, then 467,289 and 492,953/s | The 2-core passes show no trend, and other programs were under 2 cores at every window. **Cause not measured** |
| 3 | The README check counted the operator `orders-source` as naming the topic `orders`, and its own self-test missed | It was a text search. #154 matches whole names; replayed on the three READMEs on record, the result is unchanged |
| 4 | A deliberate "STOPPED:" from the self-test in `harness.log` ended the agent's wait loop early | #154 sends the self-test's messages to `selftest.log`, each marked "self-test:" |
| 5 | Section 6 told an unattended agent to measure, to tune, and to stop, for the same short step | #154 replaces the three with one table keyed on the scorecard's verdict; the doc check holds it |
| 6 | Flink's metrics reporter writes `position-mv-by-symbol` as `position_mv_by_symbol`, so a panel written with dashes is empty and the chain stops | #155: section 7 says so, and the empty-panel message gives the spelling to use |
| 7 | Compose values were written without quotes, so `"true"` became a true/false value (read from the code, not hit) | #155 quotes text values. Every `pipeline.json` on disk (40) rendered with the old code and the new: the 22 that render read identically in `docker compose config`, 11 with extra services; the other 18 are turned away by today's guards either way |
| 8 | Completeness could stop a run for a busy Mac though it measures no speed; the probe's file was not kept in git; the examples' verifiers left out `{arm}` | All three fixed in #155, with self-tests or doc checks |
| 9 | Starting the 2026-10-12 reference run, the harness's sweep for leftover chains killed the shell watching it, because that shell's command line named `prove.py all` | The run was unaffected. The same trap as run 53's own wait loop: watch `results/DONE` or a process id, never a command line that names `prove.py` |
| 10 | The reference run on #154 and #155 ([`reference-pipeline/run-2026-10-12/`](reference-pipeline/run-2026-10-12/)) | Passed in 83.1 min: 1→2 1.980×, 2→4 1.819× (low end 1.796×, within the 1% margin), 10 of 10 cases plus 5 settling passes. README PASS; `harness.log` carried no self-test stop; both completeness arms recorded the Mac's load (0.22 and 0.45 cores) |

## 11. Clean-room run 54 and the wording it led to — 2026-10-12/13

The first clean-room run on the skill after #154 and #155
([clean-room-run-54.md](clean-room/clean-room-run-54.md)), and the skill
change made from it (scalable-flink-skill #156). Rates compare only within one
run.

| # | what happened | what it showed |
|---|---|---|
| 1 | Run 54 met 1→2 at 2.00× (low end 1.919×) and stopped with no 2→4: the 4-core readings, 333,487, 406,141 and 334,852/s, were 20.3% apart against a 20% limit | The spread guard did its job: a case that cannot say which of two rates is right gives no step. The two 4-core readings that agree give about 1.54× against the 2-core mean, which is not a reported step |
| 2 | The second pass read high at every core count: +21% at 4 cores, about +14% at 1 core, +4% at 2 cores | The three high passes are the three descending ones, back to back from 22:58 to 23:08. In them the source spent less of its time held back by the stages after it (4 cores 16% against 25–30%, 1 core 36% against 42–46%, 2 cores 29% against 30–31%) on the same cores, so the job did more work per core, not more cores. Other programs' load, swap, garbage collection and the broker's memory do not separate them. **Cause not measured**: the run's directory and stack are gone, so it can only be measured by rebuilding the job from `run-54/job/` |
| 3 | The stop line said "its readings are 20% apart and the limit is 20%" about 20.3% | Both numbers were rounded to whole percent, so the reason read as no reason. #156 prints one decimal and the direction: "20.3% apart, over the 20% limit" |
| 4 | Messages said "the claim" — "a step the claim needs was not measured", "CLAIM NOT MET" | The author, 2026-10-13: "stop say 'claim' that not english, goal or target". #156 says target or goal throughout, and the doc check fails on the word; JSON keys stay, so recorded runs replay |
| 5 | `harness.log` said "the chain measured a valid table whose step ratios do not meet the claim" for a table that had thrown out its 4-core case | One sentence was logged for every stop. #156 logs what actually stopped the report |
| 6 | `headline.md` printed 1→2's range as 1.786–2.179× beside "the low end of its range, 1.919x" | Two different ranges under one name: the lowest and highest pass, and the 95% range from same-time pairs the verdict uses. #156 prints the one the verdict uses |
| 7 | Preflight printed PASS for "nothing else is using the cores" beside "busy: 2.20 of 8 cores" | The row is reported, never a stop; its name stated the hoped-for answer. #156 names it "other programs' use of the cores" |
| 8 | The harness README said preflight stops a run that does not leave the VM a spare gigabyte | That rule was removed on 2026-09-07; the code only reports the sum. #156 corrects the README and writes the sum down (the example: 4,560 + 6,144 + 2,048 = 12,752 MiB on a 12,955 MiB VM) |
| 9 | The agent's verifier crashed on a Jackson version clash, and chain 1 stopped at completeness after 6.5 minutes | A defect in the agent's own build, caught where it should be. Fixed by pinning the version the Kafka connector brings |
| 10 | The agent handed back at 22:13, 71 minutes before the chain finished, and three times more as steps passed, the last at 22:41 with the suite's backlog still filling. Its transcript shows why: it started the watcher in the background and ended its turn to wait, and ending a turn hands a subagent's report back. scalable-flink-skill #158 tells an agent to wait inside its turn, with a time limit on `watch.sh` | No scorecard reached the agent, and the probe and ceiling were not run. The chain itself finished at 23:25 |

## 12. Clean-room run 55: the agent waited, and 1→2 fell short — 2026-10-14/15

The first clean-room run on the skill after #158
([clean-room-run-55.md](clean-room/clean-room-run-55.md)), and the skill
change made from it (scalable-flink-skill #159). Rates compare only within one
run.

| # | what happened | what it showed |
|---|---|---|
| 1 | The agent re-ran `watch.sh` with a time limit 12 times, reported after `results/DONE` with the scorecard, and ran the probe and the ceiling | #158 fixed what row 10 of §11 found. One wait ran over a 10-minute tool limit at `WATCH_MINUTES=9`; #159 says 8 |
| 2 | 1→2 read 1.830× and was not met: it could be as low as 1.763×, 2.1% under 1.80×, after six extra settling passes. 2→4 read 1.863×, met inside the 1% margin | The probe's memory-heavy 1→2 is about 1.80× with a spread wider than the shortfall, so the Mac is neither ruled in nor out. The ceiling shows Kafka is not the limit. **Cause not measured** |
| 3 | The tiny proof passed 1→2 at 1.778×, under the target, and said nothing | One short pass per case cannot judge the target, but it can warn. #159 prints a warning and carries on |
| 4 | The verdict sentence "the low end of its range, 1.763x, is 2.1% under the 1.80x target" sat beside 1.83× | The author, 2026-10-15: "this does not make sense". #159 names both numbers: "measured 1.830x, but it could be as low as 1.763x" |
| 5 | The Mac slept from 20:01 to 20:06 on battery, inside completeness | It fell between the drain and the verifier, so no reading moved. Fixed in scalable-flink-skill #160 (`all` keeps the Mac awake) and #162 (a case whose window spans a sleep is thrown out, each step records how long the machine slept, and preflight reports mains or battery) |
| 6 | The agent's findings F1–F4 and F10 were gaps in `harness/README.md` and an error message | Fixed in #159. F6–F8 are fixed in #163: the broker advice says to carry on with nobody to ask, the README warns that the start of `all` kills a watcher, and the sink-name rule is in the README's `design` row. F5 is row 5 |


## 13. What the dashboard's output reader costs — 2026-10-18

The harness-built dashboard (scalable-flink-skill #161) runs one more consumer
over the pipeline's outputs, `<project>-kafkametrics`, which counts their
records and distinct keys. On a pipeline whose outputs grow with its input it
reads five records for every input record, so it was the part of the dashboard
most likely to slow the pipeline down.

**How it was measured.** Clean-room run 55's build (`6cdf0a365d09554f`), the
skill at `4ffa99a`, the tiny proof's 4-core case. One stack, one backlog of
120,000,000 orders, the dashboard on. The only change between cases was
whether the reader's container was running: stopped for the reader-off cases,
started for the reader-on ones, alternating, three of each. Its state was read
back before every case, and Prometheus's count of the records it read was
taken before and after every reader-on case. The script, the log and every
case record are in [reader-cost/](reader-cost/).

| case | reader | records per second | against the reader-off cases on either side | records the reader read |
|---|---|---|---|---|
| 1 | off | 463,095 | | |
| 2 | on | 448,325 | +0.5% | 187,515,936 |
| 3 | off | 429,472 | | |
| 4 | on | 419,972 | +0.6% | 204,475,390 |
| 5 | off | 405,478 | | |
| 6 | on | 415,801 | +2.5% (case 5 only) | 158,513,582 |

**No cost that can be measured.** Every reader-on case read within 2.5% of
the reader-off cases beside it, and the three are all higher, not lower. A
two-pass ratio carries about ±4% on this laptop. Taken across all six, the
reader-on average is 1.1% under the reader-off average, which is inside the
same range. All six cases passed every check, with the worker at 98.8–100.4%
of its core limit.

**The rate fell 12.4% over the hour, whether the reader was off or on** (case
1 to case 5). That is why each reader-on case is compared with its neighbours
and not with the averages. **Cause not measured.** Other programs used 3.22
cores at the close of case 3 and under 0.62 at every other open and close.

**An earlier single reading is superseded.** A tiny proof of each half that
morning, with the whole dashboard off and then on, read 4 cores 6.9% lower with
it on (485,360 against 452,029 records per second). One reading per half, taken
with the rate drifting as above, cannot separate a cost from the drift. The
alternating cases above can.
