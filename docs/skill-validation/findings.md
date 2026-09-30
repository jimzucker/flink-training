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
