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
