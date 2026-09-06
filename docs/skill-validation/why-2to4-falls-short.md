# Why 2→4 reads 1.8× in the skill runs and 1.99× in the demo

The clean-room runs kept reporting a 2→4 step ratio of 1.72–1.84× while this
repository's own demo reports 1.96–1.99×. Same laptop, same 8 partitions, same
axis — `scale-units.sh` sets `TASKMANAGER_CPUS=n` and `PARALLELISM=n` on one
task manager, which is exactly what the harness does. This is what the
difference turned out to be.

## The demo, re-measured today

`UNITS="2 4" scripts/scale-units.sh`, 2026-09-06, on the same machine as the
skill runs, with the same broker and 8 partitions:

| units | orders/sec | per core | Flink cores | back-pressure |
|---:|---:|---:|---:|---:|
| 2 | 81,582 | 40,791 | 2.00 | 25.7% |
| 4 | 162,657 | **40,868** | 3.98 | 48.0% |
| | **1.99×** | **+0.2%** | | |

Per-core throughput is flat. The demo did not degrade and the machine is not
the problem.

## The skill runs, same machine

| run | 2→4 | per core, 2c → 4c |
|---|---:|---|
| 13 | 1.850× | 160,406 → 148,383 (−7.5%) |
| 15 | 1.836× | 262,828 → 241,228 (−8.2%) |
| 16 | 1.809× | 161,457 → 146,008 (−9.6%) |
| 14 (re-measured, broker fed) | 1.725× | 158,926 → 142,549 (−10.3%) |

## The controlled comparison

Run 16's own build, CPU cap pinned at **4 cores** in every case, parallelism
alternated 2 / 4 so order cannot produce the effect. Nine cases:

| | parallelism 2 | parallelism 4 |
|---|---:|---:|
| n | 4 | 5 |
| rate | 621,501 | 553,124 |
| per used core | **158,218** (154.5k–161.7k) | **142,549** (140.2k–145.9k) |
| cap used | 98.2% | 97.0% |
| source idle | 0.4% | 2.6% |
| GC | 2.0% | 3.6% |

The ranges do not overlap. Two subtasks on four cores beat four subtasks by
12.3%, and 158,218 per used core is the 2-core case's own figure (158,926) —
so per-core efficiency is preserved by *cores* and lost to *parallelism*.
That single effect accounts for the whole step-ratio shortfall: at 142,549 the
4-core case reads 1.74×; at 158,218 it would read 2.0×.

## Where the lost CPU goes, as far as it was measured

Flink's own counters, one case at each parallelism on the same 4-core cap:

| | par 2 | par 4 | costs |
|---|---:|---:|---:|
| shuffle bytes per record | 225.1 | 230.8 | ~0.3 pts |
| checkpoint duration | 110 ms | 154 ms | 0.4 pts (44 ms per 10 s) |
| checkpoint state | 180 KB | 187 KB | — |
| GC | 1.79% | 3.79% | 2.0 pts |
| source idle | 0.41% | 3.03% | 2.6 pts |

About 5 of the ~11 points are accounted for. **The remaining ~6 points are
unattributed CPU inside the worker.** Two attempts to attribute it failed and
are recorded as failures, not evidence: a context-switch probe read only the
thread-group leader's counters (1.3 switches/sec — the idle main thread), and a
per-thread CPU probe collected no samples because its process lookup inside the
container returned nothing. Attributing the rest needs a JVM profile, and the
Flink image ships `jfr` but no `jcmd`, so nothing can attach to a running JVM
without changing how the task manager is started.

## Why the demo does not pay it

The overhead is a fixed cost per record, not a fraction of the work: 6.32 µs
per record per core at parallelism 2 against 7.02 µs at parallelism 4, a
difference of **0.70 µs per record**. The demo's job spends ~24.5 µs of CPU per
record (40,791 per core) against the skill pipelines' 4.2–6.8 µs. The same
absolute overhead is therefore ~10% of a light record and ~3% of a heavy one.

Applying that 0.70 µs to the demo's records predicts 1.95× where 1.99× was
measured — the model over-predicts the loss slightly, so it explains most but
not all of the difference.

## What this means for the skill

Nothing is wrong with the harness or the method: the skill reproduced the
demo's axis, its partition count and its backlog-drain measurement, and its
guards held throughout. The step ratio an agent measures depends on how much
CPU its own pipeline spends per record, and the agents write pipelines 4–6×
lighter than the demo's. A 2→4 of 1.8× on a light pipeline and 1.99× on a heavy
one are the same rig telling the truth about two different jobs.
