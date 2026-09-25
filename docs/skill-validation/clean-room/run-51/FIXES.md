# Fixes

Every change made to chase a number, with its prediction written before it was measured.

## Attempt 0 — first chain, build fa1f233378a9cfaa, exactly as PLAN.md

STOPPED at the tiny proof (08:07:56, 26.9 min in). 1→2 = 2.268x, 2→4 = **1.486x** against a
lower bound of 1.50x. Every case at 99–100% of its CPU cap; broker at 0.28 / 0.42 / 0.89 of
2.5 cores; GC 9.1% / 2.6% / 0.9%.

What I measured before changing anything (§6: "measure before you guess"):

| measurement | result |
|---|---|
| `prove.py probe --repeats 9` | plain arithmetic 2→4 **2.01x** [1.96–2.03]; memory-heavy work 2→4 **1.50x** [1.32–1.70], middle half 7%. The machine's own memory-heavy work doubles no better than this pipeline did. That keeps the machine in as a possible cause; it does **not** show the pipeline is memory-bound (not measured). |
| `prove.py ceiling` (4 cores, suite backlog, broker capped 2.5 → 1.0 → 0.5 cores) | 89,609 / 90,878 / 85,045 rec/s, worker 99–100% of cap throughout, broker at 0.49 of 0.5 in the last step. The broker is not the ceiling: squeezing it to 1 core changed nothing. |
| the tiny proof's own per-checkpoint offsets at 4 cores | warm-up intervals read 87,952 / 98,490 / 93,674 / 97,242 rec/s. The 30 s window then held one interval at **48,866/s**, then 78,365 and 85,923. The 2- and 1-core cases show no such dip (46–48k and 20.5–21.7k in every interval). |

## Attempt 1 — no change; re-run the chain unchanged

- **Which §6a row:** none. This is a retry, not a lever: §6 says *"Retry the same case once if
  the failure is plainly transient"*. The one-interval dip at 4 cores, bracketed by readings at
  nearly twice its rate, is the only thing that put 2→4 under the tiny proof's bound. Cause of
  the dip: **unknown** — not measured. Host load average was 8.55 → 10.0 across that window,
  but the ceiling cases ran at 7.7–13.3 and did not dip, so load does not explain it either.
- **Prediction, written before measuring:** the tiny proof passes its bounds, with 2→4 between
  1.70x and 1.95x (the ceiling's 85–91k at 4 cores over the tiny proof's 47k at 2 cores —
  different backlogs and windows, so this is a guess, not a comparison). I also predict the
  **suite's** 2→4 will come in **below the 1.80x target** on the low end of its range, because
  the machine's memory-heavy arm returns 1.50x on the same step. If the tiny proof fails again
  the same way, I stop there: nothing in §6a matches these symptoms (every case at its cap,
  source not idle, back-pressure 2–3%, GC under 1% at 4 cores, broker not the ceiling).

**Result of attempt 1 (chain 2, 08:31–09:02):** STOPPED at the tiny proof again, on the other
step. 2→4 = **1.907x** (inside my predicted 1.70–1.95x; per-interval 83–95k at 4 cores, no dip).
1→2 = **2.580x**, above the 2.50x bound: the 1-core case read 18,503 rec/s (chain 1: 20,791),
GC 8.6% of capacity, host load **12.5 → 9.96** across its window. At 09:03, with the stack idle,
macOS's XProtect remediator was using 71–84% of a core and the load average was 7.8; it finished
by 09:05 and idle load fell to 2.6. So the 1-core window very probably overlapped that scan —
a hypothesis, not measured.

## Attempt 2 — no change; the tiny proof alone, on a quiet host (control)

- **Why:** the 1-core case read low in both chains (1→2 = 2.27x quiet, 2.58x noisy). Before
  changing memory for it, I need one measurement of the unchanged build on a quiet host, or a
  memory change cannot be told apart from the host calming down. This is a control arm, not a fix.
- **Prediction:** 1→2 between 2.15x and 2.40x (chain 1's quiet reading was 2.27x) — inside
  the bound; 2→4 between 1.75x and 1.95x. 1-core GC stays at 8–9%. If 1→2 is above 2.50x again
  on a quiet host, the next attempt is the memory lever (§6a row 2): raise `tmMemoryBase`.

**Result of attempt 2 (09:06–09:26):** TINY PROOF PASSED, 147/147 guards fired as expected.
1c 20,163 / 2c 43,065 / 4c 92,272 rec/s: 1→2 = **2.136x**, 2→4 = **2.143x**. Host load stayed at
2.5–7.4. 1-core GC 8.6% (as predicted: unchanged). 1→2 landed inside my predicted 2.15–2.40x only
just below it; 2→4 came in **above** my predicted 1.75–1.95x, because the 2-core case read low
(43,065 against 47,150 and 47,739 in the two chains before). The prediction was wrong on 2→4.

Three single-pass tiny proofs of one build read 2→4 at 1.486x, 1.907x and 2.143x, and 1→2 at
2.268x, 2.580x and 2.136x. That spread (about ±18%) is the tiny proof's own noise on this
pipeline and this host; it is wider than the bounds it is judged against. No lever was pulled:
the memory change I had queued for the 1-core case was not needed to pass and would have been
measured against noise this wide.

Next: the suite (3 passes per case plus the sentinel) on this same build, unchanged.

## Suite A — build fa1f233378a9cfaa, unchanged (09:39–10:23)

2→4 = **2.121x** (range across passes 2.067–2.186x), 2c mean 45,778 (spread 1.2%), 4c mean
97,108 (spread 4.4%), every case 97.9–100.3% of its cap. **All four 1-core passes were thrown
out as ceilings**: garbage collection 6.7% / 7.3% / 8.2% / 7.4% of capacity (limit 5.5%) and
9.7–12.4% less work per core than the 2-core case. So 1→2 has no number and the claim is not met.
The harness's advice: "Give it more memory instead of more cores."

## Attempt 3 — §6a row 2, memory per subtask: raise the fixed base

- **Change (one):** `caps.tmMemoryBase` 1088m → 1728m and `caps.tmMemoryPerCore` 640m → 512m.
  Process size per case: 1c 1728m → **2240m**, 2c 2368m → **2752m**, 4c 3648m → **3776m**.
  The per-core share drops so the 4-core worker still fits the VM (3,776 + 4,608 broker + 1,600
  job manager = 9,984 MiB against 9,937 — 47 MiB over, where the unchanged 640m would have been
  over by 559). The 4-core case ran GC at 0.8%, so a smaller per-core share should not hurt it.
- **Which row:** §6a "memory per subtask, not one flat figure"; the harness's base-term note
  (*"Flink's fixed overheads are most of a small process size. Hence the base term."*).
- **Prediction, before measuring:** 1-core GC falls from 6.7–8.2% to **3–5%**, under the 5.5%
  limit, so the 1-core passes are kept. 1-core rate rises **0–5%** (GC can return at most the
  3–4 points it gives back). 1→2 then reads about **2.15–2.30x** — above 2.00x, i.e. the harness
  will say the baseline reads low: the 1-core case does less work per core than 2 cores (about
  20.3k against 22.9k per core in suite A), and GC explains only part of that. 2→4 unchanged
  within ±4% (2.05–2.20x). If the 2- or 4-core case gets slower by more than 4%, revert.
- **Measured with:** `prove.py tinyproof` (every case, one build), then the suite once to confirm.

**Result of attempt 3 (tiny proof 10:24–10:36, suite B 10:36–11:21):** the prediction was
**wrong** on the thing it was for.

| | suite A (1088m + 640m/core) | suite B (1728m + 512m/core) |
|---|---:|---:|
| 1-core GC, four passes | 6.7 / 7.3 / 8.2 / 7.4% | 6.6 / 8.1 / 7.3 / 8.1% |
| 1-core passes kept | 0 of 4 | 0 of 4 |
| 1-core rate, mean of the four | 20,266 | 21,025 |
| 2-core mean (spread) | 45,778 (1.2%) | 46,236 (6.7%) |
| 4-core mean (spread) | 97,108 (4.4%) | 93,147 (8.5%); one of three passes thrown out at 94.5% of cap |
| 2→4 (range across passes) | 2.121x (2.067–2.186) | 2.015x (**1.864**–2.169) |

The tiny proof had read 1-core GC at 5.54%; the suite read 6.6–8.1%. Giving the 1-core worker
30% more memory did not move its garbage collection. What drives it is **not known** — it is not
the amount of memory, on this evidence. The 2→4 step's low end, which the claim is judged on, fell
from 2.067x to 1.864x. **Reverted** to 1088m + 640m per core (pipeline.json restored;
`results/suite.*` restored from suite A, the same build under the configuration it ran with;
suite B kept in `scratch/suite-B/`).

## Stopping here

- Levers used: one (memory). Retries without a change: two.
- No row left in §6a matches: every case at its CPU cap, source never idle, back-pressure 2–3%,
  broker not the ceiling (`prove.py ceiling`: squeezing it to 1 core changed nothing), key
  layout even, input already read once, sink not waiting to write, and the harness cannot
  give fewer subtasks for the same cores.
- What stays unmeasured: 1→2. The harness says the two ways out are to fix the 1-core case or to
  take it out of `cases` and claim only 2→4. Removing it is a new claim; with no human to agree
  to it, I have not made it. The result stands as: **2→4 = 2.121x, 1→2 has no number.**
