# Clean-room validation, run 9 — the short skill, and the guard that was scoped wrong

The first run against the skill after it was cut from 11,286 words to 3,546 —
guards, preflight and interview kept; the run anecdotes removed. The question:
**does the shorter skill still carry the method, and does 2→4 scale linearly?**

Same conditions as [runs 1–8](README.md): fresh agent, empty directory, barred
from this repository, every earlier test directory and the preserved results,
allowed only the skill, one prompt, no human input. **DataStream pinned.** The
prompt asked for the 2→4 step ratio with its spread as the headline.

**Model: Claude Opus 5.**

## What it cost

| | |
|---|---:|
| Human time | **0 h** |
| Human prompts | **1** |
| Wall clock | 2.75 h (06:53 → 09:39, then killed by API overload) |
| Suites run | 7 |
| Final report | **not written** — the agent was terminated four times by a server-side `529 Overloaded` and never got to it |

The record below is assembled from the raw results it left in `results/`,
which is what they are for.

## The answer: yes, 2→4 is linear

Seven suites, one build (`350db38e745d3f5e`), the 2- and 4-core cases ≥98% of
cap and 0.0% external back-pressure in every row:

| suite | passes | 2c legs/s | 4c legs/s | **2→4** |
|---|---:|---:|---:|---:|
| 1 | 2 | 485,780 | 1,095,404 | 2.25× |
| 2 | 2 | 487,765 | 1,065,649 | 2.18× |
| 3 | 3 | 496,583 | 978,942 | 1.97× |
| 4 | 3 | 469,316 | 1,039,650 | 2.22× |
| 5 | 1 | 496,271 | 1,034,084 | 2.08× |
| 6 | 3 | 502,229 | 1,081,979 | 2.15× |
| 7 | 3 | 546,876 | 1,070,832 | 1.96× |
| **pooled, 17 passes per case** | | **499,316** | **1,051,200** | **2.10× (105%)** |

Rates are input legs per second — this run split each block order into 4
allocation legs and measured the leg stream, so divide by 4 for orders.

Range 1.96–2.25×, never below linear. The demo pipeline in this repository
measured 2.11× on the same axis. Run 8's 1.79× is now the outlier in the record,
not the trend.

Suite 7, the last and cleanest, in full:

| case | pass 1 | pass 2 | pass 3 | mean | spread | % of cap | broker |
|---|---:|---:|---:|---:|---:|---:|---:|
| 1c | 253,551 | 283,515 | 292,511 | 276,526 | **15.4%** | 99.8–99.9% | 0.30 |
| 2c | 533,571 | 543,745 | 563,312 | 546,876 | 5.6% | 99.9–100% | 0.50 |
| 4c | 1,058,231 | 1,090,517 | 1,063,749 | 1,070,832 | 3.1% | 99.5–99.9% | 0.87 |

Header, as the new skill asks: axis *one worker growing* (parallelism = cap =
slots, read back); DataStream, no SQL; exactly-once checkpointing with an
at-least-once sink emitting the absolute position; checkpoint 5 s; 4
partitions; rate from committed offsets; CPU from cgroup `cpu.stat`.

## What the shorter skill carried

- **Preflight 17/17 PASS** in one file, printed as the skill's table: both
  images arm64-native, JDK 17 pinned with the host's Java 25 noted and unused,
  state dirs writable, no reporter jar copied, disk budget 9 GB against 134 GB
  free, retention on both sink topics, generator deterministic *and* seed-
  sensitive, one cap mechanism, consumer group and transactional prefix
  carrying the run id, the trim command recorded.
- **Tiny proof** 1→2 = **1.99×**, both cases at cap, and **two refusals fired
  on purpose** — a worker claimed at 4 cores with 2, and a backlog one record
  off its manifest — both caught.
- **Completeness** on 12,000,000 records: 64 and 2,048 keys exactly as
  predicted, both aggregations 4,855,159 net quantity against the manifest,
  and the same assertions after **killing the worker at record 6,658,745**.
- **Baseline constraint problem solved by measurement, not story.** Its first
  1-core case reached 53% of cap with heavy throttling. It ran a controlled
  A/B on the CFS period — one rig, one build, one variable, both arms twice —
  before touching anything else. That is §8 of the skill being followed
  under time pressure.
- It ran **two further controlled experiments** (page cache, window length)
  on the 1-core noise before concluding it could not explain it, and said so.

## The one it broke — and it was the skill's fault

The new spread guard was written *"refuse if the spread exceeds 10%"*, and the
harness did exactly that: six of seven suites were refused, every one on a
case whose spread was 10.8–41.7%, five of them on the 1-core case. In all six
the 2- and 4-core cases were within tolerance and the 2→4 ratio was valid.

The 1-core case spread 14–42% in every suite and rose monotonically within
each one (253k → 284k → 293k in suite 7). **The cause is unmeasured.** The
agent's last hypothesis — each pass reads a different region of the log — was
about to be tested when the API terminated it. It is a hypothesis.

What is not a hypothesis: a guard about one case's *data* was scoped like a
guard about the *rig*, and it cost about 110 of the run's 165 minutes in
re-runs of a suite whose headline number had been valid since 07:49. The skill
now distinguishes the two — rig refusals stop the suite; data refusals mark
the case and every ratio it is part of as unreportable, and the rest reports.

## What it sent back into the skill

- **spread voids the case, not the suite** — the fix above, promoted to the
  guard table with the run that paid for it
- the 1-core case is the noisy one in every run that repeated it (8 and 9);
  that is one more reason the headline is 2→4
