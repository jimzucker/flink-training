# Clean-room validation

Seven runs of the same problem, each by a fresh agent in an empty directory, barred
from reading this repository or any earlier run, allowed only
[`SKILL.md`](../../.claude/skills/prove-it-scales/SKILL.md), given one prompt and
no human input.

The point is not that the runs succeeded. It is that **the skill has to carry the
method**, because nothing else is available to the agent: not this repository, not
the person who wrote it, not the runs that came before. A skill that merely
*describes* a method reads the same as one that carries it, and this is the only
way to tell them apart.

| run | API | wall | cost | tool calls | 1 core | 4 cores | ratio | TM % of cap at 4 |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| [1](clean-room-run-1.md) | DataStream | 2.52 h | $166.58 | 863 | 54,029 | 242,983 | 4.50× | — |
| [2](clean-room-run-2.md) | DataStream | 1.78 h | $43.74 | 110 | 65,027 | 262,599 | 4.04× | 101% |
| [3](clean-room-run-3.md) | DataStream | 1.74 h | $36.19 | 113 | 82,452 | 288,220 | 3.50× | 98% |
| [4](clean-room-run-4.md) | **SQL** | 1.61 h | $35.65 | 141 | 43,437 | 164,393 | 3.78× | 104% |
| [5](clean-room-run-5.md) | DataStream | 1.47 h | $30.24 | 84 | 71,278 | 201,033 | ~~2.82×~~ | **94%** |
| [6](clean-room-run-6.md) | DataStream | 1.94 h | $38.20 | 120 | 81,651 | 318,868 | **3.91×** | **100%** |
| [7](clean-room-run-7.md) | DataStream | **0.92 h** | **$22.63** | 98 | 82,530 | 267,734 | 3.24× | **101%** |

Human time was **0 h** and human prompts **1** in every row.

**Do not read the throughput columns across rows.** Each agent chose its own
fan-out, key cardinality, checkpoint mode, record shape and partition count, so
the columns are not measuring the same work. Only the ratio is constant-workload,
and only within a row.

## The trend that is the actual result

From run 1 to run 5: **2.52 h → 1.47 h, $166.58 → $30.24, 863 tool calls → 84.**
An 82% reduction in both time and cost, with the problem unchanged and only the
skill text between them. The later runs are not thinking less; they are thrashing
less.

**Run 7 is the fastest and cheapest of the seven** — 55 minutes and $22.63 — after
two changes aimed squarely at wall clock: kill a worker during the tiny proof
rather than after the suite, and budget the suite before running it. Per-case
overhead fell from 67 seconds to 14, because it asserted warm-up to steady instead
of sleeping a fixed interval.

**Read the step ratios, not the ratios against the baseline.** Run 7 scales at
**97% efficiency from two cores up**; its headline 3.24× is dragged down by a
baseline that did structurally less work, because at parallelism 1 a `keyBy`
repartitions nothing. Run 6 has the mirror distortion in the other direction.
Both reported the 1→4 ratio as the result, and in both the honest number was
measured from two cores up. That is now a rule: **the baseline is a case, not a
reference point.**

**Run 6 breaks that trend deliberately** — slower and dearer than run 5, and the
only recent table that survives scrutiny. Run 5's 2.82× is struck through above
because the pipeline had stopped being the constraint: 94% of cap with 43.6%
back-pressure, so the number describes the sink path rather than the pipeline. A
table that has to be withdrawn costs more than 27 extra minutes.

The declining ratios are runs getting more honest, not systems getting worse. Run
1's 4.50× is superlinear and no later run reproduced it. Run 6 met the same effect
at its 1→2 step, explained it — garbage collection is a fixed cost *per JVM*, not
per core, so a single capped core carries overhead the wider cases amortise — and
recommended quoting ratios from two cores up rather than banking the flattering
number.

## What replicated

**Every run that went looking for the ceiling found the broker**, at a 0.5-core
squeeze (run 2 pushed to 0.15). Six agents, no shared code, no shared prompt
beyond the problem statement. That is the closest thing here to a replicated
result.

## What each run paid for

| run | what it hit | what went into the skill |
|---|---|---|
| 1 | its own `compose` command took 15.6 GB of backlog with it | infrastructure commands that quietly destroy data |
| 2 | 90M records lost to `--delete --topic a --topic b` | the same trap, differently shaped |
| 3 | refused twice at 16.6% and 20.0% vantage-point disagreement | anchor windows on commit boundaries, ≥3 per window |
| 4 | **`apache/flink` publishes amd64 only; the host is arm64** | check every image's architecture — the silent invalidator |
| 5 | a full host disk, then a table that had to be withdrawn | a disk budget, retention, and constraint ownership as a guard |
| 6 | its own idempotence claim disproved by killing a worker | the sink guarantee and the checkpointing guarantee are two settings |
| 7 | its own baseline refused by a guard written the day before | measure back-pressure at the **external boundary**; the baseline is a case, not a reference point |

**Runs 4, 5, 6 and 7 each corrected the skill's own text.** Run 4 found that the
documented fix for a no-op CPU command creates a second trap. Run 5 found that
`retention.bytes` is a periodic sweep rather than a bound. Run 6 disproved an
idempotence argument the skill was carrying as an exemplar. That is the strongest
evidence the method does something a careful review could not.

## The finding that outranks the rest

Across six runs, **every rule written as a guard was obeyed and several rules
written as prose were broken** — sometimes by the same run, in the same hour, with
the violating number printed in its own results table.

Run 5 obeyed forty-odd prose rules and broke the one that mattered, while
implementing seven guards perfectly. An audit then counted **eighty rules and
seven guards**, and fourteen mechanisable rules became a mandatory guard set, with
a promotion rule attached to the skill itself:

> When a run breaks a rule, that rule becomes a guard with a self-test, or it gets
> deleted. A rule that cannot be enforced is a suggestion.

Run 7 then corrected the guard set itself. Its constraint-ownership guard refused
a valid baseline, because "no material back-pressure" never said *where* to
measure it — and inside a CPU-capped single-slot task manager the source always
waits on threads sharing that core. It diagnosed the guard rather than relaxing
the threshold. Its own verdict:

> All four of my real refusals were guards, and the one rule I had implemented
> from prose rather than thought through — where to measure back-pressure — is
> the one that misfired.

Run 6 was the test of that. It implemented all fourteen, self-tested every one at
**56/56** — each broken on purpose *and* confirmed silent on good input — added
three more of its own, and **refused four times, changing the design on every
one** rather than working around a refusal. That included discarding a completed
set of measurements when the kill test disproved its guarantee choice, because
rows from two builds are a collection rather than a curve.

It produced the first table in six runs where the component under test owned the
constraint in every single case.

## What is still unmade

**The DataStream-versus-SQL comparison.** Run 4 remains the only SQL run and is
the slowest per core, but it used half the fan-out of the DataStream runs, so it
did roughly half the downstream work per input record. Suggestive, not
conclusive. Settling it needs one more run: run 6's exact workload, expressed in
SQL.
