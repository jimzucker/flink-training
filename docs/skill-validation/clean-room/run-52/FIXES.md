# Fixes

Every change made to chase a number, each with its prediction written before
it was measured. A change that made a step worse is reverted.

## Changes to chase a number

**None.** The chain ended "not settled" on 2→4 (1.86×, interval 1.77×–1.90×
against the 1.80× target), and 1→2 was met (1.97×). SKILL.md §6 says of a step
that is not settled: "A step that is not settled is not a shortfall ... if it is
still open the chain stops saying so, and nothing in the pipeline is changed on
it." The tuning loop ("tune until you run out of levers") is for a claim that
is not met; this one was neither met nor missed. So no lever from §6a was
tried, and there is no prediction to record.

What I would try first if a person asked for it, written down before any
measurement so it can be wrong in public: the symbol stage's keys land
1065/1008/979/1044 at 4 cores (preflight), so the busiest subtask holds 4.0%
more than an even share. Setting a `pipeline.max-parallelism` that evens it
(§6a "spread the keys evenly") predicts at most +4% on the 4-core case, so 2→4
from 1.86× to about 1.90–1.93×. Not measured.

## Changes made before any number existed (not tuning, listed for completeness)

These were made while getting the build through its first completeness run
(`prove.py completeness`, run by hand as a smoke test before `prove.py all`).
None of them was measured for speed, and none was chosen to move a ratio.

| # | change | why |
|---|---|---|
| B1 | The job parses its own `--key=value` arguments | Flink's `ParameterTool.fromArgs` does not read `--key=value`; the first submit stopped with "No data for required key 'bootstrap'" |
| B2 | Repeats on purpose went from 1 in 400 to 1 in 200 records, and `outputsPerInput` from 5 to 4.975 | The completeness design diff compares `outputsPerInput` with the measured rows per input record exactly, and read back 4.987 against 5. With 1 in 200 the true figure is exactly 5 × 199/200 = 4.975 |
| B3 | Two dashboard queries: the source operator is named `Source: orders-source` in the metrics, not `orders-source`; task names shortened with `label_replace` | Read off Prometheus after the smoke run |
