# Fixes

Every change made to chase a number, each with its prediction written
**before** it was measured, and reverted if it made the step worse
(SKILL.md §6, "When there is no human to say yes").

Changes made to get the build working — compile errors, a job that would not
start — are not here; they are in `JOURNAL.md`. This file is only for changes
whose purpose was to move a measured step.

| # | change (§6a row) | prediction, written before measuring | measured | kept or reverted |
|---|---|---|---|---|
| — | **none made** | — | — | — |

**Why no change was made.** The chain stopped at the report with 2→4 not met:
1.799× mean, low end 1.725×, 4.2% under the 1.80× target, range across pairs
1.573×–2.183×. The harness's own verdict was *"More passes could change that;
change nothing in the pipeline on it"*, and SKILL.md §6 says *"With nobody to
ask, report the step as not met, with the percent, and stop — that is a
result"*. §6 also says the two measurements below are *"not optional when a
step falls short and the scorecard says CPU"*, so both were run, and neither
pointed at a lever:

- `prove.py probe --repeats 9` (no pipeline): simple arithmetic 2→4 1.98×
  (middle half 1%); memory-heavy work 2→4 **1.65×** (range 1.37–1.72×, middle
  half 7%). This machine's own cores return less than the target on
  memory-heavy work, so the host cannot be ruled out as the limit.
- `prove.py ceiling` (4 cores, broker squeezed): broker at 2.5 cores
  504,732/s; at 1.0, 503,901/s; at 0.5 (broker pinned at 95%), 487,985/s. The
  worker held 99% of its cap throughout. The broker is not the 4-core ceiling.

The §6a rows whose symptoms could match were checked against the record:
every case at 95–100% of cap, source idle 0–2.2%, garbage collection 0.7–2.9% (case means),
input already read once, memory already per subtask, sinks already
compressed. The key-spread preflight row passed (busiest by-symbol subtask
holds 26.0% of the keys at 4 cores, against 25% even), so the "spread the keys
evenly" row's trigger — *"the preflight row says the keyed stage's keys land
unevenly"* — did not fire. No row's symptom matched, so the tuning loop had
nothing to apply.

**What is not explained.** The 4-core passes rose late in the session on one
unchanged build: 422,338, 428,256, 410,837 and 429,480/s in the first four,
then 467,289 and 492,953/s in the last two, and 504,732/s in the ceiling run
10 minutes later. The 2-core passes did not
(256,072, 225,781, 258,294, 238,688, 234,027, 261,124). Other programs used
0.07–1.9 cores at every window open, and host swap sat at 1.6–2.7 GB with no
split between fast and slow passes. Why the 4-core rate rose is not known; I
have not measured it.

