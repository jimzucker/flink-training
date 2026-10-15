# Fixes

Every change made to chase a number, each with its prediction written **before**
it was measured, and reverted if it made the step worse (SKILL.md §6).

**No change was made to chase a number.** The suite's 1→2 step (1.830×, judged
on 1.763×–1.847×) reaches both sides of the 1.80× target, and SKILL.md §6 says
for that case: run `probe` and `ceiling`, *"Change nothing in the pipeline.
Report the step as not met, with the percent, and stop."* Both were run:

| measurement | what it found |
|---|---|
| `probe --repeats 9` (`results/hostprobe.json`) | the machine's own cores, no pipeline: simple arithmetic 1→2 1.94× (1.92–1.96×), memory-heavy 1→2 1.80× (1.68–1.94×), middle half 4%. The shortfall is 2.1%, inside that middle half: the machine can be neither ruled in nor out |
| `ceiling` (`results/ceiling.json`) | 4 cores, broker capped at 2.5 / 1.0 / 0.5 cores: 447,262 / 437,915 / 394,602 orders/s, broker using 0.48 / 0.49 / 0.49 cores. The broker is the limit only when squeezed to half a core; at its real cap the pipeline is at its own limit |

Why 1→2 reads short is **not known**; nothing measured here isolates a cause.

Two changes were made **before any throughput number existed**, to fix
defects, not to move a number (both in `JOURNAL.md`):

1. The verifier counted a key that repeated its last record after the restart as
   a gap; corrected so a repeat counts as that key's one allowed step back.
   Proved on the recorded killed-arm outputs before re-running.
2. The market-value sinks were renamed `sink-market-values-by-*` so the job graph
   ties them to their topics.
