# Clean-room validation, run 14 — one clean run, 1/2/4

Run 13's prompt again (headline: both step ratios, 1→2 and 2→4, each with
its spread) on the harness as it stands at #49 — `capFloorBaseline` 0.95,
band one-sided ≥ 1.85, `--quick` available but not used here. Same conditions
as [runs 1–13](README.md): fresh agent, empty directory, barred from this
repository and every other test directory, one prompt, no human input.

The question is speed and stability: how long a run takes when nothing is
being changed underneath it, and whether the stack holds up for a full suite.
An earlier attempt at run 14 (2026-09-04 19:57 and 20:05) was abandoned for a
laptop restart before its chain finished; nothing from it is kept, and its
directory was moved aside before this launch.

## The criteria, written before launch

| criterion | what the record says |
|---|---|
| uses the harness verbatim; one suite; 0 forbidden-path reads | runs 11–13 |
| chain `preflight` → `report` ≤ 1.25 h (required) | 57.7 min (run 13), 64.4 (12), 66.2 (rig, 2026-09-04) |
| whole-run wall clock (reported, not judged) | 2 h 05 m (13), 4 h 32 m (12), 1 h 58 m (11) |
| the stack holds: no Flink restart, no rig-scoped refusal, no rebuild after the chain starts | run 13 had none; the rig run had one case refusal (92.8% of cap) and reported anyway |
| every case's spread inside the 20% ceiling | 8.8 / 5.8 / 6.1% (13); 8.8 / **17.6** / 3.6% (rig) |
| baseline ≥ 3 valid passes of 4 | 4 of 4 (13) |
| sentinel measured, drift reported | −2.5% (13), −7.0% (rig) |
| 1→2 ≥ 1.85 | 2.171× (13), 2.013× (12), 1.990× (rig) |
| 2→4 ≥ 1.85 | 1.850× (13), 2.154× (12), 2.004× (rig), 1.869× (11) |

The rig run of 2026-09-04 is the harness on a built pipeline
([record](rig-2026-09-04.md)); this run adds the agent, so the wall clock is
expected to be longer by whatever it spends writing and calibrating its own
pipeline — 1 h 07 m of run 13's 2 h 05 m.

## Response

*(written after the run)*
