# Clean-room validation, run 14 — one clean run, 1/2/4

**One pass per case, the way a real user looks at it:** `prove.py all
--quick` on the harness as it stands at #49 (`capFloorBaseline` 0.95, band
one-sided ≥ 1.85). Cases 1/2/4, one pass each, the sentinel still repeating
the baseline at the end. Every per-case guard stays live; the table it
produces is stamped `publishable: false`, and the ratios below are recorded,
not judged — the record shows single passes of the same suites reading
2.039–2.273× where three passes reported 2.154×. Same conditions
as [runs 1–13](README.md): fresh agent, empty directory, barred from this
repository and every other test directory, one prompt, no human input.

The question is speed and stability: how fast a real user gets an answer,
and whether the stack holds up for a full chain.
An earlier attempt at run 14 (2026-09-04 19:57 and 20:05) was abandoned for a
laptop restart before its chain finished; nothing from it is kept, and its
directory was moved aside before this launch.

## The criteria, written before launch

| criterion | what the record says |
|---|---|
| uses the harness verbatim; one suite; 0 forbidden-path reads | runs 11–13 |
| **chain `preflight` → `report` ≤ 45 min (required)** | derived, not rounded: the rig's gates cost 25 min (up 19 s, preflight 68, completeness 460, tiny proof 634, fill 325) and a 4-case quick suite costs ~16 min at the rig's 242 s per case |
| whole-run wall clock (reported, not judged) | 2 h 05 m (13), 1 h 58 m (11); of run 13's, 1 h 07 m was the agent writing and calibrating its own pipeline |
| the stack holds: no Flink restart, no rig-scoped refusal, no rebuild after the chain starts | run 13 had none; the rig run had one case refusal (92.8% of cap) and reported anyway |
| every case at 95–101% of its CPU cap | 95.1–100.4% (13); 92.8% refused (rig) |
| sentinel measured, drift reported | −2.5% (13), −7.0% (rig) |
| the table is stamped unpublishable | `--quick` must mark `quickLook`/`publishable: false` and print the banner |
| ratios (recorded, not judged) | 1→2: 2.171 (13), 2.013 (12), 1.990 (rig). 2→4: 1.850 (13), 2.154 (12), 2.004 (rig), 1.869 (11) |


The rig run of 2026-09-04 is the harness on a built pipeline
([record](rig-2026-09-04.md)); this run adds the agent, so the wall clock is
expected to be longer by whatever it spends writing and calibrating its own
pipeline — 1 h 07 m of run 13's 2 h 05 m.

## Response

*(written after the run)*
