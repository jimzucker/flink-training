# Clean-room validation, run 16 — a repeat of run 15, nothing changed

Run 15's prompt again, on the same harness (#54/#55, `main` at `83437ff`),
same model, fresh agent, empty directory. Nothing has changed between the two
runs except documentation. The question is stability: does a repeat land where
run 15 landed, and does the whole run come in clean this time.

## The criteria, written before launch

| criterion | run 15, the sample to compare against |
|---|---|
| the chain passes on the first attempt — no phase rerun, no fallback | **FAIL in run 15** — three attempts, both false starts the agent mis-sizing its own backlog |
| harness verbatim (`prove.py all --quick`), one suite, 0 forbidden-path reads | 155 tool calls, 0 hits |
| chain `preflight` → `report` ≤ 45 min | 37.9 min (15), 37.1 (14) |
| whole-run wall clock (reported, not judged) | 1 h 51 m (15), 55.9 min (14) |
| no case refused for broker memory | 0 limit hits in every case (15) |
| every case at 95–101% of cap | 99.4 / 100.4 / 97.7% (15) |
| sentinel measured, drift reported | −3.1% (15) |
| table stamped unpublishable | `quickLook` / `publishable: false` (15) |
| ratios (recorded, not judged) | 1→2 = 2.137×, 2→4 = 1.836× (15) |
| **stability**: each step ratio within the run-to-run spread the record already carries | 1→2 has read 2.01–2.17× across runs 12–15; 2→4 has read 1.35–2.15×, and 1.35× is now explained ([broker](rig-2026-09-05-broker.md)). A 2→4 outside 1.72–1.85× is the finding |

Each agent builds its own pipeline, so absolute rates are not comparable
between runs; the ratios and the clock are.

## Response

*(written after the run)*
