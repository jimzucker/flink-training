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

Model: Claude Opus 5. Harness at `83437ff`, run from the skill directory,
nothing copied. 77 tool calls, 0 forbidden-path reads. 18:23 → 20:04 local.
Raw results in [run-16/](run-16/) (the manifests' per-key maps are stripped;
they are regenerable from the seed).

| criterion | result |
|---|---|
| **the chain passes on the first attempt** | **PASS** — one `phase=all start`, every phase rc=0, the suite refused nothing |
| harness verbatim, one suite, 0 forbidden reads | **PASS** — 77 tool calls, 0 hits |
| chain ≤ 45 min | **PASS — 35.6 min** (up 2 s, preflight 68, completeness 400, tinyproof 507, fill 221, suite 939) |
| whole-run wall clock (reported) | **1 h 37 m** — 34 min of it the agent's own sizing calibration, including a 10-minute tool timeout that killed a run and forced a 70M refill |
| no case refused for broker memory | **PASS** — 0 limit hits in every case (broker 4 GiB) |
| every case at 95–101% of cap | **PASS** — 99.6 / 96.3 / 97.9%, sentinel 100.2% |
| sentinel measured | **PASS** — +4.1% |
| table stamped unpublishable | **PASS** — `quickLook: true`, `publishable: false`, banner quoted in the agent's own report |
| ratios (recorded, not judged) | 1→2 = **2.060×**, 2→4 = **1.809×** |
| stability: 2→4 inside 1.72–1.85× | **PASS** — 1.809× |

| cores | records/s | % of cap | src idle | broker hits |
|---:|---:|---:|---:|---:|
| 1 | 153,525 | 99.6% | 0.1% | 0 |
| 2 | 322,914 | 96.3% | 0.7% | 0 |
| 4 | 584,031 | 97.9% | 2.8% | 0 |
| 1 (sentinel) | 159,993 | 100.2% | 0.0% | 0 |

## Is it stable?

Three one-pass runs on the shipped harness, each a different agent's
pipeline, so only the ratios and the clock compare:

| | run 14 | run 15 | run 16 |
|---|---:|---:|---:|
| 1→2 | 2.113× | 2.137× | **2.060×** |
| 2→4 | 1.354×† | 1.836× | **1.809×** |
| chain | 37.1 min | 37.9 min | **35.6 min** |
| whole run | 55.9 min | 1 h 51 m | 1 h 37 m |
| chain attempts | 1 | 3 | **1** |
| broker limit hits | not measured | 0 | 0 |

† measured on a starved broker; the cause is in
[rig-2026-09-05-broker.md](rig-2026-09-05-broker.md) and is now a guard.

With the broker guard live, the two runs that have it read 2→4 within
**1.5%** of each other (1.836× and 1.809×) and 1→2 within 3.6%. The chain
clock has landed in a 2.3-minute band across all three. That is the stability
answer for the chain; it is not a statement about the ratio's own
repeatability, which one pass per case cannot give.

## Measured, not explained

- **1→2 reads above linear and 2→4 below it, again.** Three runs now agree on
  the shape and none has measured a cause. The agent ruled out the broker
  (never above 0.57 of its 2.5-core cap), a starved source (idle ≤ 2.8%) and
  the cap itself (95–100% of periods throttled). No mechanism is offered here
  either.
- The 1-core case's own two measurements differ by 4.1%, and swapping which
  one is used moves 1→2 between 2.018× and 2.103× — a single pass wanders
  more than the gap being argued about.
- The whole-run clock is now dominated by the agent's sizing calibration
  (34 min here, 36 min in run 15) rather than by the harness.

