# Clean-room validation, run 15 — the same quick look, with the broker guard

Run 14's prompt again — `prove.py all --quick`, cases 1, 2 and 4, one pass
each — on the harness at #54, which adds the guard that run 14 needed and did
not have: a case is refused if the broker hit its container memory limit
inside the measurement window.

Run 14 reported **2→4 = 1.35×**. That number was measured on a starved
broker; the diagnosis is in [rig-2026-09-05-broker.md](rig-2026-09-05-broker.md)
and the fix in #54. This run asks whether the skill, unaided, now either
avoids the trap or is refused by it — and what the step ratios read when the
worker really is the constraint.

## The criteria, written before launch

| criterion | what the record says |
|---|---|
| the chain passes on the first attempt — no phase rerun, no fallback, no wrapper | run 14 did |
| harness verbatim (`prove.py all --quick`), one suite, 0 forbidden-path reads | runs 11–14 |
| chain `preflight` → `report` ≤ 45 min | 37.1 min (run 14) |
| whole-run wall clock (reported, not judged) | 55.9 min (14), 2 h 05 m (13) |
| **no case refused for broker memory** — the agent sizes the broker so the guard never fires | run 14 shipped 2 GiB and its 4-core case was starved; the rig needed 4 GiB for a 264M backlog |
| every case at 95–101% of cap | 99.9 / 98.4 / 95.9 (14) |
| sentinel measured, drift reported | +2.3% (14) |
| the table is stamped unpublishable | `--quick` sets `quickLook` / `publishable: false` |
| ratios | **recorded, not judged** — one pass carries no spread. For context, the same build measured three passes per case with a fed broker read 2→4 = 1.725× |

## Response

*(written after the run)*
