# Clean-room validation, run 19 — the two-pass quick mode

Run 18's prompt again on the harness at #61, where `--quick` now measures each
case **twice** instead of once. Run 18 reported 2→4 = 1.539× from a single
pass; three passes of its own build read 1.678×, so the one-pass ratio was
8.3% low. This run tests whether two passes make the reported ratio stable.

## The criteria, written before launch

| criterion | the record |
|---|---|
| chain passes on the first attempt | 1 of the last 4 |
| harness verbatim, one suite, 0 forbidden-path reads | every run |
| chain ≤ **60 min** (raised from 45: two passes add ~12 min) | 36.1 min at one pass (18) |
| whole-run wall clock (reported) | 1 h 25 m (18), 1 h 37 m (16) |
| **every case carries a spread** | new — one pass could not |
| every case at 95–101% of cap | 96.0–99.8% (18) |
| no case refused for broker memory | 0 hits (16, 18); the guard caught the rig's 3 GiB config today |
| sentinel drift reported | +7.5% (18), −1.5% (17) |
| ratios (recorded, not judged) | 2→4 has read 1.54–1.95× at one pass; 1.678× at three passes on run 18's build |

**The consistency test**: this run's 2→4 and its pass-to-pass spread, against a
second run of the same prompt. Two runs whose ratios agree inside their spreads
is the bar; anything else is still inconsistent.

## Response

*(written after the run)*
