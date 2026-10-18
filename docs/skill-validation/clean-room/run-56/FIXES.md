# Fixes

Every change made to chase a number, each with its prediction written before
it was measured (SKILL.md §6). Rig fixes that did not chase a number (the
prices source's parallelism, the backlog size the harness self-test needed)
are in `JOURNAL.md`, not here.

## 1. Re-measure the suite on the same build, nothing changed (not a §6a lever)

**What and why.** Chain 3's suite (build e35f29848e13) had no scaling result:
the 2-core and 4-core cases spread 24.4% and 28.8%, and the report tied the
slow third passes to the host having 2.7–3.9 GB of its memory moved out to
disk, against 1.5–1.8 GB in the fast passes. The pipeline was not changed; this
is the harness's own advice ("Close what else is using memory and measure
again"). I cannot close the user's programs (no human; they are not mine), so
this re-run is on the same machine state, swap at 4.6 GB before it starts.

**Prediction, written 15:31 before measuring.** From chain 3's two clean
passes: 1→2 about 1.90× (233,598 / 122,940 and 227,223 / 121,436 →
1.90, 1.87), 2→4 about 1.72–1.81× (421,902 / 233,598 = 1.806; 391,822 /
227,223 = 1.724). If the host's memory holds steady, every case's spread
comes in under 20% and 1→2 is met; 2→4 is likely **not met**, or its range
straddles 1.80× and the suite runs its extra cases. If the swap moves again
during a pass, a case will again spread over 20% and there will be no result —
that would say the machine, not the pipeline, decides this table.

**Result (16:13).** The second branch of the prediction: no result again.
Every case pinned at its cap; the cases read slow exactly when the host had
more memory moved out to disk, as the report's own lines show:

| cores | passes, records/s | spread | slow passes taken at | fast passes taken at |
|---|---|---:|---|---|
| 1 | 103,820 / 116,469 / 125,258 (thrown out: 95.0% of cap) / sentinel 128,193 | 21.0% | 4.4–4.6 GB swapped out | 1.6–2.5 GB |
| 2 | 195,952 / 184,726 / 220,085 | 17.7% | — | — |
| 4 | 301,274 / 323,899 / 400,108 | 28.9% | 4.6–5.1 GB | 1.6 GB |

Compared with chain 3 (same build, same backlog): its fast passes were
1c 122,940 / 121,436 / 121,815, 2c 233,598 / 227,223, 4c 421,902 / 391,822.
The pipeline was not changed, so nothing is reverted. I did not run a third
suite: the swap was already 4.6 GB before this one started, the programs
holding the memory are the user's, and with nobody to ask I cannot close them.
What decides this table on this machine today is the host's memory, not the
pipeline. That is an observation from the harness's own swap readings at each
window, not a measured cause: no run here held the swap still and changed it.
