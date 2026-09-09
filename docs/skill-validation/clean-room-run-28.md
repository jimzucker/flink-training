# Clean-room validation, run 28 — the full project again, to see if it repeats

[Run 27](clean-room-run-27.md) built the whole job — positions *and* market
value at close on a ten-second window — proved it exactly under a killed
worker, and read 2→4 = **1.853×** [1.824, 1.881], missing the claim at 91.2%
of linear on the lower bound. That is one measurement of the real workload.

This run repeats it, on the harness at #85, with the example now shipping no
worker memory keys so an uncapped default is what a new pipeline inherits.

## The criteria, written before launch

| criterion | run 27 |
|---|---|
| the job does both halves, with the windowed outputs proved exactly under a killed worker | 16 assertions, no tolerances |
| chain passes on the first attempt | yes, 47.1 min |
| chain ≤ 60 min | 47.1 min |
| every case at 95–101% of cap, GC under 11% | 99.6–100.2%, GC 2.9–5.9% |
| **2→4 meets the claim** — lower bound ≥ 95% of linear | missed at 91.2% |
| 1→2 meets the claim | met at 120% |
| **the two runs agree inside their intervals** | 2→4 interval was [1.824, 1.881] |

**The question**: is 1.853× a property of this workload on this machine, or one
run's luck? Run 27's own sentinel drifted +7.8% and its order effect ran
1.07–1.09, so the interval may be narrower than the truth.

## Response

*(written after the run)*
