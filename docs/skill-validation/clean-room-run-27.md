# Clean-room validation, run 27 — the demo's whole job, not half of it

Every run since 17 asked for positions only. The demo does more: positions
**and** market value at close on a ten-second tumbling window, with the price
taken at the window boundary, and correctness proved exactly. That is the
workload the skill is supposed to be able to produce, and comparing a
positions-only pipeline against the demo's numbers was comparing two different
jobs — the demo spends ~24 µs of CPU per record, the positions-only runs ~7 µs.

This run asks for the whole thing, on the harness at #83.

## What the demo does, measured on `main` today

| units | orders/sec | vs previous | Flink cores |
|---:|---:|---|---:|
| 1 | 35,399 | — | 1.00 |
| 2 | 80,974 | 2.29× | 2.00 |
| 4 | 168,200 | 2.08× | 3.98 |

One pass per unit, its own script, no spread — the harness's stricter view of
the same machine is that a two-pass ratio carries about ±4%.

## The criteria, written before launch

| criterion | note |
|---|---|
| the job does both halves: positions by symbol and account, **and** market value at close on a 10 s tumbling window, price at the boundary | the demo's requirement |
| completeness passes with no tolerances, clean and with a worker killed mid-drain | including the windowed outputs |
| chain passes on the first attempt | run 26 managed it |
| chain ≤ 60 min | 46.0 min (26) |
| every case at 95–101% of cap, GC under the 11% ceiling | the guards, unchanged |
| **2→4 meets the claim** — lower bound ≥ 95% of linear | run 17, the last full-workload run, read 1.946× point estimate on the old harness |
| 1→2 meets the claim | |
| CPU µs per record reported per case | the number that separates this from the positions-only runs |

## Response

*(written after the run)*
