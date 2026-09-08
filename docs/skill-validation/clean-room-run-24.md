# Clean-room validation, run 24 — the first run judged on the claim

The harness now gates the claim as well as the measurement: each step ratio
carries an interval from its adjacent pairs, and `meetsClaim` is true only when
the **lower** bound clears 95% of linear. Replayed across the twelve recorded
runs, one clears it — run 12, the heaviest job per record.

This run is the first measured under that gate, and its prompt tells the agent
the claim must pass, so a shortfall is the agent's to fix in its own pipeline
rather than a number to report and move on from.

## What the record predicts

| | |
|---|---|
| true 2→4 on the last build measured | **1.849 ± 2.8%** (13 adjacent pairs, one hour, nothing changed) |
| what two passes resolve | ±3.9% at 95% |
| recorded runs whose interval clears 95% of linear | 1 of 12 |

So the expected outcome is a **fail on 2→4** unless the agent's pipeline is
materially different from the eight that came before it. What the run tests is
whether the diagnosis the harness prints — per-core rate, cap, source idle, GC
and back-pressure for both cases — is enough for an agent to act on.

## The criteria, written before launch

| criterion | note |
|---|---|
| chain passes on the first attempt | run 23 managed it |
| harness verbatim, one suite, 0 forbidden-path reads | every run |
| chain ≤ 60 min | 49.0 min (23), 44.1 (21) |
| every case carries a spread | two-pass quick mode |
| **2→4 meets the claim** — lower bound ≥ 95% of linear | 1 of 12 recorded runs would |
| 1→2 meets the claim | most recorded runs would |
| if the claim fails, the agent says what it changed and what that did | the point of the diagnosis block |

## Response

*(written after the run)*
