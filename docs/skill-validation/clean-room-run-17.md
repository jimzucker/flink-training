# Clean-room validation, run 17 — the demo's workload, as a prediction test

[Why 2→4 falls short](why-2to4-falls-short.md) concluded that the skill's
1.8× step ratios and the demo's 1.99× are the same rig measuring two jobs of
different weight: the parallelism overhead is a fixed **0.70 µs per record**,
which is ~10% of the agents' 4.2–6.8 µs records and ~3% of the demo's 24.5 µs
ones.

That is a falsifiable claim, so this run tests it. The prompt asks for the
demo's workload — positions **and** market value at close on a ten-second
tumbling window, the specified window — instead of positions alone. Everything
else is run 16's setup: same harness (#57), `prove.py all --quick`, cases 1, 2
and 4, one pass each, fresh agent, empty directory.

## The prediction, written before launch

| | run 16 (positions only) | predicted here | falsified if |
|---|---:|---|---|
| CPU per record | 4.2–6.8 µs | **rises toward the demo's ~24 µs** | it stays under 8 µs |
| per-core rate | 146k–161k | **falls toward the demo's ~41k** | it stays above 100k |
| 2→4 | 1.809× | **≥ 1.90×** | it comes in below 1.85× while per-core rate is under 60k |

The model's own arithmetic: at 24 µs per record the 0.70 µs overhead costs
2.8%, so 2→4 should read about 1.94×. Between 1.90× and 2.0× confirms it;
under 1.85× on a heavy pipeline refutes it and the explanation goes back to
being unexplained.

## The other criteria, unchanged from run 16

| criterion | run 16 |
|---|---|
| chain passes on the first attempt | PASS |
| harness verbatim, one suite, 0 forbidden-path reads | 77 tool calls, 0 |
| chain ≤ 45 min | 35.6 min |
| whole-run wall clock (reported) | 1 h 37 m |
| no case refused for broker memory | 0 hits |
| every case at 95–101% of cap | 99.6 / 96.3 / 97.9% |
| sentinel measured | +4.1% |
| table stamped unpublishable | `quickLook` / `publishable: false` |

## Response

*(written after the run)*
