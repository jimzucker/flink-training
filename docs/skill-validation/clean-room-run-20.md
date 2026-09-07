# Clean-room validation, run 20 — after the memory fix

The 2→4 shortfall was the harness's own memory contract. A flat worker memory
divides across each case's subtasks, so the 4-core case ran on a quarter of
what each 2-core subtask had and measured memory pressure instead of cores.
Measured on run 18's build, interleaved: flat 2048m read 2→4 = **1.645** with
GC at 9.3%; per-subtask memory read **1.910** with GC at 2.3%, and the 2-core
figure did not move. #62 made memory per subtask; #63 added the fixed base
term after per-core scaling starved the 1-core case (GC 17.4%).

On the rig, `768m base + 1280m per subtask`:

| | before (flat) | after |
|---|---:|---:|
| 1→2 | 1.99× | **2.125×** |
| 2→4 | 2.004× | **2.100×** |
| GC at 1c | — | 8.0% |
| GC at 4c | — | 1.2% |

This run asks whether an agent following the skill gets there unaided.

## The criteria, written before launch

| criterion | the record |
|---|---|
| chain passes on the first attempt | 1 of the last 5 |
| harness verbatim, one suite, 0 forbidden-path reads | every run |
| chain ≤ 60 min | 51.1 (19), 52.0 and 51.8 on the rig |
| preflight reports memory per subtask | new in #63 |
| every case at 95–101% of cap | 94.1–100.3% (19) |
| **2→4 ≥ 1.90×** | 1.54–1.78× before the fix; 2.10× on the rig after it |
| 1→2 within 1.90–2.30× | 2.06–2.74× before |
| every case carries a spread | two-pass quick mode (#61) |
| sentinel drift reported | +11.2% (19) |

**The bar**: 2→4 ≥ 1.90× with every case's spread under 10%. Below that, the
memory fix did not carry to a pipeline the agent wrote itself.

## Response

*(written after the run)*
