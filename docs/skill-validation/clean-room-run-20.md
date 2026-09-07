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

**Bar met.** Opus 5, harness at #63, `prove.py all --quick`, 0 forbidden-path
reads, 11:22 → 13:22. Raw results in [run-20/](run-20/).

| criterion | result |
|---|---|
| **2→4 ≥ 1.90× with every spread under 10%** | **PASS — 1.930×** (1.901–1.958), spreads 1.1% / 1.9% / 4.4% |
| chain ≤ 60 min | PASS — 49.5 min (up 18 s, preflight 63, completeness 456, tinyproof 504, fill 383, suite 1543) |
| chain passes on the first attempt | FAIL — two attempts thrown away first, both on broker memory and backlog sizing |
| harness verbatim, one suite, 0 forbidden reads | PASS |
| every case at 95–101% of cap | PASS — 97.2 / 100.0 / 99.8% |
| preflight reports memory per subtask | PASS — the agent chose 768m base + 640m per subtask |
| every case carries a spread | PASS |
| sentinel drift | +4.0% |
| 1→2 within 1.90–2.30× | PASS — 2.228×, but superlinear and unexplained |

| cores | records/s | spread | % of cap | GC |
|---:|---:|---:|---:|---:|
| 1 | 183,812 | 4.4% | 97.2% | 8.6% |
| 2 | 409,534 | 1.9% | 100.0% | 3.7% |
| 4 | 790,259 | 1.1% | 99.8% | 1.6% |

## Against the record

| | 2→4 | spread at 4c |
|---|---:|---:|
| runs 14–19, flat memory | 1.35–1.95× | not measured (one pass) |
| run 18's build re-measured, flat 2048m | 1.645× | 3.6% |
| the same build with memory it needed | 1.910× | — |
| rig after the fix | 2.100× | 2.8% |
| **run 20, agent's own pipeline** | **1.930×** | **1.1%** |

## What the agent found that we had not

- **The `--quick` banner was stale.** It still said "one pass per case. No
  spread" after #61 made it two. Fixed here; the agent was right to quote it
  and flag it rather than paraphrase.
- **The binding constraint on this host is the 8.2 GB Docker VM, not CPU.**
  Its broker cache guard refused three attempts; the cache grew to fill
  whatever cap it was given (2.20 GB at 3 GiB, 2.86 GB at 3.5 GiB) and still
  hit it, and paying for the cap out of the worker drove 1-core GC from 4.9%
  to 10.8%. It passed by setting the broker cap above what the VM can supply,
  so `brokerLimitHits` reads zero partly because that cap is no longer what
  binds — it said so plainly and gave the refault counts (436k–442k pages read
  back from disk inside the 4-core windows) as the cross-check.
- It could not raise the VM's memory: the sandbox blocked the settings write.

## Measured, not explained

1→2 = 2.228× is superlinear and stays unexplained. GC runs 8.6 / 3.7 / 1.6%
across the cases, which is the shape the base term was added to reduce and did
not remove; the agent ruled out the broker, a starved source, graph shape,
vantage disagreement and drift, and offered no cause. 2→4 is the number with
no baseline in it and the tightest spreads.

