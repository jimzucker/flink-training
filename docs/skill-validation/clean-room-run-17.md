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

**The prediction's ratio came true and its mechanism did not.** Model: Claude
Opus 5. Harness at #57, `prove.py all --quick`, 187 tool calls, 0
forbidden-path reads, 11:14 → 13:07. Raw results in [run-17/](run-17/).

| prediction | measured | verdict |
|---|---|---|
| CPU per record rises toward the demo's ~24 µs | **6.41 / 4.67 / 4.79 µs** at 1 / 2 / 4 cores | **refuted** — no heavier than run 16's job |
| per-core rate falls toward ~41k | 157,216 / 213,790 / 208,058 | **refuted** |
| 2→4 ≥ 1.90× | **1.946×** | met |

| cores | records/s | per core | % of cap | µs CPU per record | src idle | GC |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 157,216 | 157,216 | 100.0% | 6.41 | 0.3% | 6.0% |
| 2 | 427,580 | 213,790 | 99.7% | 4.67 | 1.8% | 6.2% |
| 4 | 832,233 | 208,058 | 99.5% | 4.79 | 4.7% | 6.1% |
| 1 (sentinel) | 154,919 | | 100.2% | | 0.1% | 4.9% |

1→2 = 2.740× (137% of linear) and 2→4 = 1.946×, one pass each, table stamped
`quickLook` / `publishable: false`. Four cases, no suite refusals, zero broker
memory-limit hits, sentinel −1.5%.

## What this does to the explanation in [why-2to4-falls-short](why-2to4-falls-short.md)

That page argued the demo scales better because its records are heavier: a
fixed ~0.70 µs parallelism overhead is ~3% of a 24 µs record and ~10% of a
5 µs one. This run refutes it. Its records cost **4.79 µs at four cores** —
lighter than run 16's — and it still lost only **2.7%** per core from 2 to 4,
where run 16 lost 9.6%.

| pipeline | µs CPU per record at 4c | per-core loss 2c → 4c | 2→4 |
|---|---:|---:|---:|
| demo (`scale-units.sh`) | 24.5 | 0.0% | 1.99× |
| run 17 (this one) | 4.79 | 2.7% | 1.946× |
| run 16 | ~6.8 | 9.6% | 1.809× |
| run 14 (broker fed) | ~6.3 | 10.3% | 1.725× |

Record weight does not order that column. The parallelism penalty is
**job-specific and not predicted by CPU per record**, and what does predict it
is not established.

One structural difference is on record and untested as a cause: run 16's job
is three vertices with the source chained into `allocate`; run 17's is four,
with a second source for prices and the windowed close, and its shuffle
carries per-window state rather than per-record updates alone.

## Also measured, and not explained

The 1-core case costs **37% more CPU per record** than the 2-core one (6.41 vs
4.67 µs), which is the whole of the superlinear 1→2 = 2.74×. The agent ruled
out starvation (source idle 0.3%, zero broker limit hits), the cap (fully
consumed) and GC (6.0 / 6.2 / 6.1%, flat), and named no cause. Neither do we.

## What the run cost, and what the gates caught

1 h 53 m end to end, of which **two thirds was rework the gates demanded** and
33.1 min was the passing chain (completeness 442 s, tiny proof 442 s, fill
60 s, suite 975 s). The chain failed once at the tiny proof and passed on the
second attempt.

Four defects in the agent's own job, each caught by a gate before any table
was produced:

1. 1,024 (account, symbol) pairs against a predicted 16,384 — accounts tied to
   four symbols because 256 is a multiple of 64.
2. 12,451 wrong market values — the close used the *running* position at timer
   time, which includes trades past the window boundary.
3. 19 of 39 windows produced no market value in the killed-worker arm: the
   restored price source had already consumed its topic, emitted nothing, and
   so never advanced its watermark. Diagnosed with an instrumented repeat, not
   guessed, and fixed with `withIdleness`.
4. The broker-memory guard from #54 fired at 4 GiB with **841 limit hits**; the
   agent raised the broker to 6 GiB and every suite case then recorded zero.
   That is the guard doing exactly what it was added for, on a pipeline that
   did not exist when it was written.

