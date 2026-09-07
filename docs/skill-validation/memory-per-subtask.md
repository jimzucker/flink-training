# The 2→4 shortfall was the memory contract

Every run from 1 to 19 gave the task manager a flat amount of memory,
whatever the case's parallelism. Flink divides that across the case's
subtasks, so the 4-core case ran on a quarter of what each 2-core subtask
had — and the largest case was measuring memory pressure rather than cores.
This is the measurement that found it and the fix that followed.

## The controlled comparison

Run 18's build, cap == parallelism, cases interleaved so drift cannot produce
the effect (the rig moved ±5% over the same window and the interleaving
cancels it):

| worker memory | 2 cores | 4 cores | 2→4 | GC at 4c |
|---|---:|---:|---:|---:|
| flat 2048m | 558,059 | 917,807 | **1.645** | 9.3% |
| 5g | 549,380 | 1,049,130 | **1.910** | 2.3% |

The 2-core figure moved −1.6%; the 4-core figure moved **+14.3%**. A change
that lifts only the starved case is the signature of a per-subtask resource.

## What it explains, and what it does not

It explains the run-to-run scatter in 2→4 (1.54–1.95× across runs 14–19) and
why the demo's own job never showed it: `scale-units.sh` runs one job per
unit rather than one worker growing, so its per-subtask memory never shrank.

It does not explain everything. Before this was found, three other
explanations were tested and dropped:

| candidate | verdict |
|---|---|
| broker starved of page cache | real, and a separate guard (#54) — it explained run 14's 1.35×, not the rest |
| records too light to amortise a fixed parallelism overhead | **refuted** by [run 17](clean-room-run-17.md): light records, 2.7% loss |
| checkpoint interval | untestable as designed — at 60 s the harness has too few commit boundaries to measure at all |
| GC alone | 5g against 2560m cut GC 6.3% → 2.7% and bought only 2.9%; the cliff is below 2560m |

## The fix

- **#62** — `caps.tmMemoryPerCore` is multiplied by the case's cores, and a flat
  `tmMemory` is refused when there is more than one case.
- **#63** — `caps.tmMemoryBase` covers what does not scale (metaspace, JVM
  overhead, the network buffer floor). Scaling the whole figure had starved the
  other end: at 1280m per core with no base the rig read GC **17.4%** at one
  core against 1.1% at four.

On the rig, `768m + 1280m per subtask` gives 2048m / 3328m / 5888m at 1 / 2 / 4:

| | flat | per-subtask | + base |
|---|---:|---:|---:|
| 1→2 | 1.99× | 2.205× | **2.125×** |
| 2→4 | 2.004× | 2.079× | **2.100×** |
| GC at 1c | — | 17.4% | 8.0% |
| GC at 4c | — | 1.1% | 1.2% |

## Still open

GC is 8.0% at one core against 1.2% at four even with the base term, so the
smallest case is still the least comfortable. It no longer distorts the ratios
on the rig, and no further tuning was done rather than tuning until the answer
looked right.
