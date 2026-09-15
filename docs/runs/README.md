# Runs ledger

Every scaling measurement this repository has made, one row per measured
pass, in one file: [`ledger.csv`](ledger.csv).

It exists so the numbers survive. They were spread across 23 harness suite
files, 32 tiny-proof files, 12 rate records, 29 run write-ups and 22
step logs, in four formats, and the early ones only existed as prose.

Regenerate after any new run:

```
python3 scripts/build-runs-ledger.py
```

The script reads every source listed below and overwrites `ledger.csv`.
It computes nothing: every value is copied from its source, percentages
converted to fractions, and a field the source does not record is left
empty — never estimated.

## What is in it

392 rows as of 2026-09-15.

| source_kind | rows | from | covers |
|---|---:|---|---|
| `suite.json` | 156 | `docs/skill-validation/**/suite.json` | clean-room runs 11–28 (not 22), the demo under the harness, the 2026-09-04 rig, harness-live-1, plan 12 phase 3 |
| `tinyproof` | 70 | `docs/skill-validation/**/*tinyproof*.json` | the same runs' tiny proofs, including refused and retried attempts; the only measurements for run 29, the 32,768-key demo arms, and the [2 KB payload](../skill-validation/payload-2k.md) experiment |
| `record` | 82 | `.claude/skills/prove-it-scales/harness/record/{run9,run10,plan12}-*.json` | runs 9 and 10 and plan 12 phase 1 — input rate per pass, nothing else |
| `transcribed` | 84 | [`transcribed.csv`](transcribed.csv) | clean-room runs 1–8 and steps 10–12, copied by hand from their tables and logs |

Every run is present except **clean-room run 22**, which produced no table.

## Columns

| column | meaning |
|---|---|
| `study` | `step`, `clean-room`, `rig`, `harness-live`, `plan-12`, `demo-under-harness`, `payload-2k` |
| `run` | run number, step, or results directory |
| `suite` | the suite or file within a run — `suite`, `tinyproof`, `tinyproof-attempt1`, `suite7-final`, `paired-rep2` |
| `source_kind` | where the row came from — see above |
| `variant` | what differs from the run's main suite: a broker cap in a squeeze test, a window length, an AWS instance |
| `cores` | CPU cap on the component under test; empty where nothing was capped |
| `parallelism` | degree of parallelism |
| `pass` | pass label (`p1-asc`, `p2-desc`, `sentinel`) or number; empty where the source publishes one value per case |
| `status` | harness verdict: `OK`, `REFUSED`, `CEILING`; `SATURATED` for step 10's live-load tests |
| `input_rate`, `input_unit` | records consumed per second at the source, and what a record is |
| `output_rate`, `output_unit` | records written per second at the sink |
| `elapsed_s` | measurement window, or drain time for step 10 and 11 |
| `tm_cores`, `tm_cap_frac`, `tm_throttled_pct` | CPU used by the worker, as cores and as a fraction of its cap; throttled periods |
| `kafka_cores`, `kafka_cap`, `kafka_cap_frac` | the same for the broker |
| `gc_frac`, `gc_ms` | garbage collection as a fraction of capacity, and milliseconds inside the window |
| `tm_memory_config` | the worker's **configured** process memory |
| `broker_limit_hits`, `broker_refaults`, `broker_file_cache_bytes`, `broker_limit_bytes` | the broker's cgroup memory counters |
| `busy_frac`, `backpressure_frac`, `source_idle` | the source task's time split; transcribed rows carry the published figure, often a peak |
| `max_task_backpressured` | the highest back-pressure fraction of any task in the job |
| `vantage_disagreement` | broker offsets against sink records |
| `headroom_s` | backlog left at window close, in seconds at the measured rate |
| `checkpoint_ms` | checkpoint interval |
| `build` | build hash of the job under test |
| `started_at` | UTC, from the suite or log |
| `source_file` | the file the row was copied from |
| `notes` | anything the row's numbers need to be read correctly |

## What is not recorded

Measured by column fill, not assumed:

| gap | where |
|---|---|
| **worker memory actually used** | nowhere, in any run. Only the configured size (`tm_memory_config`) and GC share exist |
| broker memory counters | `suite.json` from run 15 on; the broker's memory limit from run 23 on |
| CPU, GC, output rate, memory | not in `record` rows at all — runs 9 and 10 kept rates only |
| per-pass values | runs 1–8 publish one value per case; the ledger cannot recover the passes behind them |
| output rate | transcribed rows only where the source published it: runs 3, 4, 7, 8 and step 11 |
| checkpoint interval | not stored in tiny-proof files |
| client CPU for AWS step 11 at parallelism 8 and 16 | the logs do not state it, or the client instance |
| step 10's latency cases (`results.txt`) | not scaling measurements; left out |

## Reading it

Rates are only comparable within one `study` and `run`. Different runs built
different pipelines, with different fan-out, different key counts and
different record definitions — `input_unit` says which. A step ratio compares
two cases from the same suite and build; comparing across runs is comparing
different code.

`tm_cap_frac` below 0.95 means the worker was not the constraint and the case
cannot support a scaling claim. `status = CEILING` is a measurement of where
scaling stops, not an error.
