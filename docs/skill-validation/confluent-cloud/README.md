# Confluent Cloud probes: raw results

The tests behind [findings §8](../findings.md), run 2026-10-03 to 05 on Confluent
Cloud (GCP us-east1, a Basic Kafka cluster, Flink SQL compute pools), each on a
stack it created and deleted itself. Every log ends with the teardown's check
that nothing named `flink-training` survived, except where noted.

| file prefix | what it tested | script |
|---|---|---|
| 01 | one generator job writing at 5 and 10 CFU | `probes/01-fill-one-job.py` |
| 02 | one generator job, then two side by side, one stack | `probes/02-fill-one-and-two-jobs.py` |
| 03 | the first drain attempt; it stopped when Confluent would not lower a pool's size | `probes/04-drain.py` (earlier version) |
| 04 | drains at 5, 10 and 20 CFU, a new pool each, no baseline | `probes/04-drain.py` |
| 05 | drains in 20 CFU pools from 24- and 48-partition topics | `probes/05-partitions-24-and-48.py` |
| 06 | the same drain twice in 20 CFU pools: no baseline, then baseline 20 | `probes/06-baseline-cfu.py` |
| 07 | per-key sum and pass-through copy at 10 and 20 CFU, baseline = pool size | `probes/07-sum-and-copy.py` |
| 08 | the copy at 20 CFU with the Kafka cluster allowed 50 eCKU | `probes/08-ecku-50.py` |
| 09 | the copy at 20 CFU with 96 partitions in and out, 50 eCKU | `probes/09-partitions-96.py` |
| 10, 11 | asking the metrics API for parallelism, on a 1-CFU statement (10 stopped early: its new table never became visible) | `probes/11-parallelism-metrics.py` |
| 12, 13 | the copy at 20 CFU on 24 partitions, held running to read the Query Profiler (12 stopped early as 10 did; 13 stopped when its statement was deleted by hand, see §8) | `probes/13-profiler.py` |
| 14 | the same copy with watermark alignment's allowed drift raised to 1 day | `probes/14-alignment-drift-1d.py` |
| 15a, b, c | the copy at 20 CFU on 40 partitions through the harness's own checks: a unusable stack torn down by the readiness check, a stop at the baseline (`organization list`), then the full case | `probes/15-confirm.py` |
| 16a, b | the same case traced every 30 s from the output's log end, with phase, scaling status and exceptions (16a: unusable stack torn down) | `probes/16-dip-trace.py` |
| 17 | the copy at 10 and then 20 CFU on one stack and one fill | `probes/17-step-10-and-20.py` |
| 18a, b | run 17's step with the Kafka cluster allowed 50 eCKU, its eCKU count recorded (18a: unusable stack torn down) | `probes/18-step-ecku-50.py` |
| 19 | run 18 again, unchanged, on a new stack | `probes/19-step-ecku-50-repeat.py` |
| 20a, b | one 20 CFU case through the harness's own run_case_cloud, then a bounded COUNT over the REST results API (20a: stopped at readiness on a schema-registry error) | `probes/20-live-harness-case.py` |
| 21 | six stacks alternating the reused and fresh environment names, readiness timed; a bounded GROUP BY read through statement_rows on the first usable one | `probes/21-readiness-names-and-rows.py` |
| 22 | run 18 again, unchanged, on a new stack: a third reading of the 10→20 CFU step at 50 eCKU | `probes/22-step-ecku-50-third.py` |
| 23a, b, c | why the cloud chain's bounded count did not finish: the plain count for 40 minutes; its progress through the input, and a windowed count; three count forms on a known three-row table, then the snapshot query on the backlog | `probes/23a-count-plain-40-minutes.py`, `probes/23b-count-progress-and-window.py`, `probes/23c-count-forms.py` |
| 24 | the input counted four times after the fill, and by Kafka's own consumer committed-only and everything; the completeness drain repeated at 5 CFU and its output counted the same ways | `probes/24-probe.py`, `probes/24-consume.py`, `probes/24-drain.py` |

- `NN-*.log` — what the probe printed. Paths are shortened (`<scratch>`, `~`).
- `NN-*.record.json` — the per-minute readings it saved: Confluent's metrics API
  (CFU in use, records read, records waiting, busy and held-back time) and the
  Kafka cluster's bytes and eCKU count.
- `13-query-profiler-readings.txt` and `screens/` — what the Console's Query
  Profiler showed; no API returns these.
- `step-100m.py` — the rule behind runs 18, 19 and 22: the minutes after the statement first reaches its pool's size with at least 100 million records waiting. `python3 step-100m.py 18b-*.record.json 19-*.record.json 22-*.record.json`.
- `analyse.py` — the rule for the records in its format (runs 05–09 and 14): the minutes from the one
  after the statement first reaches its pool's size, leaving out the last
  reported minute, any minute that starts with nothing waiting, and the minute
  in which the backlog runs out. `python3 analyse.py 07-*.record.json`.

The scripts import the adapter from `<scalable-flink-skill>/harness` and read
the run's keys from an owner-only file the adapter writes and deletes; no key
is in any file here. The organization id in the REST calls is replaced with
`<organization-id>`.
