# Confluent Cloud probes: raw results

The tests behind [findings §8](../findings.md), run 2026-10-03/04 on Confluent
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

- `NN-*.log` — what the probe printed. Paths are shortened (`<scratch>`, `~`).
- `NN-*.record.json` — the per-minute readings it saved: Confluent's metrics API
  (CFU in use, records read, records waiting, busy and held-back time) and the
  Kafka cluster's bytes and eCKU count.
- `13-query-profiler-readings.txt` and `screens/` — what the Console's Query
  Profiler showed; no API returns these.
- `analyse.py` — the one rule every rate in §8 uses: the minutes from the one
  after the statement first reaches its pool's size, leaving out the last
  reported minute, any minute that starts with nothing waiting, and the minute
  in which the backlog runs out. `python3 analyse.py 07-*.record.json`.

The scripts import the adapter from `<scalable-flink-skill>/harness` and read
the run's keys from an owner-only file the adapter writes and deletes; no key
is in any file here. The organization id in the REST calls is replaced with
`<organization-id>`.
