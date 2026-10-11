# Hourly temperature average by location

The pipeline the reference run builds and measures. `run-reference.sh` copies
this file into the run directory as `README.md`, where the harness's preflight
checks it names every topic and operator. This directory's own `README.md`
is about the reference run itself.

## What it does

Temperature readings come in; one row per location per hour goes out,
carrying the total and the count, not a rounded average (ANSWERS.md, question
4). The hour comes from the timestamp inside the reading, never from the
machine clock. 100 locations, 10 sensors each. Each reading adds to its
location's open hour. When a reading for a later hour arrives, the open hour
is published once and the new one opened; a reading for an hour already
published is dropped. So the output grows with the hours covered, not with
the number of readings.

## Topics

| topic | what it is | key | value | partitions | written by | in `pipeline.json` |
|---|---|---|---|---|---|---|
| `temperature-readings` | the input, filled and measured by the harness | location | one reading: sensor, location, timestamp, temperature | 8 (`partitions`); a location's readings all go to partition location mod 8 | the generator, `st44.Gen` | `topics.in`, `design.inputs` |
| `hourly-average-by-location` | the output | location | JSON: `location`, `hourStart`, `totalC`, `count` | created by the harness | the job | `topicsAlsoWritten`, `design.outputs`; one row per location per hour (`design.every`) |

## Record formats

One real record off each topic, read by the harness after completeness:
[`results/records.md`](results/records.md). Not typed here, so it cannot go
stale.

## How the job is built

`st44.TempJob`, DataStream, exactly-once checkpoints every 10 s, an
at-least-once Kafka sink made idempotent by publishing the absolute total and
count.

| operator | what it does |
|---|---|
| read temperature readings | the Kafka source on `temperature-readings` |
| parse reading | turns the bytes into a reading |
| hourly average by location | keyed by location; a `KeyedProcessFunction` holding the current hour's total and count |
| publish hourly average | the Kafka sink to `hourly-average-by-location` |

No watermarks and no timers: a reading in a later hour closes the earlier one,
so the last hour of each location stays open and is never published.

## Correctness

`st44.Verify` compares the output with the generator's manifest, with no
tolerances.

- **Clean run:** nothing is published twice, and each location's published
  hour strictly increases.
- **Killed and restarted run:** a location may step back once, and must then
  reach the same values. Duplicates are allowed, and every copy must be
  identical byte for byte.

One section 4 assertion does not apply: "two paths over the same input agree
exactly" needs two aggregations, and this pipeline has one (ASSUMPTIONS.md
B9).

## How to build and run it

```
./run-reference.sh /path/to/a/new/run/directory
```

It copies the job, builds `job/target/st44.jar` with `mvn -DskipTests
package`, and runs `prove.py all`: up, preflight, completeness, the short
proof, fill, suite, report.

## Results

- Headline numbers: [`results/headline.md`](results/headline.md), generated
  by `prove.py report`.
- The job graph, read off the running plan during completeness:
  [`results/graph.md`](results/graph.md).
- The expected outcome of a reference run: this directory's `README.md`.
