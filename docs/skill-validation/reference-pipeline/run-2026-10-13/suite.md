| field | value |
|---|---|
| axis | one machine, more cores |
| API level | DataStream |
| guarantee | state: exactly-once checkpointing; sink: at-least-once, idempotent (the absolute total and count per location and hour) |
| checkpoint interval | 10000 ms |
| build hash | `a2d319dcf0390a80` (completeness passed for `a2d319dcf0390a80`) |
| passes per case | 3 |
| study | scaling: every case configured identically |
| workload | 900,000,000 records, locationCount 100, sensorCount 1,000, sensorsPerLocation 10, recordBytes 103, hourMs 3,600,000, locationStaggerSeconds 36, partitions 8, closedWindowCount 24,999, readingsInClosedWindows 898,182,000 |
| rate source | committed broker offsets on `temperature-readings` |
| CPU source | cgroup `cpu.stat usage_usec` |
| suite span | 2026-10-13 08:15:39 -> 2026-10-13 08:56:30 EDT (41m)  dashboard: from=1791893739531&to=1791896190217 |

**1→2 cores: 1.87× (target 1.80×), range it is judged on 1.813x to 1.926x.**
**2→4 cores: 1.90× (target 1.80×), range it is judged on 1.877x to 1.899x.**

| cores | speed | scaling | pipeline CPU | pipeline memory | Kafka CPU | Kafka memory | blocked by | what to do |
|---:|---:|---:|---|---|---|---|---|---|
| | | what the step into it gave | cores it could use / how much it used | memory it could use / share of the time spent tidying memory up | cores Kafka could use / how much it used | memory Kafka could use / how many times it filled up | | |
| 1 | 838,207/s | — | 1 / 100% | 1536m / 1.2% | 2.5 / 1% | 5.5g, never full | Pipeline CPU | check it matches |
| 2 | 1,565,813/s | 1.87x | 2 / 98% | 2048m / 0.6% | 2.5 / 1% | 5.5g, full 985x | Pipeline CPU | add cores for more |
| 4 | 2,973,908/s | 1.90x | 4 / 100% | 3072m / 0.4% | 2.5 / 3% | 5.5g, full 2,123x | Pipeline CPU | add cores for more |

| cores | pass | records/s | tm cores | % of cap | throttled | broker cores | src idle | src BP | headroom | vantage |
|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | p1-asc | 850,810 | 0.99 | 99.5% | 100% | 0.02 / 2.5 | 10.0% | 0.0% | 900 s | 0.51% |
| 2 | p1-asc | 1,563,080 | 2.01 | 100.3% | 100% | 0.04 / 2.5 | 9.3% | 0.0% | 425 s | 0.52% |
| 4 | p1-asc | 2,949,774 | 3.98 | 99.6% | 100% | 0.07 / 2.5 | 14.8% | 0.0% | 153 s | 0.14% |
| 4 | p2-desc | 2,881,404 | 4.00 | 99.9% | 100% | 0.07 / 2.5 | 15.5% | 0.0% | 164 s | 0.17% |
| 2 | p2-desc | 1,514,529 | 1.96 | 97.7% | 100% | 0.03 / 2.5 | 9.5% | 0.0% | 448 s | 1.94% |
| 1 | p2-desc | 839,307 | 0.98 | 97.9% | 100% | 0.02 / 2.5 | 12.7% | 0.0% | 929 s | 1.73% |
| 1 | p3-asc | 860,943 | 1.00 | 99.7% | 100% | 0.02 / 2.5 | 12.0% | 0.0% | 895 s | 0.17% |
| 2 | p3-asc | 1,637,554 | 2.00 | 99.7% | 100% | 0.03 / 2.5 | 11.7% | 0.0% | 399 s | 0.09% |
| 4 | p3-asc | 3,090,546 | 4.01 | 100.2% | 100% | 0.07 / 2.5 | 16.8% | 0.0% | 141 s | 0.39% |
| 1 | sentinel | 801,767 | 1.00 | 99.6% | 100% | 0.02 / 2.5 | 10.1% | 0.0% | 970 s | 0.05% |
| 2 | settle-1 | 1,548,092 | 1.96 | 97.9% | 100% | 0.03 / 2.5 | 9.0% | 0.0% | 435 s | 1.97% |

| cores | passes | mean records/s | spread | reportable |
|---:|---:|---:|---:|---|
| 1 | 4 | 838,207 | 7.1% | yes |
| 2 | 4 | 1,565,813 | 7.9% | yes |
| 4 | 3 | 2,973,908 | 7.0% | yes |

Sentinel: the 1-core case first (850,810 rec/s) and last (801,767 rec/s), drift -5.9% across the suite; counted in that case's spread.

### The job graph that ran

Read off the running plan, not drawn. Every case ran this shape — a row whose shape differed would have been thrown out. 2 tasks, 5 operators. Each box is one Flink task. The operators inside a box are chained: they run in one thread, with no network hop between them. Arrows between boxes are how records cross from one task to another: keyBy is a hash shuffle that sends each key to one parallel instance, and broadcast sends every record to every instance. Rounded shapes are Kafka topics, outside Flink. Every task runs at the case's parallelism (1, 2 or 4). Partition and key counts come from the configuration and the run's own manifest; an interval on an output is what the build declared in design.every, not something the harness measured.

![Job graph](suite-graph.png)

```mermaid
flowchart LR
  subgraph t1["Task 1: 2 operators"]
    t1o0["Source: read temperature readings"]
    t1o1["parse reading"]
    t1o0 --> t1o1
  end
  subgraph t2["Task 2: 3 operators"]
    t2o0["hourly average by location"]
    t2o1["publish hourly average: Writer"]
    t2o0 --> t2o1
    t2o2["publish hourly average: Committer"]
    t2o1 --> t2o2
  end
  t1o1 -->|"keyBy hourly average by location, 100 keys"| t2o0
  in0(["temperature-readings<br/>8 partitions"])
  in0 --> t1o0
  out0(["hourly-average-by-location<br/>100 keys"])
  t2o1 --->|"every 1 hour"| out0
```

The same shape as the graph completeness drew (`graph.md`).
