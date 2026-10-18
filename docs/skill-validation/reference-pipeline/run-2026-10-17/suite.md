| field | value |
|---|---|
| axis | one machine, more cores |
| API level | DataStream |
| guarantee | state: exactly-once checkpointing; sink: at-least-once, idempotent (the absolute total and count per location and hour) |
| checkpoint interval | 10000 ms |
| build hash | `8ada16a74ff53772` (completeness passed for `8ada16a74ff53772`) |
| passes per case | 3 |
| study | scaling: every case configured identically |
| workload | 900,000,000 records, locationCount 100, sensorCount 1,000, sensorsPerLocation 10, recordBytes 103, hourMs 3,600,000, locationStaggerSeconds 36, partitions 8, closedWindowCount 24,999, readingsInClosedWindows 898,182,000 |
| rate source | committed broker offsets on `temperature-readings` |
| CPU source | cgroup `cpu.stat usage_usec` |
| suite span | 2026-10-17 18:19:24 -> 2026-10-17 19:01:25 EDT (42m)  dashboard: from=1792275564172&to=1792278085706 |

**1→2 cores: not reported — the 2-core case does not count: its readings are 29.4% apart, over the 20% limit.**
**2→4 cores: not reported — the 2-core case does not count: its readings are 29.4% apart, over the 20% limit.**

| cores | speed | scaling | pipeline CPU | pipeline memory | Kafka CPU | Kafka memory | blocked by | what to do |
|---:|---:|---:|---|---|---|---|---|---|
| | | what the step into it gave | cores it could use / how much it used | memory it could use / share of the time spent tidying memory up | cores Kafka could use / how much it used | memory Kafka could use / how many times it filled up | | |
| 1 | 773,584/s | — | 1 / 100% | 1536m / 1.4% | 2.5 / 2% | 5.5g, never full | Pipeline CPU | check it matches |
| 2 \* | 1,425,094/s | — | 2 / 95% | 2048m / 0.7% | 2.5 / 3% | 5.5g, full 531x | Pipeline CPU | no usable step |
| 4 | 2,822,201/s | — | 4 / 99% | 3072m / 0.3% | 2.5 / 5% | 5.5g, full 1,799x | Pipeline CPU | no usable step |

| cores | pass | records/s | tm cores | % of cap | throttled | broker cores | src idle | src BP | headroom | vantage |
|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | p1-asc | 801,547 | 1.00 | 99.9% | 100% | 0.04 / 2.5 | 13.6% | 0.0% | 973 s | 0.12% |
| 2 | p1-asc | 1,490,756 | 1.96 | 97.9% | 100% | 0.06 / 2.5 | 10.2% | 0.0% | 457 s | 1.72% |
| 4 | p1-asc | 2,932,640 | 3.81 | 95.4% | 100% | 0.10 / 2.5 | 15.4% | 0.0% | 162 s | 4.34% |
| 4 | p2-desc | 2,865,576 | 3.98 | 99.5% | 100% | 0.10 / 2.5 | 15.7% | 0.0% | 167 s | 0.22% |
| 2 | p2-desc | 1,532,630 | 1.99 | 99.2% | 100% | 0.05 / 2.5 | 9.5% | 0.0% | 440 s | 0.55% |
| 1 | p2-desc | 804,828 | 0.96 | 96.4% | 100% | 0.03 / 2.5 | 12.9% | 0.0% | 976 s | 2.62% |
| 1 | p3-asc | 808,037 | 0.99 | 99.4% | 100% | 0.03 / 2.5 | 12.4% | 0.0% | 968 s | 0.16% |
| 2 | p3-asc | 1,548,117 | 1.99 | 99.7% | 100% | 0.04 / 2.5 | 8.9% | 0.0% | 431 s | 0.08% |
| 4 | p3-asc | 2,668,388 | 3.98 | 99.4% | 98% | 0.13 / 2.5 | 16.6% | 0.0% | 183 s | 1.31% |
| 1 | sentinel | 679,923 | 1.00 | 100.4% | 100% | 0.04 / 2.5 | 9.9% | 0.0% | 1168 s | 0.45% |
| 2 | settle-1 | 1,128,874 | 1.90 | 95.2% | 100% | 0.09 / 2.5 | 12.1% | 0.0% | 642 s | 0.72% |

| cores | passes | mean records/s | spread | reportable |
|---:|---:|---:|---:|---|
| 1 | 4 | 773,584 | 16.6% | yes |
| 2 | 4 | 1,425,094 | 29.4% | no — its readings are 29.4% apart, over the 20% limit |
| 4 | 3 | 2,822,201 | 9.4% | yes |

Sentinel: the 1-core case first (801,547 rec/s) and last (679,923 rec/s), drift -16.4% across the suite; counted in that case's spread.

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
