| field | value |
|---|---|
| axis | one machine, more cores |
| API level | DataStream |
| guarantee | state: exactly-once checkpointing; sink: at-least-once, idempotent (the absolute total and count per location and hour) |
| checkpoint interval | 10000 ms |
| build hash | `da70e36c1e600ecd` (completeness passed for `da70e36c1e600ecd`) |
| passes per case | 3 |
| study | scaling: every case configured identically |
| workload | 900,000,000 records, locationCount 100, sensorCount 1,000, sensorsPerLocation 10, recordBytes 103, hourMs 3,600,000, locationStaggerSeconds 36, partitions 8, closedWindowCount 24,999, readingsInClosedWindows 898,182,000 |
| rate source | committed broker offsets on `temperature-readings` |
| CPU source | cgroup `cpu.stat usage_usec` |
| suite span | 2026-10-18 12:06:30 -> 2026-10-18 12:47:52 EDT (41m)  dashboard: from=1792339590991&to=1792342072867 |

**1→2 cores: 1.87× (target 1.80×), range it is judged on 1.795x to 1.906x.**
**2→4 cores: 1.86× (target 1.80×), range it is judged on 1.813x to 1.936x.**

| cores | speed | scaling | pipeline CPU | pipeline memory | Kafka CPU | Kafka memory | blocked by | what to do |
|---:|---:|---:|---|---|---|---|---|---|
| | | what the step into it gave | cores it could use / how much it used | memory it could use / share of the time spent tidying memory up | cores Kafka could use / how much it used | memory Kafka could use / how many times it filled up | | |
| 1 | 832,922/s | — | 1 / 100% | 1536m / 1.2% | 2.5 / 1% | 5.5g, never full | Pipeline CPU | check it matches |
| 2 | 1,557,917/s | 1.87x | 2 / 100% | 2048m / 0.8% | 2.5 / 2% | 5.5g, full 1,050x | Pipeline CPU | add cores for more |
| 4 | 2,904,055/s | 1.86x | 4 / 100% | 3072m / 0.4% | 2.5 / 4% | 5.5g, full 1,980x | Pipeline CPU | add cores for more |

| cores | pass | records/s | tm cores | % of cap | throttled | broker cores | src idle | src BP | headroom | vantage |
|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | p1-asc | 859,693 | 1.00 | 100.0% | 100% | 0.03 / 2.5 | 11.6% | 0.0% | 887 s | 0.16% |
| 2 | p1-asc | 1,576,450 | 1.99 | 99.7% | 100% | 0.05 / 2.5 | 11.2% | 0.0% | 412 s | 0.67% |
| 4 | p1-asc | 2,840,766 | 3.98 | 99.4% | 100% | 0.10 / 2.5 | 14.9% | 0.0% | 155 s | 1.05% |
| 4 | p2-desc **(ceiling)** | 2,796,689 | 3.79 | 94.7% | 100% | 0.09 / 2.5 | 14.9% | 0.0% | 178 s | 4.62% |
| 2 | p2-desc | 1,488,477 | 2.00 | 99.7% | 100% | 0.05 / 2.5 | 9.2% | 0.0% | 454 s | 0.07% |
| 1 | p2-desc | 812,862 | 0.97 | 97.0% | 100% | 0.04 / 2.5 | 14.1% | 0.0% | 963 s | 2.80% |
| 1 | p3-asc | 811,044 | 1.00 | 99.9% | 100% | 0.02 / 2.5 | 12.9% | 0.0% | 955 s | 0.37% |
| 2 | p3-asc | 1,583,171 | 2.00 | 99.8% | 100% | 0.04 / 2.5 | 10.1% | 0.0% | 412 s | 0.55% |
| 4 | p3-asc | 2,967,344 | 4.00 | 100.0% | 100% | 0.09 / 2.5 | 15.6% | 0.0% | 154 s | 0.21% |
| 1 | sentinel | 848,091 | 1.00 | 99.9% | 100% | 0.03 / 2.5 | 11.0% | 0.0% | 911 s | 0.36% |
| 2 | settle-1 | 1,583,572 | 2.00 | 99.9% | 100% | 0.05 / 2.5 | 10.5% | 0.0% | 420 s | 0.08% |

| cores | passes | mean records/s | spread | reportable |
|---:|---:|---:|---:|---|
| 1 | 4 | 832,922 | 5.8% | yes |
| 2 | 4 | 1,557,917 | 6.1% | yes |
| 4 | 2 | 2,904,055 | 4.4% | yes |

Sentinel: the 1-core case first (859,693 rec/s) and last (848,091 rec/s), drift -1.4% across the suite; counted in that case's spread.

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
