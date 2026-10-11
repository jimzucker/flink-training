| field | value |
|---|---|
| axis | one machine, more cores |
| API level | DataStream |
| guarantee | state: exactly-once checkpointing; sink: at-least-once, idempotent (the absolute total and count per location and hour) |
| checkpoint interval | 10000 ms |
| build hash | `8071c9e3f9dd9e9f` (completeness passed for `8071c9e3f9dd9e9f`) |
| passes per case | 3 |
| study | scaling: every case configured identically |
| workload | 900,000,000 records, locationCount 100, sensorCount 1,000, sensorsPerLocation 10, recordBytes 103, hourMs 3,600,000, locationStaggerSeconds 36, partitions 8, closedWindowCount 24,999, readingsInClosedWindows 898,182,000 |
| rate source | committed broker offsets on `temperature-readings` |
| CPU source | cgroup `cpu.stat usage_usec` |
| suite span | 2026-10-11 17:27:02 -> 2026-10-11 18:24:40 EDT (58m)  dashboard: from=1791754022629&to=1791757480072 |

**1→2 cores: 1.94× (target 1.80×), range 1.84–2.04× across passes.**
**2→4 cores: 1.84× (target 1.80×), range 1.76–1.95× across passes.**

| cores | speed | scaling | pipeline CPU | pipeline memory | Kafka CPU | Kafka memory | blocked by | what to do |
|---:|---:|---:|---|---|---|---|---|---|
| | | what the step into it gave | cores it could use / how much it used | memory it could use / share of the time spent tidying memory up | cores Kafka could use / how much it used | memory Kafka could use / how many times it filled up | | |
| 1 | 823,178/s | — | 1 / 100% | 1536m / 1.3% | 2.5 / 1% | 5.5g, full 58x | Pipeline CPU | check it matches |
| 2 | 1,596,840/s | 1.94x | 2 / 99% | 2048m / 0.6% | 2.5 / 1% | 5.5g, full 696x | Pipeline CPU | add cores for more |
| 4 | 2,933,062/s | 1.84x | 4 / 100% | 3072m / 0.4% | 2.5 / 3% | 5.5g, full 2,091x | Pipeline CPU | add cores for more |

| cores | pass | records/s | tm cores | % of cap | throttled | broker cores | src idle | src BP | headroom | vantage |
|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | p1-asc | 806,425 | 0.99 | 99.3% | 100% | 0.02 / 2.5 | 11.0% | 0.0% | 970 s | 0.21% |
| 2 | p1-asc | 1,640,826 | 2.00 | 100.2% | 100% | 0.04 / 2.5 | 10.3% | 0.0% | 398 s | 0.60% |
| 4 | p1-asc | 2,905,561 | 3.97 | 99.4% | 100% | 0.07 / 2.5 | 14.8% | 0.0% | 156 s | 0.52% |
| 4 | p2-desc | 2,882,004 | 3.98 | 99.4% | 100% | 0.07 / 2.5 | 17.0% | 0.0% | 161 s | 0.30% |
| 2 | p2-desc | 1,602,619 | 2.00 | 100.2% | 100% | 0.04 / 2.5 | 9.5% | 0.0% | 414 s | 0.35% |
| 1 | p2-desc | 844,996 | 1.00 | 99.6% | 100% | 0.02 / 2.5 | 12.2% | 0.0% | 921 s | 0.18% |
| 1 | p3-asc | 810,785 | 0.96 | 96.2% | 100% | 0.03 / 2.5 | 10.3% | 0.0% | 968 s | 3.41% |
| 2 | p3-asc | 1,601,479 | 1.99 | 99.3% | 100% | 0.03 / 2.5 | 9.8% | 0.0% | 415 s | 0.29% |
| 4 | p3-asc | 2,905,330 | 4.00 | 100.1% | 100% | 0.07 / 2.5 | 15.3% | 0.0% | 159 s | 0.41% |
| 1 | sentinel | 830,505 | 1.00 | 100.0% | 100% | 0.02 / 2.5 | 13.8% | 0.0% | 932 s | 0.71% |
| 2 | settle-1 | 1,558,663 | 2.00 | 100.2% | 100% | 0.04 / 2.5 | 8.6% | 0.0% | 419 s | 0.22% |
| 4 | settle-2 | 2,936,634 | 3.99 | 99.8% | 100% | 0.07 / 2.5 | 15.3% | 0.0% | 148 s | 0.43% |
| 2 | settle-3 | 1,580,614 | 1.99 | 99.4% | 100% | 0.03 / 2.5 | 9.8% | 0.0% | 419 s | 0.51% |
| 4 | settle-4 | 3,035,779 | 3.99 | 99.7% | 100% | 0.07 / 2.5 | 17.0% | 0.0% | 142 s | 0.13% |

| cores | passes | mean records/s | spread | reportable |
|---:|---:|---:|---:|---|
| 1 | 4 | 823,178 | 4.7% | yes |
| 2 | 5 | 1,596,840 | 5.1% | yes |
| 4 | 5 | 2,933,062 | 5.2% | yes |

Sentinel: the 1-core case first (806,425 rec/s) and last (830,505 rec/s), drift +2.9% across the suite; counted in that case's spread.

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
