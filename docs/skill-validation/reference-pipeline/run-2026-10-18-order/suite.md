| field | value |
|---|---|
| axis | one machine, more cores |
| API level | DataStream |
| guarantee | state: exactly-once checkpointing; sink: at-least-once, idempotent (the absolute total and count per location and hour) |
| checkpoint interval | 10000 ms |
| build hash | `16a07060336b9172` (completeness passed for `16a07060336b9172`) |
| passes per case | 3 |
| study | scaling: every case configured identically |
| workload | 900,000,000 records, locationCount 100, sensorCount 1,000, sensorsPerLocation 10, recordBytes 103, hourMs 3,600,000, locationStaggerSeconds 36, partitions 8, closedWindowCount 24,999, readingsInClosedWindows 898,182,000 |
| rate source | committed broker offsets on `temperature-readings` |
| CPU source | cgroup `cpu.stat usage_usec` |
| suite span | 2026-10-18 17:22:08 -> 2026-10-18 18:22:48 EDT (1h 01m)  dashboard: from=1792358528636&to=1792362168521 |

**1→2 cores: 1.79× (target 1.80×), range it is judged on 1.759x to 1.849x.**
**2→4 cores: 1.88× (target 1.80×), range it is judged on 1.857x to 2.108x.**

| cores | speed | scaling | pipeline CPU | pipeline memory | Kafka CPU | Kafka memory | blocked by | what to do |
|---:|---:|---:|---|---|---|---|---|---|
| | | what the step into it gave | cores it could use / how much it used | memory it could use / share of the time spent tidying memory up | cores Kafka could use / how much it used | memory Kafka could use / how many times it filled up | | |
| 1 | 844,045/s | — | 1 / 99% | 1536m / 1.7% | 2.5 / 1% | 5.5g, never full | Pipeline CPU | check it matches |
| 2 | 1,507,136/s | 1.79x | 2 / 100% | 2048m / 0.5% | 2.5 / 2% | 5.5g, never full | Pipeline CPU | more passes |
| 4 | 2,836,615/s | 1.88x | 4 / 99% | 3072m / 0.3% | 2.5 / 7% | 5.5g, full 1,660x | Pipeline CPU | add cores for more |

| cores | pass | records/s | tm cores | % of cap | throttled | broker cores | src idle | src BP | headroom | vantage |
|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 4 | p1-desc | 2,978,430 | 3.98 | 99.4% | 100% | 0.09 / 2.5 | 15.6% | 0.0% | 156 s | 0.26% |
| 2 | p1-desc | 1,502,310 | 1.98 | 98.9% | 100% | 0.05 / 2.5 | 8.5% | 0.0% | 452 s | 0.86% |
| 1 | p1-desc | 826,533 | 0.99 | 99.4% | 100% | 0.03 / 2.5 | 13.1% | 0.0% | 936 s | 0.55% |
| 1 | p2-asc | 848,918 | 0.97 | 96.7% | 100% | 0.03 / 2.5 | 12.6% | 0.0% | 909 s | 3.86% |
| 2 | p2-asc | 1,568,412 | 2.00 | 100.0% | 100% | 0.04 / 2.5 | 11.0% | 0.0% | 424 s | 0.10% |
| 4 | p2-asc | 2,946,208 | 3.80 | 95.1% | 100% | 0.10 / 2.5 | 16.5% | 0.0% | 159 s | 4.19% |
| 4 | p3-desc | 2,970,848 | 3.99 | 99.8% | 100% | 0.10 / 2.5 | 16.3% | 0.0% | 156 s | 0.26% |
| 2 | p3-desc | 1,415,059 | 1.95 | 97.2% | 100% | 0.06 / 2.5 | 10.5% | 0.0% | 485 s | 1.47% |
| 1 | p3-desc | 858,152 | 1.00 | 100.4% | 100% | 0.03 / 2.5 | 11.1% | 0.0% | 902 s | 1.17% |
| 4 | sentinel | 2,450,975 | 3.96 | 99.1% | 96% | 0.17 / 2.5 | 13.6% | 0.0% | 206 s | 4.00% |
| 1 | settle-1 | 841,417 | 0.99 | 99.5% | 100% | 0.03 / 2.5 | 13.0% | 0.0% | 916 s | 1.04% |
| 2 | settle-2 | 1,473,048 | 1.91 | 95.5% | 100% | 0.05 / 2.5 | 9.3% | 0.0% | 468 s | 3.98% |
| 1 | settle-3 | 840,451 | 0.99 | 99.5% | 100% | 0.02 / 2.5 | 12.1% | 0.0% | 926 s | 0.04% |
| 2 | settle-4 | 1,523,560 | 2.00 | 100.0% | 100% | 0.05 / 2.5 | 11.3% | 0.0% | 438 s | 0.61% |
| 1 | settle-5 | 848,797 | 0.99 | 99.4% | 100% | 0.03 / 2.5 | 11.8% | 0.0% | 910 s | 1.08% |
| 2 | settle-6 | 1,560,425 | 2.00 | 99.8% | 100% | 0.04 / 2.5 | 10.0% | 0.0% | 424 s | 0.83% |

| cores | passes | mean records/s | spread | reportable |
|---:|---:|---:|---:|---|
| 1 | 6 | 844,045 | 3.8% | yes |
| 2 | 6 | 1,507,136 | 10.2% | yes |
| 4 | 4 | 2,836,615 | 18.6% | yes |

Sentinel: the 4-core case first (2,978,430 rec/s) and last (2,450,975 rec/s), drift -19.4% across the suite; counted in that case's spread.

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
