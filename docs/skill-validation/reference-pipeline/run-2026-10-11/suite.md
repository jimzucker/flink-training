| field | value |
|---|---|
| axis | one machine, more cores |
| API level | DataStream |
| guarantee | state: exactly-once checkpointing; sink: at-least-once, idempotent (the absolute total and count per location and hour) |
| checkpoint interval | 10000 ms |
| build hash | `78f31aba4b78b933` (completeness passed for `78f31aba4b78b933`) |
| passes per case | 3 |
| study | scaling: every case configured identically |
| workload | 900,000,000 records, locationCount 100, sensorCount 1,000, sensorsPerLocation 10, recordBytes 103, hourMs 3,600,000, locationStaggerSeconds 36, partitions 8, closedWindowCount 24,999, readingsInClosedWindows 898,182,000 |
| rate source | committed broker offsets on `temperature-readings` |
| CPU source | cgroup `cpu.stat usage_usec` |
| suite span | 2026-10-11 11:28:19 -> 2026-10-11 12:33:06 EDT (1h 05m)  dashboard: from=1791732499177&to=1791736386032 |

**1→2 cores: not reported — voided: 2c its readings are 23% apart and the limit is 20%.**
**2→4 cores: not reported — voided: 2c its readings are 23% apart and the limit is 20%.**

| cores | speed | scaling | pipeline CPU | pipeline memory | Kafka CPU | Kafka memory | blocked by | what to do |
|---:|---:|---:|---|---|---|---|---|---|
| | | what the step into it gave | cores it could use / how much it used | memory it could use / share of the time spent tidying memory up | cores Kafka could use / how much it used | memory Kafka could use / how many times it filled up | | |
| 1 | 647,103/s | — | 1 / 41% | 1536m / 0.6% | 2.5 / 0% | 5.5g, full 235x | Kafka memory | raise kafkaMemory |
| 2 \* | 1,261,040/s | — | 2 / 100% | 2048m / 0.8% | 2.5 / 4% | 5.5g, full 1,252x | Pipeline CPU | no usable step |
| 4 | 2,596,678/s | — | 4 / 100% | 3072m / 0.4% | 2.5 / 4% | 5.5g, full 1,902x | Pipeline CPU | no usable step |

**1 core:** Kafka's memory, blocking higher throughput. Kafka hit its limit 235 times and had to read the test data back off disk, so the pipeline was waiting on Kafka rather than using its CPU.

| cores | pass | records/s | tm cores | % of cap | throttled | broker cores | src idle | src BP | headroom | vantage |
|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | p1-asc | 666,966 | 0.99 | 99.0% | 100% | 0.07 / 2.5 | 12.9% | 0.0% | 1103 s | 2.66% |
| 2 | p1-asc | 1,441,040 | 1.98 | 99.0% | 100% | 0.05 / 2.5 | 10.3% | 0.0% | 448 s | 1.45% |
| 4 | p1-asc | 2,622,712 | 4.00 | 99.9% | 99% | 0.09 / 2.5 | 14.6% | 0.0% | 195 s | 0.34% |
| 4 | p2-desc | 2,816,239 | 3.98 | 99.5% | 100% | 0.07 / 2.5 | 16.2% | 0.0% | 169 s | 0.04% |
| 2 | p2-desc | 1,149,816 | 1.99 | 99.4% | 100% | 0.11 / 2.5 | 11.2% | 0.0% | 602 s | 0.41% |
| 1 | p2-desc **(ceiling)** | 510,305 | 0.96 | 96.1% | 100% | 0.05 / 2.5 | 22.2% | 0.0% | 1621 s | 1.82% |
| 1 | p3-asc | 627,241 | 1.00 | 99.9% | 100% | 0.05 / 2.5 | 19.5% | 0.0% | 1231 s | 0.11% |
| 2 | p3-asc | 1,192,266 | 1.99 | 99.7% | 100% | 0.09 / 2.5 | 11.9% | 0.0% | 494 s | 1.58% |
| 4 | p3-asc | 2,351,084 | 3.98 | 99.6% | 100% | 0.11 / 2.5 | 17.4% | 0.0% | 244 s | 0.54% |
| 1 | sentinel **(ceiling)** | 331,797 | 0.41 | 41.0% | 100% | 0.01 / 2.5 | 13.9% | 0.0% | 2371 s | 3.09% |

| cores | passes | mean records/s | spread | reportable |
|---:|---:|---:|---:|---|
| 1 | 2 | 647,103 | 6.1% | yes |
| 2 | 3 | 1,261,040 | 23.1% | no — its readings are 23% apart and the limit is 20% |
| 4 | 3 | 2,596,678 | 17.9% | yes |

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
