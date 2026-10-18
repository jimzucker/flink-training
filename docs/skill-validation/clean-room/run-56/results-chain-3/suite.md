| field | value |
|---|---|
| axis | one machine, more cores: one task manager container capped at N cores, parallelism N, N slots |
| API level | Flink DataStream API, hand-written operators (no SQL, no Table API) |
| guarantee | state: exactly-once checkpointing; sink: at-least-once Kafka sinks, idempotent by publishing the absolute position per key |
| checkpoint interval | 10000 ms |
| build hash | `e35f29848e13cb4f` (completeness passed for `e35f29848e13cb4f`) |
| passes per case | 3 |
| study | scaling: every case configured identically |
| workload | 250,000,000 records, duplicateRecords 1,243,401, uniqueOrders 248,756,599, partitions 8, allocationsPerOrder 4, symbolKeyCount 4,096, accountKeyCount 16,384, priceRecords 16,384, 5 outputs per input |
| rate source | committed broker offsets on `orders` |
| CPU source | cgroup `cpu.stat usage_usec` |
| suite span | 2026-10-18 14:39:03 -> 2026-10-18 15:29:36 EDT (51m)  dashboard: from=1792348743886&to=1792351776744 |

**1→2 cores: not reported — the 2-core case does not count: its readings are 24.4% apart, over the 20% limit.**
**2→4 cores: not reported — the 2-core case does not count: its readings are 24.4% apart, over the 20% limit; the 4-core case does not count: its readings are 28.8% apart, over the 20% limit.**

| cores | speed | scaling | pipeline CPU | pipeline memory | Kafka CPU | Kafka memory | blocked by | what to do |
|---:|---:|---:|---|---|---|---|---|---|
| | | what the step into it gave | cores it could use / how much it used | memory it could use / share of the time spent tidying memory up | cores Kafka could use / how much it used | memory Kafka could use / how many times it filled up | | |
| 1 | 120,080/s | — | 1 / 99% | 1728m / 3.1% | 2.5 / 8% | 5g, full 258x | Pipeline CPU | check it matches |
| 2 \* | 214,065/s | — | 2 / 100% | 2368m / 1.3% | 2.5 / 17% | 5g, full 3,945x | Pipeline CPU | no usable step |
| 4 \* | 375,851/s | — | 4 / 100% | 3648m / 0.5% | 2.5 / 33% | 5g, full 10,357x | Pipeline CPU | no usable step |

| cores | pass | records/s | tm cores | % of cap | throttled | broker cores | src idle | src BP | headroom | vantage |
|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | p1-asc | 122,940 | 1.00 | 100.0% | 100% | 0.19 / 2.5 | 0.0% | 34.1% | 1895 s | 0.51% |
| 2 | p1-asc | 233,598 | 2.00 | 99.8% | 100% | 0.32 / 2.5 | 0.7% | 26.6% | 914 s | 0.38% |
| 4 | p1-asc | 421,902 | 3.98 | 99.5% | 99% | 0.60 / 2.5 | 0.6% | 22.9% | 437 s | 0.93% |
| 4 | p2-desc | 391,822 | 4.00 | 100.1% | 97% | 0.71 / 2.5 | 1.0% | 23.3% | 466 s | 0.56% |
| 2 | p2-desc | 227,223 | 2.00 | 100.3% | 100% | 0.33 / 2.5 | 0.6% | 27.8% | 950 s | 0.13% |
| 1 | p2-desc | 121,436 | 0.99 | 99.5% | 100% | 0.16 / 2.5 | 0.1% | 35.8% | 1915 s | 0.99% |
| 1 | p3-asc | 121,815 | 1.00 | 99.8% | 100% | 0.17 / 2.5 | 0.0% | 34.8% | 1908 s | 0.80% |
| 2 | p3-asc | 181,374 | 1.99 | 99.7% | 100% | 0.41 / 2.5 | 0.8% | 25.7% | 1211 s | 0.30% |
| 4 | p3-asc | 313,830 | 3.98 | 99.5% | 95% | 0.82 / 2.5 | 0.6% | 25.6% | 647 s | 0.86% |
| 1 | sentinel | 114,128 | 0.99 | 99.1% | 100% | 0.21 / 2.5 | 0.1% | 32.7% | 2057 s | 0.51% |

| cores | passes | mean records/s | spread | reportable |
|---:|---:|---:|---:|---|
| 1 | 4 | 120,080 | 7.3% | yes |
| 2 | 3 | 214,065 | 24.4% | no — its readings are 24.4% apart, over the 20% limit |
| 4 | 3 | 375,851 | 28.7% | no — its readings are 28.8% apart, over the 20% limit |

Sentinel: the 1-core case first (122,940 rec/s) and last (114,128 rec/s), drift -7.4% across the suite; counted in that case's spread.

### The job graph that ran

Read off the running plan, not drawn. Every case ran this shape — a row whose shape differed would have been thrown out. 4 tasks, 13 operators. Each box is one Flink task. The operators inside a box are chained: they run in one thread, with no network hop between them. Arrows between boxes are how records cross from one task to another: keyBy is a hash shuffle that sends each key to one parallel instance, and broadcast sends every record to every instance. Rounded shapes are Kafka topics, outside Flink. Every task runs at the case's parallelism (1, 2 or 4). Partition and key counts come from the configuration and the run's own manifest; an interval on an output is what the build declared in design.every, not something the harness measured.

![Job graph](suite-graph.png)

```mermaid
flowchart LR
  subgraph t1["Task 1: 1 operator"]
    t1o0["Source: kafka-source-prices"]
  end
  subgraph t2["Task 2: 2 operators"]
    t2o0["Source: kafka-source-orders"]
    t2o1["parse-orders"]
    t2o0 --> t2o1
  end
  subgraph t3["Task 3: 5 operators"]
    t3o0["position-and-market-value-by-symbol"]
    t3o1["sink-positions-by-symbol: Writer"]
    t3o0 --> t3o1
    t3o2["sink-positions-by-symbol: Committer"]
    t3o1 --> t3o2
    t3o3["sink-market-values-by-symbol: Writer"]
    t3o0 --> t3o3
    t3o4["sink-market-values-by-symbol: Committer"]
    t3o3 --> t3o4
  end
  subgraph t4["Task 4: 5 operators"]
    t4o0["position-and-market-value-by-account"]
    t4o1["sink-positions-by-account: Writer"]
    t4o0 --> t4o1
    t4o2["sink-positions-by-account: Committer"]
    t4o1 --> t4o2
    t4o3["sink-market-values-by-account: Writer"]
    t4o0 --> t4o3
    t4o4["sink-market-values-by-account: Committer"]
    t4o3 --> t4o4
  end
  t1o0 -.->|broadcast| t3o0
  t2o1 -->|"keyBy symbol, 4,096 keys"| t3o0
  t1o0 -.->|broadcast| t4o0
  t2o1 -->|"keyBy account, 16,384 keys"| t4o0
  in0(["orders<br/>8 partitions"])
  in0 --> t2o0
  in1(["prices"])
  in1 --> t1o0
  out0(["positions-by-symbol<br/>4,096 keys"])
  t3o1 ---> out0
  out1(["positions-by-account<br/>16,384 keys"])
  t4o1 ---> out1
  out2(["market-values-by-symbol<br/>4,096 keys"])
  t3o3 --->|"every 10 s"| out2
  out3(["market-values-by-account<br/>16,384 keys"])
  t4o3 --->|"every 10 s"| out3
```

The same shape as the graph completeness drew (`graph.md`).
