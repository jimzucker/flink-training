| field | value |
|---|---|
| axis | one machine, more cores: one task manager container capped at N cores, parallelism N, N slots |
| API level | Flink DataStream API, hand-written operators (no SQL, no Table API) |
| guarantee | state: exactly-once checkpointing every 10 s; sink: at-least-once Kafka sinks, idempotent by publishing the absolute position per key |
| checkpoint interval | 10000 ms |
| build hash | `d7e14e7505068416` (completeness passed for `d7e14e7505068416`) |
| passes per case | 3 |
| study | scaling: every case configured identically |
| workload | 250,000,000 records, uniqueOrders 249,501,308, duplicateRecords 498,692, partitions 8, allocationsPerOrder 4, outputsPerUniqueOrder 5, symbolKeys 4,096, accountKeys 16,384, grandTotal -40,607,965, pricesPerSymbol 3, 5 outputs per input |
| rate source | committed broker offsets on `orders` |
| CPU source | cgroup `cpu.stat usage_usec` |
| suite span | 2026-10-12 22:43:23 -> 2026-10-12 23:25:01 EDT (42m)  dashboard: from=1791859403975&to=1791861901931 |

**1→2 cores: 2.00× (target 1.80×), range 1.79–2.18× across passes.**
**2→4 cores: not reported — voided: 4c its readings are 20% apart and the limit is 20%.**

| cores | speed | scaling | pipeline CPU | pipeline memory | Kafka CPU | Kafka memory | blocked by | what to do |
|---:|---:|---:|---|---|---|---|---|---|
| | | what the step into it gave | cores it could use / how much it used | memory it could use / share of the time spent tidying memory up | cores Kafka could use / how much it used | memory Kafka could use / how many times it filled up | | |
| 1 | 108,433/s | — | 1 / 100% | 1536m / 3.4% | 2.5 / 5% | 5.5g, full 308x | Pipeline CPU | check it matches |
| 2 | 217,213/s | 2.00x | 2 / 99% | 2048m / 2.1% | 2.5 / 9% | 5.5g, full 3,611x | Pipeline CPU | add cores for more |
| 4 \* | 358,160/s | — | 4 / 98% | 3072m / 0.8% | 2.5 / 20% | 5.5g, full 7,382x | Pipeline CPU | no usable step |

| cores | pass | records/s | tm cores | % of cap | throttled | broker cores | src idle | src BP | headroom | vantage |
|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | p1-asc | 103,314 | 1.00 | 100.1% | 100% | 0.11 / 2.5 | 0.0% | 45.9% | 2285 s | 0.05% |
| 2 | p1-asc | 213,594 | 1.94 | 96.7% | 100% | 0.25 / 2.5 | 0.3% | 29.9% | 1032 s | 0.06% |
| 4 | p1-asc | 333,487 | 3.86 | 96.6% | 100% | 0.51 / 2.5 | 0.8% | 29.9% | 606 s | 0.04% |
| 4 | p2-desc | 406,141 | 3.99 | 99.9% | 100% | 0.62 / 2.5 | 2.1% | 16.0% | 465 s | 0.37% |
| 2 | p2-desc | 222,730 | 1.99 | 99.6% | 100% | 0.27 / 2.5 | 0.5% | 28.6% | 977 s | 0.35% |
| 1 | p2-desc | 119,562 | 1.00 | 99.6% | 100% | 0.12 / 2.5 | 0.0% | 35.9% | 1957 s | 0.04% |
| 1 | p3-asc | 102,230 | 1.00 | 99.8% | 100% | 0.10 / 2.5 | 0.0% | 43.0% | 2300 s | 0.26% |
| 2 | p3-asc | 215,316 | 1.97 | 98.7% | 98% | 0.22 / 2.5 | 0.4% | 30.6% | 1014 s | 0.04% |
| 4 | p3-asc | 334,852 | 3.94 | 98.5% | 99% | 0.50 / 2.5 | 1.8% | 25.0% | 592 s | 0.79% |
| 1 | sentinel | 108,628 | 1.00 | 100.2% | 100% | 0.12 / 2.5 | 0.0% | 41.9% | 2165 s | 0.26% |

| cores | passes | mean records/s | spread | reportable |
|---:|---:|---:|---:|---|
| 1 | 4 | 108,433 | 16.0% | yes |
| 2 | 3 | 217,213 | 4.2% | yes |
| 4 | 3 | 358,160 | 20.3% | no — its readings are 20% apart and the limit is 20% |

Sentinel: the 1-core case first (103,314 rec/s) and last (108,628 rec/s), drift +5.0% across the suite; counted in that case's spread.

### The job graph that ran

Read off the running plan, not drawn. Every case ran this shape — a row whose shape differed would have been thrown out. 4 tasks, 13 operators. Each box is one Flink task. The operators inside a box are chained: they run in one thread, with no network hop between them. Arrows between boxes are how records cross from one task to another: keyBy is a hash shuffle that sends each key to one parallel instance, and broadcast sends every record to every instance. Rounded shapes are Kafka topics, outside Flink. Every task runs at the case's parallelism (1, 2 or 4). Partition and key counts come from the configuration and the run's own manifest; an interval on an output is what the build declared in design.every, not something the harness measured.

![Job graph](suite-graph.png)

```mermaid
flowchart LR
  subgraph t1["Task 1: 2 operators"]
    t1o0["Source: orders-source"]
    t1o1["parse-orders"]
    t1o0 --> t1o1
  end
  subgraph t2["Task 2: 1 operator"]
    t2o0["Source: prices-source"]
  end
  subgraph t3["Task 3: 5 operators"]
    t3o0["position-mv-by-symbol"]
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
    t4o0["position-mv-by-account"]
    t4o1["sink-positions-by-account: Writer"]
    t4o0 --> t4o1
    t4o2["sink-positions-by-account: Committer"]
    t4o1 --> t4o2
    t4o3["sink-market-values-by-account: Writer"]
    t4o0 --> t4o3
    t4o4["sink-market-values-by-account: Committer"]
    t4o3 --> t4o4
  end
  t1o1 -->|"keyBy position-mv-by-symbol, 4,096 keys"| t3o0
  t2o0 -.->|broadcast| t3o0
  t1o1 -->|"keyBy position-mv-by-account, 16,384 keys"| t4o0
  t2o0 -.->|broadcast| t4o0
  in0(["orders<br/>8 partitions"])
  in0 --> t1o0
  in1(["prices"])
  in1 --> t2o0
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
