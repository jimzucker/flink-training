| field | value |
|---|---|
| axis | one machine, more cores: one task manager container capped at N cores, parallelism N, N slots |
| API level | Flink DataStream API (KeyedBroadcastProcessFunction per key space), Flink 1.20.1 |
| guarantee | state: exactly-once checkpointing every 5 s; sink: at-least-once Kafka sinks, idempotent by emitting the absolute position per key |
| checkpoint interval | 5000 ms |
| build hash | `55e693b915fbe2a6` (completeness passed for `55e693b915fbe2a6`) |
| passes per case | 3 |
| study | scaling: every case configured identically |
| workload | 300,000,000 records, uniqueOrders 298,506,588, duplicateRecords 1,493,412, partitions 8, fanOut 5, distinctSymbols 4,096, distinctAccountKeys 16,384, accountSubAccountPairs 4, 5 outputs per input |
| rate source | committed broker offsets on `orders` |
| CPU source | cgroup `cpu.stat usage_usec` |
| suite span | 2026-10-14 20:26:45 -> 2026-10-14 21:32:06 EDT (1h 05m)  dashboard: from=1792024005015&to=1792027926934 |

**1→2 cores: 1.83× (target 1.80×), range it is judged on 1.763x to 1.847x.**
**2→4 cores: 1.86× (target 1.80×), range it is judged on 1.784x to 1.980x.**

| cores | speed | scaling | pipeline CPU | pipeline memory | Kafka CPU | Kafka memory | blocked by | what to do |
|---:|---:|---:|---|---|---|---|---|---|
| | | what the step into it gave | cores it could use / how much it used | memory it could use / share of the time spent tidying memory up | cores Kafka could use / how much it used | memory Kafka could use / how many times it filled up | | |
| 1 | 128,653/s | — | 1 / 96% | 1664m / 4.5% | 2.5 / 4% | 5.5g, full 756x | Pipeline CPU | check it matches |
| 2 | 235,385/s | 1.83x | 2 / 99% | 2304m / 2.0% | 2.5 / 8% | 5.5g, full 3,190x | Pipeline CPU | more passes |
| 4 | 438,463/s | 1.86x | 4 / 97% | 3584m / 1.0% | 2.5 / 20% | 5.5g, full 6,892x | Pipeline CPU | add cores for more |

| cores | pass | records/s | tm cores | % of cap | throttled | broker cores | src idle | src BP | headroom | vantage |
|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | p1-asc | 133,982 | 0.99 | 99.3% | 100% | 0.11 / 2.5 | 0.0% | 31.2% | 2103 s | 0.33% |
| 2 | p1-asc | 241,924 | 2.00 | 100.2% | 100% | 0.22 / 2.5 | 0.8% | 23.9% | 1096 s | 1.04% |
| 4 | p1-asc | 455,338 | 3.98 | 99.4% | 99% | 0.47 / 2.5 | 2.5% | 13.5% | 514 s | 0.34% |
| 4 | p2-desc **(ceiling)** | 425,876 | 3.78 | 94.5% | 99% | 0.44 / 2.5 | 1.8% | 14.9% | 566 s | 0.15% |
| 2 | p2-desc | 235,642 | 1.99 | 99.4% | 100% | 0.23 / 2.5 | 0.7% | 29.5% | 1134 s | 0.47% |
| 1 | p2-desc | 120,403 | 0.97 | 97.0% | 100% | 0.12 / 2.5 | 0.0% | 27.8% | 2360 s | 0.32% |
| 1 | p3-asc | 132,430 | 1.00 | 100.0% | 100% | 0.10 / 2.5 | 0.1% | 30.7% | 2135 s | 0.41% |
| 2 | p3-asc | 239,018 | 1.98 | 99.2% | 100% | 0.21 / 2.5 | 0.6% | 25.3% | 1108 s | 0.20% |
| 4 | p3-asc | 421,588 | 3.88 | 96.9% | 96% | 0.51 / 2.5 | 4.3% | 14.9% | 559 s | 0.87% |
| 1 | sentinel | 123,611 | 0.97 | 97.1% | 100% | 0.10 / 2.5 | 0.0% | 25.9% | 2289 s | 0.88% |
| 2 | settle-1 | 232,266 | 1.96 | 98.2% | 98% | 0.21 / 2.5 | 0.5% | 23.2% | 1145 s | 0.04% |
| 1 | settle-2 | 128,954 | 0.98 | 98.4% | 100% | 0.10 / 2.5 | 0.1% | 28.9% | 2188 s | 1.00% |
| 2 | settle-3 | 226,074 | 1.93 | 96.4% | 100% | 0.20 / 2.5 | 0.5% | 25.3% | 1190 s | 0.25% |
| 1 | settle-4 | 128,564 | 0.97 | 96.7% | 100% | 0.11 / 2.5 | 0.0% | 26.2% | 2200 s | 0.13% |
| 2 | settle-5 | 237,388 | 1.97 | 98.6% | 98% | 0.21 / 2.5 | 0.9% | 24.2% | 1118 s | 0.25% |
| 1 | settle-6 | 132,625 | 0.95 | 95.5% | 100% | 0.10 / 2.5 | 0.0% | 26.9% | 2132 s | 0.15% |

| cores | passes | mean records/s | spread | reportable |
|---:|---:|---:|---:|---|
| 1 | 7 | 128,653 | 10.5% | yes |
| 2 | 6 | 235,385 | 6.7% | yes |
| 4 | 2 | 438,463 | 7.7% | yes |

Sentinel: the 1-core case first (133,982 rec/s) and last (123,611 rec/s), drift -8.1% across the suite; counted in that case's spread.

### The job graph that ran

Read off the running plan, not drawn. Every case ran this shape — a row whose shape differed would have been thrown out. 4 tasks, 14 operators. Each box is one Flink task. The operators inside a box are chained: they run in one thread, with no network hop between them. Arrows between boxes are how records cross from one task to another: keyBy is a hash shuffle that sends each key to one parallel instance, and broadcast sends every record to every instance. Rounded shapes are Kafka topics, outside Flink. Every task runs at the case's parallelism (1, 2 or 4). Partition and key counts come from the configuration and the run's own manifest; an interval on an output is what the build declared in design.every, not something the harness measured.

![Job graph](suite-graph.png)

```mermaid
flowchart LR
  subgraph t1["Task 1: 2 operators"]
    t1o0["Source: kafka-source-orders"]
    t1o1["parse-orders"]
    t1o0 --> t1o1
  end
  subgraph t2["Task 2: 2 operators"]
    t2o0["Source: kafka-source-prices"]
    t2o1["parse-prices"]
    t2o0 --> t2o1
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
  t1o1 -->|"keyBy positions-by-symbol, 4,096 keys"| t3o0
  t2o1 -.->|broadcast| t3o0
  t1o1 -->|"keyBy positions-by-account, 16,384 keys"| t4o0
  t2o1 -.->|broadcast| t4o0
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
