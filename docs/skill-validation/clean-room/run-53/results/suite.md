| field | value |
|---|---|
| axis | one machine, more cores: one task manager container capped at N cores, parallelism N, N slots |
| API level | Flink DataStream API, hand-written operators (no SQL, no Table API) |
| guarantee | state: exactly-once checkpointing every 10 s; sink: at-least-once Kafka sinks, idempotent by publishing the absolute position per key |
| checkpoint interval | 10000 ms |
| build hash | `b0d1448e7640f0c3` (completeness passed for `b0d1448e7640f0c3`) |
| passes per case | 3 |
| study | scaling: every case configured identically |
| workload | 250,000,000 records, uniqueOrders 248,755,421, duplicateRecords 1,244,579, partitions 8, outputsPerUniqueOrder 5, symbolKeysPredicted 4,096, accountKeysPredicted 16,384, symbolKeysWithOrders 4,096, accountKeysWithOrders 16,384, priceSeed 20,261,011, pricesPerSymbol 3, 5 outputs per input |
| rate source | committed broker offsets on `orders` |
| CPU source | cgroup `cpu.stat usage_usec` |
| suite span | 2026-10-11 20:20:22 -> 2026-10-11 21:24:23 EDT (1h 04m)  dashboard: from=1791764422239&to=1791768263880 |

**1→2 cores: 1.88× (target 1.80×), range 1.68–2.05× across passes.**
**2→4 cores: 1.80× (target 1.80×), range 1.57–2.18× across passes.**

| cores | speed | scaling | pipeline CPU | pipeline memory | Kafka CPU | Kafka memory | blocked by | what to do |
|---:|---:|---:|---|---|---|---|---|---|
| | | what the step into it gave | cores it could use / how much it used | memory it could use / share of the time spent tidying memory up | cores Kafka could use / how much it used | memory Kafka could use / how many times it filled up | | |
| 1 | 130,810/s | — | 1 / 100% | 1664m / 2.9% | 2.5 / 5% | 5g, full 1,181x | Pipeline CPU | check it matches |
| 2 | 245,664/s | 1.88x | 2 / 96% | 2304m / 1.5% | 2.5 / 8% | 5g, full 4,427x | Pipeline CPU | add cores for more |
| 4 | 441,859/s | 1.80x | 4 / 100% | 3584m / 0.7% | 2.5 / 20% | 5g, full 10,280x | Pipeline CPU | more passes |

| cores | pass | records/s | tm cores | % of cap | throttled | broker cores | src idle | src BP | headroom | vantage |
|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | p1-asc | 134,728 | 0.97 | 97.0% | 100% | 0.11 / 2.5 | 0.0% | 41.9% | 1721 s | 0.79% |
| 2 | p1-asc | 256,072 | 1.99 | 99.7% | 99% | 0.24 / 2.5 | 1.0% | 30.0% | 821 s | 0.99% |
| 4 | p1-asc | 422,338 | 3.86 | 96.4% | 97% | 0.62 / 2.5 | 2.1% | 22.3% | 449 s | 0.31% |
| 4 | p2-desc | 428,256 | 3.95 | 98.7% | 99% | 0.58 / 2.5 | 1.4% | 25.0% | 445 s | 0.33% |
| 2 | p2-desc | 225,781 | 1.90 | 95.1% | 100% | 0.26 / 2.5 | 0.7% | 32.0% | 968 s | 0.52% |
| 1 | p2-desc | 130,434 | 0.98 | 97.7% | 100% | 0.11 / 2.5 | 0.1% | 37.5% | 1787 s | 0.12% |
| 1 | p3-asc **(ceiling)** | 129,755 | 0.95 | 94.7% | 100% | 0.11 / 2.5 | 0.1% | 40.2% | 1797 s | 0.26% |
| 2 | p3-asc | 258,294 | 2.00 | 100.1% | 100% | 0.25 / 2.5 | 0.8% | 33.5% | 822 s | 0.14% |
| 4 | p3-asc | 410,837 | 3.96 | 99.1% | 99% | 0.64 / 2.5 | 2.0% | 20.3% | 453 s | 0.25% |
| 1 | sentinel | 127,269 | 0.99 | 99.6% | 100% | 0.12 / 2.5 | 0.1% | 42.3% | 1824 s | 0.30% |
| 2 | settle-1 | 238,688 | 2.01 | 100.5% | 100% | 0.25 / 2.5 | 0.7% | 33.0% | 905 s | 0.32% |
| 4 | settle-2 | 429,480 | 4.02 | 100.5% | 99% | 0.62 / 2.5 | 1.8% | 20.2% | 438 s | 0.53% |
| 2 | settle-3 | 234,027 | 2.00 | 100.1% | 100% | 0.25 / 2.5 | 0.7% | 35.0% | 924 s | 0.24% |
| 4 | settle-4 | 467,289 | 3.99 | 99.7% | 99% | 0.56 / 2.5 | 1.7% | 22.8% | 392 s | 0.19% |
| 2 | settle-5 | 261,124 | 1.93 | 96.4% | 100% | 0.21 / 2.5 | 0.9% | 34.3% | 820 s | 0.26% |
| 4 | settle-6 | 492,953 | 3.98 | 99.6% | 99% | 0.49 / 2.5 | 2.2% | 18.7% | 359 s | 0.31% |

| cores | passes | mean records/s | spread | reportable |
|---:|---:|---:|---:|---|
| 1 | 3 | 130,810 | 5.7% | yes |
| 2 | 6 | 245,664 | 14.4% | yes |
| 4 | 6 | 441,859 | 18.6% | yes |

Sentinel: the 1-core case first (134,728 rec/s) and last (127,269 rec/s), drift -5.7% across the suite; counted in that case's spread.

### The job graph that ran

Read off the running plan, not drawn. Every case ran this shape — a row whose shape differed would have been thrown out. 4 tasks, 14 operators. Each box is one Flink task. The operators inside a box are chained: they run in one thread, with no network hop between them. Arrows between boxes are how records cross from one task to another: keyBy is a hash shuffle that sends each key to one parallel instance, and broadcast sends every record to every instance. Rounded shapes are Kafka topics, outside Flink. Every task runs at the case's parallelism (1, 2 or 4). Partition and key counts come from the configuration and the run's own manifest; an interval on an output is what the build declared in design.every, not something the harness measured.

![Job graph](suite-graph.png)

```mermaid
flowchart LR
  subgraph t1["Task 1: 2 operators"]
    t1o0["Source: order-source"]
    t1o1["parse"]
    t1o0 --> t1o1
  end
  subgraph t2["Task 2: 2 operators"]
    t2o0["Source: prices-source"]
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
  t1o1 -->|"keyBy by-symbol, 4,096 keys"| t3o0
  t2o1 -.->|broadcast| t3o0
  t1o1 -->|"keyBy by-account, 16,384 keys"| t4o0
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
