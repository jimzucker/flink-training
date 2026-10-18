# The job graph

Build `e35f29848e13cb4f`, read off the running plan during the suite, not drawn by hand. 4 tasks, 13 operators. Each box is one Flink task. The operators inside a box are chained: they run in one thread, with no network hop between them. Arrows between boxes are how records cross from one task to another: keyBy is a hash shuffle that sends each key to one parallel instance, and broadcast sends every record to every instance. Rounded shapes are Kafka topics, outside Flink. Every task runs at the case's parallelism (1, 2 or 4). Partition and key counts come from the configuration and that run's manifest; an interval on an output is what the build declared in design.every, not something the harness measured.

![Job graph](suite-graph.png)

Source: [`suite-graph.mmd`](suite-graph.mmd)

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
