| field | value |
|---|---|
| axis | one worker growing: one task manager container capped at N cores, parallelism N, N slots |
| API level | Flink DataStream API, hand-written operators (no SQL, no Table API) |
| guarantee | state: exactly-once checkpointing; sink: at-least-once, idempotent by publishing the absolute position per key |
| checkpoint interval | 10000 ms |
| build hash | `3f6c398464479ec2` (completeness passed for `3f6c398464479ec2`) |
| passes per case | 3 |
| study | scaling: every case configured identically |
| workload | 165,000,000 records, allocationsPerOrder 4, outputRowsPerOrder 5, symbolKeyCount 4, accountKeyCount 16, totalQuantity 10,890,029,626, priceRecordsPerSymbol 8, 5 outputs per input |
| rate source | committed broker offsets on `orders` |
| CPU source | cgroup `cpu.stat usage_usec` |
| suite span | 2026-09-24 03:56:33 -> 2026-09-24 04:36:10 EDT (40m)  dashboard: from=1790236593424&to=1790238970542 |

**1→2 cores: 1.83× (target 1.90×), range 1.78–1.89× across passes.**
**2→4 cores: 1.76× (target 1.90×), range 1.57–1.98× across passes.**

| cores | speed | scaling | pipeline CPU | pipeline memory | Kafka CPU | Kafka memory | blocked by | what to do |
|---:|---:|---:|---|---|---|---|---|---|
| | | what the step into it gave | cores it could use / how much it used | memory it could use / share of the time spent tidying memory up | cores Kafka could use / how much it used | memory Kafka could use / how many times it filled up | | |
| 1 | 123,021/s | — | 1 / 100% | 1664m / 2.1% | 2.5 / 6% | 4.5g, full 5,246x | Pipeline CPU | check it matches |
| 2 | 225,255/s | 1.83x | 2 / 97% | 2304m / 1.1% | 2.5 / 10% | 4.5g, full 10,517x | Kafka memory | raise kafkaMemory |
| 4 | 396,186/s | 1.76x | 4 / 98% | 3584m / 0.3% | 2.5 / 16% | 4.5g, never full | Pipeline CPU | check the host |

**2 cores:** Kafka's memory, blocking higher throughput. Kafka hit its limit 10,517 times and had to read the test data back off disk, so the pipeline was waiting on Kafka rather than using its CPU.

| cores | pass | records/s | tm cores | % of cap | throttled | broker cores | src idle | src BP | headroom | vantage |
|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | p1-asc **(ceiling)** | 117,276 | 0.95 | 95.0% | 100% | 0.14 / 2.5 | 0.1% | 33.5% | 1276 s | 0.12% |
| 2 | p1-asc | 218,514 | 2.00 | 99.7% | 100% | 0.28 / 2.5 | 2.6% | 24.4% | 613 s | 0.20% |
| 4 | p1-asc | 392,863 | 3.98 | 99.6% | 98% | 0.50 / 2.5 | 1.0% | 28.1% | 262 s | 0.17% |
| 4 | p2-desc | 431,822 | 4.01 | 100.2% | 99% | 0.48 / 2.5 | 1.1% | 24.3% | 232 s | 0.13% |
| 2 | p2-desc | 231,996 | 1.99 | 99.6% | 99% | 0.27 / 2.5 | 2.8% | 24.6% | 570 s | 0.48% |
| 1 | p2-desc **(ceiling)** | 128,794 | 0.98 | 98.6% | 100% | 0.14 / 2.5 | 0.3% | 34.1% | 1141 s | 0.23% |
| 1 | p3-asc | 122,978 | 0.99 | 99.4% | 100% | 0.14 / 2.5 | 0.2% | 30.2% | 1205 s | 0.18% |
| 2 | p3-asc **(ceiling)** | 220,730 | 1.94 | 96.8% | 100% | 0.25 / 2.5 | 2.4% | 25.1% | 607 s | 0.21% |
| 4 | p3-asc | 363,872 | 3.92 | 98.0% | 96% | 0.39 / 2.5 | 0.7% | 30.5% | 275 s | 0.13% |
| 1 | sentinel | 123,064 | 1.00 | 99.7% | 100% | 0.14 / 2.5 | 0.0% | 32.4% | 1192 s | 0.49% |

| cores | passes | mean records/s | spread | reportable |
|---:|---:|---:|---:|---|
| 1 | 2 | 123,021 | 0.1% | yes |
| 2 | 2 | 225,255 | 6.0% | yes |
| 4 | 3 | 396,186 | 17.2% | yes |

Sentinel: the 1-core case first (122,978 rec/s) and last (123,064 rec/s), drift +0.1% across the suite; counted in that case's spread.

### The job graph that ran

Read off the running plan, not drawn. Every case ran this shape — a row whose shape differed would have been thrown out.

```mermaid
flowchart LR
  v0["market-value-by-symbol<br/>sink-mv-by-symbol: Writer<br/>sink-mv-by-symbol: Committer"]
  v1["position-by-symbol<br/>sink-positions-by-symbol: Writer<br/>sink-positions-by-symbol: Committer"]
  v2["market-value-by-account<br/>sink-mv-by-account: Writer<br/>sink-mv-by-account: Committer"]
  v3["position-by-account<br/>sink-positions-by-account: Writer<br/>sink-positions-by-account: Committer"]
  v4["Source: orders-source<br/>parse"]
  v5["Source: prices-source<br/>parse-price"]
  v1 --> v0
  v5 -- BROADCAST --> v0
  v4 -- HASH --> v1
  v3 --> v2
  v5 -- BROADCAST --> v2
  v4 -- HASH --> v3
```
