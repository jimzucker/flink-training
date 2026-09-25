| field | value |
|---|---|
| axis | one machine, more cores: one task manager container capped at N cores, parallelism N, N slots |
| API level | Flink SQL: DDL plus one STATEMENT SET of four INSERT INTO statements, submitted from a Java main; the planner chooses every operator, exchange and chain (no DataStream code) |
| guarantee | state: exactly-once checkpointing; sink: at-least-once upsert-kafka, idempotent by writing the absolute position and order count per key |
| checkpoint interval | 10000 ms |
| build hash | `cf0586cba9111def` (completeness passed for `cf0586cba9111def`) |
| passes per case | 3 |
| study | scaling: every case configured identically |
| workload | 120,000,000 records, partitions 8, symbols 4,096, accountKeys 16,384, allocationsPerOrder 4, priceTicksPerSymbol 3, 5 outputs per input |
| rate source | committed broker offsets on `orders` |
| CPU source | cgroup `cpu.stat usage_usec` |
| suite span | 2026-09-25 00:03:30 -> 2026-09-25 00:46:48 EDT (43m)  dashboard: from=1790309010708&to=1790311608340 |

**2→4 cores: 1.82× (target 1.80×), range 1.73–1.94× across passes.**

| cores | speed | scaling | pipeline CPU | pipeline memory | Kafka CPU | Kafka memory | blocked by | what to do |
|---:|---:|---:|---|---|---|---|---|---|
| | | what the step into it gave | cores it could use / how much it used | memory it could use / share of the time spent tidying memory up | cores Kafka could use / how much it used | memory Kafka could use / how many times it filled up | | |
| 2 | 57,925/s | — | 2 / 100% | 2880m / 2.5% | 2.5 / 4% | 4.5g, never full | Pipeline CPU | check it matches |
| 4 | 105,570/s | 1.82x | 4 / 96% | 3968m / 0.8% | 2.5 / 7% | 4.5g, never full | Pipeline CPU | check the host |

| cores | pass | records/s | tm cores | % of cap | throttled | broker cores | src idle | src BP | headroom | vantage |
|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | p1-asc **(ceiling)** | 28,906 | 1.00 | 100.3% | 100% | 0.04 / 2.5 | 0.0% | 2.9% | 4013 s | 0.35% |
| 2 | p1-asc | 59,178 | 2.00 | 100.1% | 100% | 0.09 / 2.5 | 0.0% | 3.0% | 1885 s | 0.37% |
| 4 | p1-asc | 108,809 | 3.94 | 98.6% | 100% | 0.17 / 2.5 | 0.0% | 2.3% | 955 s | 0.38% |
| 4 | p2-desc | 105,667 | 4.01 | 100.2% | 100% | 0.18 / 2.5 | 0.0% | 2.2% | 979 s | 0.29% |
| 2 | p2-desc | 56,066 | 1.96 | 98.1% | 100% | 0.09 / 2.5 | 0.0% | 3.1% | 2006 s | 0.30% |
| 1 | p2-desc **(ceiling)** | 27,676 | 1.00 | 100.2% | 100% | 0.04 / 2.5 | 0.0% | 3.3% | 4198 s | 0.03% |
| 1 | p3-asc **(ceiling)** | 27,455 | 1.00 | 99.7% | 100% | 0.04 / 2.5 | 0.0% | 2.5% | 4239 s | 0.28% |
| 2 | p3-asc | 58,529 | 1.99 | 99.6% | 100% | 0.09 / 2.5 | 0.0% | 2.9% | 1902 s | 0.25% |
| 4 | p3-asc | 102,234 | 3.83 | 95.8% | 100% | 0.17 / 2.5 | 0.0% | 2.2% | 1027 s | 0.24% |
| 1 | sentinel **(ceiling)** | 27,773 | 0.99 | 99.4% | 100% | 0.04 / 2.5 | 0.0% | 3.3% | 4193 s | 0.37% |

| cores | passes | mean records/s | spread | reportable |
|---:|---:|---:|---:|---|
| 2 | 3 | 57,925 | 5.4% | yes |
| 4 | 3 | 105,570 | 6.2% | yes |

### The job graph that ran

Read off the running plan, not drawn. Every case ran this shape — a row whose shape differed would have been thrown out.

```mermaid
flowchart LR
  v0["4:GroupAggregate(groupBysym, selectsym, SUM(qty) AS EXPR1, COUNT() AS EXPR2)<br/>5:ConstraintEnforcerNotNullEnforcer(fieldssym)<br/>positions_by_symbol5: Writer<br/>positions_by_symbol5: Committer"]
  v1["11:GroupAggregate(groupByacct, sub, sym, selectacct, sub, sym, SUM(qty) AS EXPR3, COUNT() AS EXPR4)<br/>12:ConstraintEnforcerNotNullEnforcer(fieldsacct, sub, sym)<br/>positions_by_account12: Writer<br/>positions_by_account12: Committer"]
  v2["35:Join(joinTypeInnerJoin, where(sym  sym0), selectacct, sub, sym, pos, orders, sym0, px, leftInputSpecHasUniqueKey, rightInputSpecJoinKeyContainsUniqueKey)<br/>36:Calc(selectacct, sub, sym, pos, px, CAST((pos  px) AS DECIMAL(38, 2)) AS EXPR5, orders)<br/>37:ConstraintEnforcerNotNullEnforcer(fieldsacct, sub, sym)<br/>market_values_by_account37: Writer<br/>market_values_by_account37: Committer"]
  v3["33:GroupAggregate(groupByacct, sub, sym, selectacct, sub, sym, SUM(dq) AS pos, SUM(dn) AS orders)"]
  v4["30:WindowAggregate(groupByacct, sub, sym, windowTUMBLE(time_colpt, size10 s), selectacct, sub, sym, SUM(qty) AS dq, COUNT() AS dn, start(w) AS window_start, end(w) AS window_end)<br/>31:Calc(selectacct, sub, sym, dq, dn)"]
  v5["25:Join(joinTypeInnerJoin, where(sym  sym0), selectsym, pos, orders, sym0, px, leftInputSpecJoinKeyContainsUniqueKey, rightInputSpecJoinKeyContainsUniqueKey)<br/>26:Calc(selectsym, pos, px, CAST((pos  px) AS DECIMAL(38, 2)) AS EXPR3, orders)<br/>27:ConstraintEnforcerNotNullEnforcer(fieldssym)<br/>market_values_by_symbol27: Writer<br/>market_values_by_symbol27: Committer"]
  v6["18:GroupAggregate(groupBysym, selectsym, SUM(dq) AS pos, SUM(dn) AS orders)"]
  v7["15:WindowAggregate(groupBysym, windowTUMBLE(time_colpt, size10 s), selectsym, SUM(qty) AS dq, COUNT() AS dn, start(w) AS window_start, end(w) AS window_end)<br/>16:Calc(selectsym, dq, dn)"]
  v8["1:TableSourceScan(tabledefault_catalog, default_database, orders, fieldsid, sym, qty, ts, allocs)<br/>2:Calc(selectsym, qty)<br/>6:Calc(selectid, sym, qty, ts, allocs, PROCTIME() AS pt)<br/>7:Correlate(invocationUNNEST_ROWS1(cor1.allocs), correlatetable(UNNEST_ROWS1(cor1.allocs)), selectid,sym,qty,ts,allocs,pt,acct,sub,qty0, rowTypeRecordType(BIGINT id, VARCHAR(2147483647) sym, BIGINT qty, BIGINT ts, RecordType:peek_no_expand(VARCHAR(2147483647) acct, VARCHAR(2147483647) sub, BIGINT qty) ARRAY allocs, TIMESTAMP_LTZ(3) PROCTIME pt, VARCHAR(2147483647) acct, VARCHAR(2147483647) sub, BIGINT qty0), joinTypeINNER)<br/>8:Calc(selectsym, acct, sub, qty0 AS qty, pt)<br/>9:Calc(selectacct, sub, sym, qty)<br/>28:Calc(selectacct, sub, sym, qty, pt)<br/>13:Calc(selectsym, qty, PROCTIME() AS pt)"]
  v9["22:Rank(strategyAppendFastStrategy, rankTypeROW_NUMBER, rankRangerankStart1, rankEnd1, partitionBysym, orderByts DESC, selectsym, ts, px)<br/>23:Calc(selectsym, px)"]
  v10["20:TableSourceScan(tabledefault_catalog, default_database, prices, fieldssym, ts, px)"]
  v8 -- HASH --> v0
  v8 -- HASH --> v1
  v3 -- HASH --> v2
  v9 -- HASH --> v2
  v4 -- HASH --> v3
  v8 -- HASH --> v4
  v6 -- HASH --> v5
  v9 -- HASH --> v5
  v7 -- HASH --> v6
  v8 -- HASH --> v7
  v10 -- HASH --> v9
```
