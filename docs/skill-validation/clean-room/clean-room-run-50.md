# Clean-room run 50 — the defaults, in Flink SQL

The same defaults as run 48 (4,096 symbols, 16,384 account keys, orders and
prices in, two positions and two market values out, Apache Kafka), except
question 5 answered **SQL**. 3 h 23 min, 132 tool calls. Files: [run-50/](run-50/),
including the job itself, [PositionsSqlJob.java](run-50/PositionsSqlJob.java).

## What it produced

| cores | [chain 1](run-50/results-chain1/suite.txt) | [suite 2](run-50/results-suite2/suite.txt), more worker memory |
|---:|---:|---:|
| 1 | 28,357/s — **every pass thrown out**: GC 8.2–9.7% | every pass thrown out: GC 6.9–8.3% |
| 2 | 56,231/s | 57,925/s |
| 4 | 105,317/s | 105,570/s |
| 2 → 4 | **1.873×**, low end 1.818× — met | **1.823×**, low end 1.759× — missed |
| 1 → 2 | **not measured** | not measured |

About a quarter of the DataStream builds' throughput per core (run 49's jar on
Apache: 128,662 / 257,855 / 481,167). 2 → 4 sits on the 1.80× line; 1 → 2 has no
number because every 1-core pass spent more than 5.5% of its time on garbage
collection, and more worker memory brought that down only to 6.7%.

Completeness passed with no tolerances, three times, including the killed arm,
with all 4,096 and 16,384 keys and both market values written.

## What the harness got wrong

**`DONE` said `PASS 70.5 min` with half the claim unmeasured** (the agent's F4):
the configuration asks for 1 → 2 and 2 → 4, every 1-core pass was thrown out, and
the report quietly treated 2 cores as the baseline and judged 2 → 4 alone. The
tiny proof had already shown the 1-core case would be thrown out, and the chain
still spent 43 minutes on a suite that could not report 1 → 2 (F5).

## What the agent reported

12 findings in [SKILL-FEEDBACK.md](run-50/SKILL-FEEDBACK.md). The largest: SQL gets
one sentence in the skill and every build instruction assumes DataStream (F1), and
the key-spread check hashes keys the DataStream way, which is not how a SQL job
lays them out (F2).
