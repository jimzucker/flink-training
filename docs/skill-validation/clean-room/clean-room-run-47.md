# Clean-room run 47 — the interview's defaults, as they stood

A fresh agent took the default answer to every interview question: orders and
prices in, positions by symbol and by account plus a market value for each,
**4 symbols and 16 account keys** (the default until #80). DataStream, Apache
Kafka. 2 h 34 min, 108 tool calls. Files: [run-47/](run-47/).

## What it produced

**No table.** The chain ran end to end and threw its own table out: each case's
passes were 54%, 71% and 147% apart against a 20% limit
([suite.txt](run-47/results/suite.txt)). Completeness passed on both arms.

## Why the passes scattered

A controlled test afterwards ([broker-memory-test/](run-47/broker-memory-test/))
found the Mac, not the pipeline or the broker. With a swap recorder beside every
pass, the three slow passes were measured with 4.3–6.0 GB of the Mac's memory
moved out to disk and the nine fast ones at 2.3 GB or less. At the broker size
the run used (4,608 MB), once swap was low, the 4-core case read 440,250 /
417,509 / 461,842 / 459,657 rec/s, 10% apart. The harness then recorded load
average only; since #83 it records swap per pass and says so.

## Re-measured on a quiet Mac

Same build, same configuration ([remeasure/](run-47/remeasure/suite.txt)):
1 → 2 **1.83×**, 2 → 4 **1.76×**, all three cases counted. Both missed the
1.90× target of the day.

## What came of it

#82 (the broker-memory guard used a 99% cap exemption and threw out healthy
passes), #83 (swap per pass), and #80 (the default became 4,096 symbols: run 47
had built a workload a thousand times smaller in keys than the published demo).
