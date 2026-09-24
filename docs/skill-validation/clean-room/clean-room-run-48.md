# Clean-room run 48 — the defaults, with 4,096 symbols

The same defaults as run 47, after #80 changed the key count to **4,096 symbols
and 16,384 account keys**. DataStream, Apache Kafka. 3 h 55 min, 122 tool calls.
Files: [run-48/](run-48/).

## What it produced

A clean table that missed the target, three times over:

| | 1 → 2 | 2 → 4 |
|---|---:|---:|
| [chain 2](run-48/chain2/suite.txt) | 1.814× | 1.944× |
| [chain 3](run-48/chain3/suite.txt), maxParallelism 440 | 1.851× | 1.940× |
| [live check](run-48/live-check/suite.txt), after #82–#85 | 1.820× | 1.962× |

Every pass at 95–100% of its cap; swap flat at 2.2–2.4 GB throughout.

## What the harness got wrong

The broker-memory guard threw out 1–4 passes per chain that were indistinguishable
from the ones it kept (the agent's F13, fixed in #82). The disk projection counted
the tiny and completeness topics as the suite's backlog (F10, #84). A generator
that died 7.5 s in was waited on for 29 minutes in silence (F8, #85).

## What came of it

Its build's 1→2 is lower than run 49's, which took the same answers; the
difference is in the code ([article-gap.md](../article-gap.md)). With its forced
G1 removed, its 1-core case runs Serial and its 1→2 rises to 1.96× — one half of
the evidence behind #90.
