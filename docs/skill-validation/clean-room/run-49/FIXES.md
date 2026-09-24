# Fixes

**None.** No change was made to chase a number.

The first build passed completeness on both arms (clean and killed at 42%), and
the first full chain met the target on both steps:

| step | ratio | range across passes | target |
|---|---:|---|---:|
| 1->2 cores | 1.944x | 1.868x-2.022x | 1.80x |
| 2->4 cores | 1.930x | 1.816x-2.052x | 1.80x |

Every case ran at 95.4-100.3% of its CPU cap, so there was nothing to tune and
SKILL section 6a was not entered. The tiny proof's broker-memory warning (381
hits in a 30 s window at 5,376m) was not acted on: the suite reported the cores
as the limit at every case, and in the suite the broker filled its memory only
at 1 core (1,138 times) with that case at 100% of cap.
