# Step 05 — idleness, and proof that the setting matters

Step 01's review identified idleness, not lateness, as the real watermark risk:
a join advances at its slower input, so a partition with no traffic stalls the
watermark and the windows stop firing — with every record that did arrive
perfectly in order. Sinks 5 and 6 simply go quiet, which during a demo is
indistinguishable from a broken pipeline.

It is not a hypothetical. With four partitions and four symbols, hashing leaves
partitions empty in an ordinary run:

```
prices:0:0      positions-by-symbol:0:0
prices:1:600    positions-by-symbol:1:145
prices:2:200    positions-by-symbol:2:58
prices:3:0      positions-by-symbol:3:0
```

Two of four partitions hold nothing on both topics.

## The two runs

Identical input, identical everything, one setting changed:

| | `IDLENESS_MS=5000` | `IDLENESS_MS=0` |
|---|---|---|
| positions-by-symbol | 200 | 200 |
| positions-by-account | 800 | 800 |
| **mv-by-symbol** | **12** | **0** |
| **mv-by-account** | **48** | **0** |

Part 1 is unaffected either way, because it does not window. Part 2 produces
nothing at all without idleness — no error, no failed job, no restart. The job
sits in RUNNING and emits silence.

That is why the setting has an off switch: a guarantee nobody has seen fail is
indistinguishable from one that was never needed.
