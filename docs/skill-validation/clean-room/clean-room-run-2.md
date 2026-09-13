# Clean-room validation, run 2

The same test as [run 1](clean-room-run-1.md), against the skill after run 1's
lessons went into it. Fresh agent, empty directory, barred from reading this
repository *and* run 1's directory. One prompt, no human input.

**Model: Claude Opus 5.**

## What it cost

| | |
|---|---:|
| Human time | **0 h** |
| Human prompts | **1** |
| Wall clock | 1.78 h |
| Tool calls | 110 |
| Assistant turns | 208 |
| Tokens | 23,479,843 |
| Cost, metered-API equivalent | $43.74 |

## What it produced

90,000,000 block trades pre-loaded, producer stopped before every case. Its own
choices: 12 partitions, 10s checkpoints, at-least-once, 10× fan-out. Throughput
read from committed Kafka offsets, never from Flink.

| cores | orders/s | vs 1 core | TM cores used | % of cap | broker |
|---|---:|---:|---|---:|---|
| 1 | 65,027 | 1.00× | 0.99 of 1 | 99% | 0.08 of 2 |
| 2 | 130,290 | **2.00×** | 2.02 of 2 | 101% | 0.19 |
| 3 | 203,914 | 3.14× | 2.99 of 3 | 100% | 0.23 |
| 4 | 262,599 | **4.04×** | 4.03 of 4 | 101% | 0.31 |

Run ascending then descending, eight cases. Repeatability between directions:
**1.2% / 4.5% / 1.5% / 4.8%**.

Correctness on a 2M-order backlog drained to completion: 200 by-symbol keys,
40,000 by-account keys, both aggregations summing to 2,014,695 — the generator's
manifest total, exactly.

### The ceiling, by starving the broker

| broker cap | orders/s | broker util | TM cores used |
|---|---:|---:|---|
| 2.00 | 262,599 | 16% | 4.03 of 4 |
| 1.00 | 273,953 | 31% | 4.03 |
| 0.50 | 272,180 | 56% | 4.02 |
| 0.25 | 229,806 | 100% | 4.00 |
| **0.15** | 134,811 | 100% | **3.30 of 4** |
| 0.10 | *refused* | — | — |

The handover is at 0.15: the broker pins and the task manager **falls off its own
cap**, which is Flink waiting rather than working.

## Judgements it made unprompted

**It chose at-least-once, with a reason.** Its outputs are absolute position rows
keyed by position key, not deltas — so a replay restates a position rather than
double-counting it, and exactly-once buys nothing. It left `--guarantee
exactly-once` available with a per-run transactional-ID prefix if challenged.

**It caught the engine lying.** In the three most-starved cases Flink returned
*identical* accumulated busy / back-pressured / idle counters at both window
edges — impossible for a running subtask, since the three must sum to ~100%. It
prints `stale` rather than the 0.0 that would have read as "no back-pressure"
exactly where back-pressure was the story.

**A refusal stopped the suite**, which is the rule run 1 taught the skill. It
refused twice at `B-broker0.1` — once for a collapsed window, once because two
vantage points disagreed by 14% — then stopped and quoted no number.

## What it sent back into the skill

**Anchor the measurement window on commit boundaries, not wall clock.** The skill
already said to read committed offsets and to hold the checkpoint interval
constant; it did not say those two interact. A source commits *at* checkpoints, so
a wall-clock window quantises the input-side reading and the two vantage points
disagree by an artifact. Fixing it took a 25% disagreement to 0.5%.

**State the backlog rule generally, not by example.** The skill warned about
`compose up` pulling in a dependency chain. This run avoided that trap entirely —
and still lost a 90M-record backlog to `--delete --topic a --topic b` silently
deleting one of them. The rule is now: verify against a manifest after *any*
destructive command.

**Log stderr from the first version.** It lost 25 minutes to a probe dying
silently inside a call with no timeout. The guard caught the consequence; the
cause was invisible until stderr went to a file.
