# Clean-room validation of `prove-it-scales`

The skill claims to carry a method. The only way to know is to give it to
something that does not already know the answer.

## The setup

A fresh agent, an empty directory, and the original problem statement. It was
barred from reading this repository — where the answers live — and allowed to
read exactly one file: the skill. No human was available to it, so where the
skill says to interview, it recorded the questions it would have asked and the
assumption it took instead.

Scope was cut to what the scaling claim needs: a generator, a Flink job with both
aggregations, and a measurement harness. No dashboards, no market value, no deck.

**Model: Claude Opus 5.** Fully autonomous, one prompt, no human input after it
started.

## What it cost

| | |
|---|---:|
| Human time | **0 h** |
| Human prompts | **1** |
| Wall clock | 2.52 h |
| Tool calls | 863 |
| Assistant turns | 1,002 |
| Tokens | 258,086,945 |
| Cost, metered-API equivalent | $166.58 |

One prompt, then two and a half hours unattended.

## What it produced

60,000,000 block trades pre-loaded, producer stopped, then drained. Its own
choices: 12 partitions, 30s checkpoints, exactly-once, 10× fan-out.

| cores | trades/s | vs 1 core | TM cores used | broker |
|---|---:|---:|---|---|
| 1 | 54,029 | 1.00× | 0.99 of 1 | 0.32 of 2 |
| 2 | 128,007 | 2.37× | 2.00 of 2 | 0.55 |
| 3 | 179,827 | 3.33× | 3.01 of 3 | 0.61 |
| 4 | 242,983 | **4.50×** | 4.02 of 4 | 0.64 |

Fan-out verified at exactly 10.0× in every case. Repeatability reported per case
— 3.0% / 6.7% / 14.7% / 5.4% — and it quoted ratios rather than absolutes
because of it.

## What it taught the skill

Four things went back into `prove-it-scales` because the run earned them:

**Starve the other component to find the ceiling.** Capped at 4 cores, it could
not reach the ceiling by scaling up — so it squeezed the broker instead: 2.0
cores → 1.0 costs 2%, → 0.5 costs 19% and the roles swap, broker pinned at 98%
while the task manager falls off its own cap to 3.84. This project found the same
answer by scaling up, which took AWS.

**Read the numbers from the transport, not the engine.** At 100% CPU the engine's
metric service is starved along with everything else and under-reported its own
output by ~3×.

**Memory arithmetic under exactly-once.** An early 1-core result of 3,085 rec/s
was garbage collection, not throughput — producer buffers eating the task heap.

**Infrastructure commands that destroy the backlog.** `compose up` of one service
brought up its dependencies and took 15.6 GB of backlog with it.

It also refused three times, each correctly — including once on a *good* case,
because its own measurement was reading uncommitted offsets. It fixed the
measurement, re-ran the whole suite, and discarded the pre-fix numbers.

## The prompt it was given

Verbatim, minus formatting:

> Build a working system from scratch and measure it. Work ONLY in the empty
> directory. Do NOT read, list, or open anything under `flink-training` — it
> contains a prior solution and reading it invalidates everything. You MAY read
> the skill; read it first and follow it. Do not ask questions — write down the
> questions you would have asked and the assumption you are proceeding on.
>
> *[the original problem statement]*
>
> Build only what the scaling claim needs. Skip market value, prices, dashboards,
> slides, extensive docs. Docker is available and all ports are free. Java 25 is
> default and Flink needs 17. Do not request more than 4 cores for any case.
> Deliver: the questions and assumptions, the scaling table, your conclusions
> including where the ceiling is, and anything the harness refused and why.
