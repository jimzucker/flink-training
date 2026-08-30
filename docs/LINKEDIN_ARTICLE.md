> **Status:** draft, 30 Aug 2026. Every figure below is in the repo:
> `docs/steps/step-12/units.txt` (laptop), `units-aws.txt` (AWS),
> `docs/reviews/` (the review exchanges), `JOURNAL.md` (per-step record).
> Open item: local runs vary ~12% in absolute throughput; the ratios are stable.

# My benchmark gave me four confident wrong answers before it gave me a right one

I built a Flink pipeline that turns block trades into positions and market
value — exactly-once, verified against real Kafka topics on every push. The
engineering was the easy part. **Measuring it honestly was not.**

---

## The demo was the problem, not the pipeline

I could prove the pipeline scaled. It took 88 million records, 32 vCPUs and a
three-broker MSK cluster to do it — and that teaches the wrong lesson before you
say a word: *Flink needs a cluster to be interesting.*

So I inverted it. Instead of overwhelming Kafka, **shrink Flink.**

Cap the task manager at one core, then two, then four. Same laptop, same single
broker, four minutes a case:

| units | orders/sec | vs previous | Flink cores | broker cores |
|---|---|---|---|---|
| 1 | 30,505 | — | 1.00 of 1 | 0.10 |
| 2 | 65,721 | **2.15×** | 2.00 of 2 | 0.25 |
| 4 | 129,056 | **1.96×** | 3.94 of 4 | 0.39 |
| 8 | 151,969 | **1.18×** | 4.98 of 8 | 0.48 |

It doubles, doubles again, then stops. No cloud account required.

## The lesson that cost me a whole step

An earlier attempt varied parallelism from 2 to 4 and got a flat line. I nearly
wrote "parallelism scaling is not demonstrated" and moved on.

It was flat because **Flink already had every core it could use.** Raising
parallelism just spread the same work over more threads.

> You can only show that something scales when it is the thing that is
> constrained.

That is why a "unit" here is one core *and* one degree of parallelism, bought
together — which is exactly what a KPU is in Managed Service for Apache Flink.

## The broker was asleep, and it was still the ceiling

Look at the last row again. At 8 units Flink reaches only 4.98 of 8 cores.
Something else is in the way — and the broker is at **0.48 cores.** Nearly idle.

It had run out of *write throughput*, not CPU. One order fans out to five
records (four account-side allocations plus one symbol-side position), so
151,969 orders/sec is **759,845 records/sec** — against the ~750,000 one broker
was independently measured to accept.

"The broker is full" would have been the wrong way to say it. It implies a busy
broker. The two cases are only distinguishable with **both CPU columns beside
the throughput**, which is the entire reason they are in the table.

## Was that Flink's ceiling, or my laptop's?

The only honest way to answer was to move the broker. Same script, unchanged,
against MSK:

| | laptop | AWS |
|---|---|---|
| 4 → 8 units | **1.18×** | **1.73×** |
| Flink cores at 8 units | 4.98 of 8 | **7.99 of 8** |
| broker cores at 8 units | 0.48 | 0.62 |

With the broker out of the way, Flink uses everything it is given and the
flattening disappears. The laptop's ceiling was never Flink.

**The prediction was still half wrong.** I had said the *shape* would hold with
the ceiling further right. It didn't: the laptop's curve decays while the AWS
curve improves, and I have no explanation for that yet. It is in the repo as
unexplained rather than explained away.

## The 470-second "cold start" was a bug

At low unit counts the pipeline sat idle for minutes before producing anything.
I built a 240-second warm-up around it and moved on — twice.

The cause: sink transactional-ID prefixes were **fixed strings**. Across ~10
benchmark runs Kafka accumulated **98,677 transactional IDs**, and every sink
startup had to fence all of them before emitting one record.

Scoping the prefix per run: **470s → 31s**, checkpoints in the window **0 → 16**.

I had spent hours tuning a warm-up to hide a defect.

## What I'd actually take away

**Build the harness to refuse.** Mine now stops rather than print a number it
can't stand behind — if the CPU limit wasn't applied to the container, if no job
is running, if the backlog ran out mid-window, if a previous run still owns the
cluster. **Every one of those guards exists because it once produced a confident
wrong answer**, including a full AWS run that measured parallelism alone because
a compose file was missing one line.

**Put the resource columns next to the throughput.** The idle-broker finding is
invisible without them, and "it got slower" is not a diagnosis.

**Repeat the cheap measurement, not just the expensive one.** I ran the AWS cases
twice and they agreed within 2%. I never repeated the laptop cases — and they
turn out to vary ~12%. I trusted the number I'd checked least.

---

*13 steps, one branch each, reviewed before the next. Every figure asserted in
CI on every push — the pipeline stands up from nothing and the expected outputs
are checked against real topics with no tolerances anywhere.*
