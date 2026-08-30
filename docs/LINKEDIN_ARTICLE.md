> **Status:** draft, 30 Aug 2026. Figures from `docs/steps/step-12/units.txt`
> (one laptop run) and `units-aws.txt`. Reproduce with `scripts/scale-units.sh`.

# You don't need a cluster to prove Kafka and Flink scale linearly

You need a laptop, one broker, and about four minutes per data point.

**Claude Code built the whole thing** — the pipeline, the test suite, the
measurement harness, the CI, the dashboards and the deck. It cost **about six
hours of my own time at the keyboard.** I directed and reviewed; more on that
below, but the headline first.

The pipeline turns block trades into positions and market
value — exactly-once, every output verified against real Kafka topics. Then I
measured what one more core actually buys. **The answer is: almost exactly one
more core's worth.**

| units | orders/sec | vs previous | Flink cores used | broker cores |
|---|---|---|---|---|
| 1 | 30,505 | — | 1.00 of 1 | 0.10 |
| 2 | 65,721 | **2.15×** | 2.00 of 2 | 0.25 |
| 4 | 129,056 | **1.96×** | 3.94 of 4 | 0.39 |

Four times the resources, **4.23× the throughput.** On a laptop. One Kafka
broker. `docker compose up`.

At 4 units that is 129,056 orders/sec in and **645,280 records/sec out** — every
order fans out to five records, four account-side allocations plus one
symbol-side position.

---

## What makes it a measurement and not a demo

Three things, and they are the whole difference:

**A unit is one core *and* one degree of parallelism, bought together.** That is
exactly what a KPU is in Managed Service for Apache Flink, so the number means
something when you go to price it. Raising parallelism without cores just spreads
the same work over more threads — it looks like scaling and measures nothing.

**Flink used every core it was given** — 1.00, 2.00, 3.94 of 1, 2 and 4. That
column is the proof the units were real. If parallelism were outrunning the cores
behind it, throughput would stall while the column flattened.

**The broker stayed under half a core the entire time.** Flink was unambiguously
the constrained component, which is the precondition for the claim: you can only
show that something scales when it is the thing that is constrained.

Partitions were held at eight for every case, and each case drained a
50-million-order backlog with the producer stopped. Nothing varies but the units.

## Where it stops, and why that is also useful

Push to 8 units on the same laptop and the curve flattens to 1.18×. Flink reaches
only 4.98 of its 8 cores — and the broker is at **0.48 cores.** Nearly idle,
while being the thing in the way.

It had run out of *write throughput*, not CPU: 759,845 records/sec against the
~750,000 a single broker accepts. A broker that looks asleep can still be the
ceiling, and you can only tell with both CPU figures next to the throughput.

That is a property of one laptop's Kafka, not of Flink. The same script against a
two-broker MSK cluster returns **1.73×** for that same step, with Flink using
7.99 of its 8 cores.

## Claude built it; I decided what "done" meant

Thirteen steps, each on its own branch, each reviewed before the next started.
**223 prompts and 20 pull requests, across about 20 hours of active session
time — roughly six of which were mine.** The rest was Claude working while I did
something else. Claude wrote the Flink jobs, the generators, the verification
suite, the Terraform, the benchmark harness, the CI pipeline and the slides.

Six hours is the number I keep coming back to. Not because the code appeared for
free, but because the expensive part of work like this has never been the typing
— it is standing up real Kafka, real Flink, a dashboard, a load harness and a
correctness suite that all agree with each other. That is what used to consume
the weekend.

What I actually did was decide what counted as proof — and push back when the
output was confident and wrong.

That mattered more than it sounds. At one point the benchmark reported a clean
scaling curve from an AWS run where the CPU limit had never been applied to the
container: four numbers that measured parallelism alone. At another, a
"cold start" was written up as an inherent warm-up cost when it was a defect —
sink transactional IDs accumulating across runs until startup had to fence
98,677 of them. The fix took first output from 470 seconds to 31.

**Neither was caught by the model volunteering doubt. Both were caught by asking
what the number actually measured.** That is the job now: not typing the code,
but refusing to accept a plausible result.

The harness that came out of it refuses on your behalf — it stops rather than
print a figure if the CPU limit was not applied, if no job is running, or if the
backlog ran out inside the measurement window.

## Why this matters more than the number

Every capacity conversation I have ever sat in starts with "we'll need to size a
cluster to find out." **You can answer most of it before you provision anything.**

Shrinking Flink is far cheaper than overwhelming Kafka, and it produces the same
curve. The expensive version of this experiment took 88 million records, 32 vCPUs
and three brokers. This version runs on the machine you are reading this on.

---

**Reproduce it:** `scripts/scale-units.sh` — 2 and 4 units by default,
`UNITS="1 2 4 8"` for the full curve. Flink 1.20, Kafka 3.9, Java 17, all in
Docker. Exactly-once delivery, proved by killing a task manager mid-run and
watching the totals still reconcile. Every expected output asserted in CI on
every push, with no tolerances anywhere.

*One honest caveat: repeated laptop runs vary about 12% in absolute throughput.
The ratios hold.*
