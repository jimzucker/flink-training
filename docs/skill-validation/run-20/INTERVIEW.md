# The interview that did not happen

The skill opens by saying: ask one question at a time, wait for the answer, do
not start building until the claim and the fan-out are known. There was no
human available, so every question below was answered by an assumption. Each
assumption is stated with the reason it was chosen, so a reader can see exactly
which of them would change the build if it were wrong.

| # | question (skill §1) | assumption taken instead | why, and what would change if it is wrong |
|---|---|---|---|
| 1 | What is the input event, and what comes out? | A **block trade** on Kafka: sequence number, symbol, signed quantity, price in cents, and an allocation instruction naming three accounts and the quantity going to each. Out: a **running position per symbol** and a **running position per account**, each written to its own Kafka topic. | This is the spec. The allocation instruction rides on the input record on purpose: the expected answer for every account is then computable from the input alone, never from the pipeline (§4). If the intended design were "the job decides the allocation from a rule", the verifier's per-account expectation would have to reimplement that rule. |
| 2 | Does one input become several outputs? | **Fan-out 4**: one symbol-position record plus three account-position records per block trade. Three accounts per block. | Fan-out decides where the load lands. The broker is capped at 2.5 cores against a worker of up to 4; the skill's own example config uses fan-out 8, which on this rig would very likely make the broker the ceiling instead of the worker, and the study would then be measuring the wrong component. Fan-out 4 keeps the write side about 2× the read side. |
| 3 | What are the keys, and how many distinct ones? | **512 symbols** and **4096 accounts**, fixed. | Small fixed key counts make the outputs arithmetic. The verifier asserts the observed cardinality equals these two numbers with no tolerance, so a key that was never intended, or one that never arrived, is a failure and not a rounding difference. |
| 4 | What has to be exactly right? | **Two settings, not one.** State: **exactly-once checkpointing**. Sink: **at-least-once, made idempotent by emitting the absolute position per key** — a replayed record rewrites the same number instead of adding a second one, so no transactions and no commit-interval latency floor. | Tested, not asserted: the completeness run drains the same backlog twice, once cleanly and once with the task manager killed 35% of the way through, and both arms must match the manifest exactly. |
| 5 | Who watches the demo, and what must they believe? | **Engineers.** Correctness is gated first (no table is published for a build that has not passed completeness), capacity second. | A capacity-for-managers build would have spent its time on a dashboard instead of on the verifier. |
| 6 | Where does it run? | **This laptop**, in Docker: one broker, one job manager, one task manager per case, everything prefixed `bt21`. | Default per the skill. |
| 7 | What claim do you want to make? | The user asked explicitly for **the fast look, not the publishable table**: "how does it scale across 1, 2 and 4 cores, the two step ratios". So the claim under test is *"this pipeline's throughput scales with the cores given to one task manager"* — and the run is deliberately not the run that would establish it. | This is why the chain was run with `--quick`, and why the numbers below are labelled unpublishable throughout. |
| 8 | Which axis is the claim about? | **One worker growing**: one task manager container capped at N cores, parallelism N, N slots — the laptop proxy, not a second JVM. | Recorded as a header field. On every case the harness reads the cap back off the container, the slots off the engine, and the vertex parallelism off the running plan, and refuses if the three disagree. |
| 9 | Which API level? | **Flink DataStream API, hand-written operators.** No SQL, no Table API. | Given in the task. |

## The one place this run knowingly departs from the skill

§5 says: *"If the claim is a step from two units up, do not run the one-unit
case at all — it is the structurally weakest case and the noisiest in every run
that repeated it."* The task asks for **both** step ratios, 1→2 and 2→4, so the
one-core case is run anyway. It is reported with that caveat attached rather
than quietly dropped.

The related trap the skill names — a baseline that chains into one vertex with
no shuffle, which flatters the 1→4 ratio — does not apply here: the job graph
is three vertices joined by two hash edges at every parallelism, and the harness
reads the shape off the running plan for every case and refuses any row whose
shape differs from the others.
