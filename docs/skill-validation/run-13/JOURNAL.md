# Journal

## Step 0 — the interview that could not happen

No human was available. The skill says to interview one question at a time; the
questions I would have asked, the default I offered myself, and the assumption I
took in place of an answer:

| # | question | assumption taken |
|---|---|---|
| 1 | What is the input event and what comes out? | A **block trade** (symbol, side, quantity, price, venue, strategy) arrives on Kafka. The job **allocates** it across 4 accounts by a fixed weight schedule (40/30/20/10 %) and maintains two running positions: **net position per symbol** and **net position per account**, each written back to Kafka as the *absolute* position for that key. |
| 2 | Does one input become several outputs? | Yes, **5×**: 1 symbol-position update per trade + 4 account-position updates (one per allocation). `outputsPerInput: 5`. The write side is five times the read side; that is where the disk and the broker load land. |
| 3 | What are the keys and how many distinct ones? | 200 symbols, 2 000 accounts — fixed, so the completeness run asserts **exact** cardinality and **exact** per-key sums, no tolerances. |
| 4 | What has to be exactly right? | A running position is a sum, so a replayed record is a wrong number. Two settings: **exactly-once checkpointing** for the keyed state, **at-least-once sink** made idempotent by emitting the absolute position per key (no transactions, no commit-interval latency floor). Tested by killing the worker mid-drain. |
| 5 | Who watches, and what must they believe? | Assumed a mixed audience: engineers who want the correctness gate, and a capacity reader who wants the step ratio. Both are in the report. |
| 6 | Where does it run? | The laptop, in Docker, as instructed. 8 CPUs and 8 GB in the Docker VM. |
| 7 | What claim do you want to make? | *"This pipeline — block-trade allocation with two keyed running-position aggregations — scales linearly on one growing worker from 1 to 2 to 4 cores, at 8 Kafka partitions and a 10 s checkpoint interval."* |
| 8 | Which axis? | **One worker growing**: one task manager container capped at N cores, parallelism N, N slots. The laptop proxy, not a second JVM. |
| 9 | Which API level? | **Flink DataStream API, hand-written operators.** No SQL, no Table API — as instructed. |

Cases the claim needs: 1, 2 and 4 cores (the headline is the two step ratios
1→2 and 2→4), baseline 1, 3 passes each plus the sentinel.

Further assumptions, stated because nobody could confirm them:

- Symbols are drawn **uniformly** at random. Real block-trade flow is skewed;
  a skewed key distribution would put a scaling story on top of a partitioning
  story, so the generator is uniform and the claim is scoped to that.
- The allocation schedule is fixed (4 accounts, 40/30/20/10) and quantities are
  always multiples of 100, so every allocation is an exact integer and the
  expected answer is arithmetic, not a tolerance.
- The broker gets 2.5 cores and the job manager 0.5 throughout, held still
  across every case; the worker is the only thing that changes.

## Step 1 — build

`job/` is a single shaded jar with three entry points, so the generator, the
verifier and the job all move together and one build hash covers the table:

- `scaletest.PositionsJob` — the DataStream job
- `scaletest.GenerateBacklog` — the deterministic backlog generator + manifest
- `scaletest.VerifyCompleteness` — the no-tolerance verifier

The allocation rule is written **twice** on purpose — once in the generator (to
predict per-account totals) and once in the job (to compute them). Sharing one
method would have made the per-account assertion check the pipeline against
itself.
