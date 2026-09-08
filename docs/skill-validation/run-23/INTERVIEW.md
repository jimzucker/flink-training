# Interview — questions asked of nobody, and the assumptions taken instead

No human was available. Section 1 of the skill says to interview one question at a
time before building; below is what I would have asked, the default I offered, and
the assumption I took in place of an answer. Every assumption is a decision the
reader can overturn, and each one is visible in `pipeline.json` or the job source.

| # | question (skill §1) | assumption taken |
|---|---|---|
| 1 | What is the input event, and what comes out? | A **block trade** — one execution done for a desk against an allocation scheme — arrives on `block-trades` as a FIX-style tag=value record. The job allocates it pro-rata across the accounts in its scheme and maintains two running positions: **net position per symbol** and **net position per account**, each written back as the *absolute* current position for that key. |
| 2 | Does one input become several outputs? | **Fan-out 5**: 4 account allocations + 1 symbol update per block trade. `outputsPerInput: 5`, and the harness's two-vantage guard divides sink growth by it. |
| 3 | What are the keys, and how many distinct ones? | **4,096 symbols** and **8,192 accounts** (2,048 allocation schemes × 4 accounts). Both fixed and fully covered by the generator, so cardinality is arithmetic and the verifier asserts it with no tolerance. |
| 4 | What has to be exactly right? | A position is a running sum, so a replayed record is a *wrong number*, not a duplicate. Two settings: **exactly-once checkpointing** for the keyed state, **at-least-once sink** made idempotent by emitting the absolute position per key. Tested by killing the worker mid-drain (completeness, kill arm). |
| 5 | Who watches, and what must they believe? | Engineers. Correctness first (no allocation lost or double-counted, quantity conserved across both paths), then the step ratios. |
| 6 | Where does it run? | This laptop, in Docker, as the task states. Docker Desktop VM: 7,838 MiB, 8 CPUs. |
| 7 | What claim do you want to make? | *"This block-trade allocation and position pipeline gets faster roughly in proportion to the cores given to one worker, from 1 to 2 to 4."* — and because this run is `--quick`, the table produced **is not evidence for that claim**; it is a smoke look. |
| 8 | Which axis? | **One worker growing**: one task manager container capped at N cores, parallelism N, N slots. Not a second JVM. |
| 9 | Which API level? | **Flink DataStream API, hand-written operators** — no SQL, no Table API, as instructed. |

Further assumptions taken without a question to hang them on:

- **8 partitions** on the input topic (divides evenly by 1, 2 and 4).
- **10 s checkpoint interval**, hashmap state backend, filesystem checkpoint storage.
- **Broker capped at 2.5 cores**, job manager at 0.5 — held still across every case.
- Worker memory **768m base + 1280m per subtask** (limit 1600m per subtask), so every
  case gives each subtask the same memory; broker 4 GiB with a 3 GiB heap. These are
  the harness's own recorded figures for this host.
- Record sizes: input ≈ 76 B of payload, each output ≈ 40 B, so the sink side carries
  roughly 3× the input bytes.
- Prices are integer cents and quantities integer shares, so allocation conserves
  quantity **exactly** (slot 0 absorbs the pro-rata remainder) and every assertion in
  the completeness run is an equality, not a tolerance.
