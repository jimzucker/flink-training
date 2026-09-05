# The interview that could not happen

The skill says to interview one question at a time and wait for the answer. No
human was available, so here are the questions I would have asked, the default
I offered myself, and the assumption I took in place of an answer. Every one of
these is an assumption, not an agreed requirement.

| # | question | assumption taken |
|---|---|---|
| 1 | What is the input event, and what comes out? | A **block trade** — symbol, side, quantity, price, and the accounts it is allocated across — arrives on `block-trades`. Out come two running positions: the net position for the **symbol**, on `positions-by-symbol`, and the net position for each **account** the block was allocated to, on `positions-by-account`. |
| 2 | Does one input become several outputs? | Yes, **4 outputs per input**: one symbol position update, and one account position update per allocation leg. Three legs per block. The write side is four times the read side, and it is the side that fills the disk. |
| 3 | What are the keys, and how many distinct ones? | **1024 symbols** and **3072 accounts**, both fixed and declared up front, so the completeness check asserts exact counts rather than tolerances. The counts are chosen so each key on either book receives the same update rate (N/1024), which keeps the keyed load even across subtasks instead of leaving it to where the hash lands. |
| 4 | What has to be exactly right, and with which two settings? | Positions are running sums, so a replayed record is a wrong number, not a duplicate. **State: exactly-once checkpointing. Sink: at-least-once, made idempotent by emitting the absolute position for the key rather than a delta.** Tested by killing the worker mid-drain and re-asserting every total. |
| 5 | Who watches, and what must they believe? | Assumed **engineers**: correctness first (nothing lost, nothing double-counted), capacity second. |
| 6 | Where does it run? | This laptop, in Docker: one broker, one job manager, one task manager, all capped. |
| 7 | What claim do you want to make? | *"This block-trade allocation and position pipeline, on one worker capped at 1, 2 and 4 cores, scales at these step ratios."* The user asked explicitly for a **quick look**, one pass per case, so the number is **not** a publishable claim — see the banner on the table. |
| 8 | Which axis? | **One worker growing**: a single task manager container capped at N cores, parallelism N, N slots. Not a second JVM. |
| 9 | Which API level? | **Flink DataStream API, hand-written operators.** No SQL, no Table API — stated in the task. |

Two more that the harness forced answers to:

| question | assumption taken |
|---|---|
| Which cases? | **1, 2 and 4 cores**, baseline 1. §5 of the skill says not to run the one-core case when the claim starts at two — the task asks for the 1→2 step, so it is run, and it is the case the record says is noisiest. |
| How many passes? | The task asks for `--quick`: **one pass per case**, plus the sentinel (the baseline once more at the end). One pass measures no spread, so the harness stamps the table unpublishable. |
