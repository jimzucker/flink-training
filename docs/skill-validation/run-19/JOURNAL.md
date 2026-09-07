# Journal

## The interview I could not hold

No human was available. The skill says to ask one question at a time and stop
when the claim can be stated; these are the questions, the answer I assumed,
and why. Everything here is an assumption, not an answer.

| # | question | assumption taken | why |
|---|---|---|---|
| 1 | What is the input event and what comes out? | A block trade (`tradeId, symbol, side, qty, priceCents, ts`) arrives on `block-trades`; the job allocates it across accounts and emits the running position by symbol on `positions-by-symbol` and by account on `positions-by-account`. | It is the task, restated in domain language. |
| 2 | Does one input become several outputs? | Yes, exactly 5: one by-symbol update plus one per allocation leg, and there are 4 legs. The fan-out is fixed, so the write side is 5× the read side and the harness's two-vantage guard has an exact divisor. | A varying fan-out would make the vantage check a tolerance instead of an assertion. |
| 3 | What are the keys and how many? | 64 symbols, 512 accounts, both fixed. | Small fixed cardinality makes the completeness answer arithmetic: 64 and 512 keys exactly, no tolerance. |
| 4 | What has to be exactly right? | A position is a running sum, so a replayed record is a wrong number, not a duplicate. Two settings: **exactly-once checkpointing** for the keyed state, **at-least-once sink** made idempotent by emitting the absolute position per key. | The skill's own default, and it is what the kill test in `completeness` actually exercises. |
| 5 | Who watches, and what must they believe? | Engineers: that nothing was lost and that the step ratio is honest. | Drove the choice to run completeness with a killed worker rather than only a throughput table. |
| 6 | Where does it run? | This laptop, in Docker, everything prefixed `bt20`. | Stated in the task. |
| 7 | What claim do you want to make? | *"This pipeline, on this laptop, moves N block trades per second per core of task-manager CPU, and buying a core buys about R× more."* Quick look only — the harness marks the table unpublishable, so the claim is not made. | The task asked for the fast look, not the publishable table. |
| 8 | Which axis? | One worker growing: one task-manager container capped at 1, 2 and 4 cores, parallelism = cap = slots on every case. | The laptop proxy; a second JVM was not in scope. |
| 9 | Which API level? | Flink 1.20 DataStream API, hand-written operators. No SQL, no Table API. | Stated in the task. |

Open question I could not settle and did not guess at: whether the audience
cares about the 1-core case at all. The skill says that if the claim starts at
two units, the one-unit case should not be run — it is the structurally
weakest case. The task asked for 1→2 explicitly, so 1 core was run.

## Steps

1. **Build.** `job/` — one jar carrying the Flink job, the deterministic
   generator and the completeness verifier. Graph: source → parse → allocate,
   then two hash edges into the two keyed aggregations, each chained to its
   sink. Three vertices at every parallelism, because a `keyBy` is a hash edge
   at parallelism 1 as much as at 4 — so the baseline is the same graph as
   every other case, which is the difference the skill measured as 2.16× vs
   3.26×.
2. **Calibrate before sizing.** 20M records on a throwaway topic, drained once
   at 4 cores and once at 1 core, reading the sink's log-end offsets:
   ~700k rec/s at 4 cores, ~190k rec/s at 1 core with the container pinned at
   1.00 of 1.00 cores. Backlogs were then sized from those rates rather than
   guessed: suite 240M, tiny proof 110M, completeness 10M.
   The same drain was verified end to end (64 + 512 keys, both paths summing
   to the manifest exactly) before any harness command was allowed to matter.
3. **Measured the task-manager deregistration.** The engine took 55 s to drop
   a removed worker; `stop_tm` allows 90 s. Measured because an earlier
   observation looked like ~3 minutes, which would have failed every case.
   One rig, one variable, both read back — the 3-minute figure was my own
   uninstrumented wall clock and is not evidence of anything.
4. **`prove.py all --quick`**, waited on `results/DONE`.

## What I did not do

- No dashboard. Section 7 is about explaining a number to an audience; this
  run is a smoke test whose own table says it is not a result, and a panel I
  cannot tie to the window would be a liability rather than an explanation.
- No `ceiling` run. Where the broker becomes the constraint is not answered
  here.
- The harness was used verbatim. Nothing in `lib.py` or `prove.py` was edited,
  and no threshold was touched.
