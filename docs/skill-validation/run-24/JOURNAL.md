# Journal

## Step 0 — the interview I could not hold

No human was available. The skill asks nine questions one at a time; here they are with
the answer I took, and why.

| # | question | assumption taken |
|---|---|---|
| 1 | What is the input event and what comes out? | A **block trade** (one order filled as a block, to be split across client accounts) arrives on `block-trades`. The job allocates it across the accounts named on the trade and maintains two running positions: **by symbol** on `positions-by-symbol` and **by account** on `positions-by-account`. |
| 2 | Does one input become several outputs? | Yes. Each block trade names **4 accounts**, so one input becomes **1 symbol position + 4 account positions = 5 outputs**. `outputsPerInput = 5`. |
| 3 | What are the keys and how many distinct ones? | 4,096 symbols and 8,192 accounts, both fixed and known, so the verifier can assert exact cardinality. |
| 4 | What has to be exactly right? | The positions are running sums, so a replayed record is a **wrong number**, not a duplicate. Two settings: **exactly-once checkpointing** for the keyed state, **at-least-once sink** made idempotent by emitting the *absolute* position per key with a per-key sequence number. All money is integer cents and all quantities integer shares, so the sums are associative and the verifier can assert them with no tolerance. |
| 5 | Who watches, and what must they believe? | Assumed engineers: correctness first (completeness verified against the input, and against a killed worker), capacity second. |
| 6 | Where does it run? | This laptop. Docker Desktop, 7.8 GB VM, 8 CPUs. |
| 7 | What claim? | *"This block-trade allocation and position pipeline scales linearly from 1 to 2 to 4 cores on one worker: each doubling of cores and parallelism returns at least 95% of double the throughput."* |
| 8 | Which axis? | **One worker growing**: one task manager container capped at N cores, parallelism N, N slots. Not a second JVM. |
| 9 | Which API level? | **DataStream API, hand-written operators.** No SQL, no Table API — as required. |

Unknowns I could not settle and so did not assume away: what the audience's production
broker looks like (the broker here is held still at 2.5 cores and is *not* the thing
being scaled), and whether 4 cores is anywhere near the ceiling of this design.

## Design decisions that are about the pipeline, not the measurement

1. **Integer money.** `pxCents` is a long; notional is `shares × cents`. Floating point
   would make the expected answer depend on the order records arrive in, and the
   completeness gate would have to carry a tolerance. It does not.
2. **Largest-remainder allocation.** The 4 account shares are integers summing to the
   block quantity exactly, so *by symbol* and *by account* are two independent paths over
   the same input that must agree to the share.
3. **Key groups balanced by construction.** Traffic is uniform over symbols and accounts,
   so a keyed subtask's load is proportional to the number of Flink key groups that land
   on it. Random names leave that binomial — with 4,096 symbols over 128 key groups the
   busiest of four subtasks carries ~3% more than the mean, and the penalty *grows with
   parallelism*, which would show up in the table as a scaling shortfall that is really
   data skew. The universes therefore put exactly 32 symbols and 64 accounts in every key
   group. 128 divides by 1, 2 and 4, so every case gives every subtask the same share.
   The job asserts at startup that our key-group arithmetic is Flink's own
   (`KeyGroupRangeAssignment`) for every key, and that each group holds its exact count.
4. **lz4 on the input topic and both sinks.** Standard production practice; it moves work
   onto the worker (the component under test, CPU-capped) and off the broker (held still)
   and cuts the disk the backlog needs. Applied identically to every case.
5. **Unaligned checkpoints with a 5 s aligned timeout.** The measured phase is a fully
   back-pressured drain, and the measurement window is anchored on committed-offset
   boundaries; alignment under back-pressure is what makes those boundaries wander.
6. **A fresh value object per state update.** The heap backend snapshots asynchronously,
   so mutating the object already in state can leak a post-barrier value into a
   checkpoint. One small allocation per output record is cheaper than that bug.

## Step log

### 14:45–14:56 — build, and one guard earning its keep

Job, generator and verifier built against Flink 1.20.1 / JDK 17. The startup assertion
that our key-group arithmetic is Flink's own **fired on the first submit**: our copy of
`MathUtils.murmurHash` was missing `code ^= 4` and had the wrong negative branch
(`SYM000000`: 31 vs 87). Fixed and re-asserted. Without the read-back the universes would
have been balanced across the wrong 128 buckets and the study would have measured skew.

### 14:53–15:19 — scouting (never a published number)

`scout.py` runs the harness's own `run_case` on a 60M-record throwaway topic with short
windows, so the backlogs could be sized from a measured rate and a broker-bound pipeline
would show up in ten minutes rather than inside the chain. Single passes; the harness's
own record says a single pass wanders 3–8%, so these separate "roughly right" from
"wrong", nothing finer.

| memory base | 1 core | 2 cores | 4 cores | GC 1c / 2c / 4c | worker cap 4c | broker 4c |
|---|---:|---:|---:|---|---:|---:|
| 768m | 120,016 | 246,752 | 509,496 | 10.6% / 3.3% / 1.1% | 98.6% | 0.49 of 2.5 |
| 2048m | 122,220 | 262,465 | 503,558 | 5.0% / 1.5% / 0.8% | 101.2% | 0.50 of 2.5 |

Two things this settled:

1. **The worker is the constraint, not the broker.** At four cores the worker sits at
   98–101% of its cap, throttled in 99% of cgroup periods, while the broker uses 0.5 of
   its 2.5 cores and never touches its memory limit. Source idle is 1–3%. The pipeline is
   CPU-bound in the component under test, which is the precondition for the whole study.
2. **`tmMemoryBase` 768m → 2048m**, one variable, all cases measured. It halves the
   baseline's GC (10.6% → 5.0%) and buys the baseline **+1.8%** of throughput. So the
   1-core case was *not* materially heap-starved — worth knowing, because a 10% GC figure
   in the baseline invites the reader to assume it was. Adopted anyway: it balances GC
   across the cases and it makes the baseline faster, which is the direction that makes
   the claim harder to meet, not easier.

Backlogs sized from the measured 4-core rate of ~505,000 rec/s: 300M for the suite, 120M
for the tiny proof, 10M for completeness.

### 15:19–15:28 — completeness, run early as a shakeout

Both arms passed first time: a clean drain and a drain with the task manager killed at
35%. 4,096 symbol positions and 8,192 account positions match the manifest exactly, and
the two paths agree to the share on net, gross and notional.

### 15:28–16:19 — chain 1, `prove.py all --quick` (superseded)

50.5 min, FAIL at `report`. Valid table, both steps short on the interval:
1→2 = 1.968 (lower bound 1.785), 2→4 = 1.861 (lower bound 1.702). Worker at 99.9% of cap
in every case, broker at 20% of its own, so the worker was the constraint and was doing
7% less useful work per core at 4 cores than at 2.

### 16:19–16:33 — diagnosis, one change, a scouting check

Before writing a mechanism down, measured what the host's cores do at all: a register-only
loop doubles at 98% of linear on both steps; a random 4 MB read-modify-write doubles at
~99% from 1→2 and only 69% from 2→4, reproduced. Memory traffic is where this host stops.
Counted the pipeline's own allocation at ~3.5 KB per input record, of which ~2.5 KB was the
output encoder. Replaced it with a direct byte-buffer writer (identical bytes out), plus
skipping the unused `blockId` String and reusing the account/weight arrays. A scouting
check at matched conditions: 4-core rate per CPU-second used 124,366 → 131,258 (+5.5%),
2-core unchanged.

### 16:33–17:23 — chain 2, `prove.py all --quick` (accepted)

49.5 min, FAIL at `report` — but only on 2→4. 1→2 = 2.040 (lower bound 2.016) **meets the
claim**; 2→4 = 1.906, 95.3% of linear as a point estimate, lower bound 1.822 = 91.1%, so
it does not. Per-case spreads fell to 1.8–2.7% from 4.3–4.7%. Stopped here rather than
re-rolling the same build or rebalancing worker memory in the big case's favour.

### 17:23–17:25 — teardown

`prove.py down`: 23.1 GiB trimmed back to the host, nothing with the `bt25` prefix
surviving (containers, volumes, networks all zero), no host process left watching the run.
