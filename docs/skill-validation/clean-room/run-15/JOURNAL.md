# Journal

No human was available, so §1 of the skill (interview one question at a time)
was answered by assumption. The questions and the answers I took are below,
followed by what drove each build decision.

## The interview I could not have

| # | question | assumption taken in place of an answer |
|---|---|---|
| 1 | What is the input event, and what comes out? | In: a **block trade** — symbol, side, quantity, price, and the allocation instruction it was booked with (four accounts, each with a share in basis points summing to 10,000). Out: the **running net position by symbol** and the **running net position by account and symbol**, both onto Kafka. |
| 2 | Does one input become several outputs? | Yes, **5**: one position-by-symbol record plus one position-by-account record per allocation leg (4 legs). Exact, not average — the write side is five times the read side. |
| 3 | What are the keys, and how many distinct ones? | **32 symbols, 128 accounts** → 32 symbol keys and 32 × 128 = **4,096** account-symbol keys. Fixed and small on purpose, so the verifier asserts exact key counts rather than tolerances. |
| 4 | What has to be exactly right? | Both positions are running sums, so a replayed record is a wrong number, not a duplicate. **State: exactly-once checkpointing. Sink: at-least-once, made idempotent by emitting the absolute position for the key on every update** (no transactions, so no commit-interval latency floor). Tested by killing a worker mid-drain. |
| 5 | Who watches, and what must they believe? | Engineers sizing capacity. The user asked for the fast look, not the publishable table, so the answer they must come away with is *"the rig runs clean and this is roughly how fast"* — explicitly not *"the ratio is proven"*. |
| 6 | Where does it run? | Given: this laptop, macOS arm64, Docker Desktop, JDK 17. |
| 7 | What claim do you want to make? | Would have been *"this pipeline scales close to linearly on one worker from 1 to 2 to 4 cores"*. **A one-pass run cannot make it** — see the harness's own banner in the report. |
| 8 | Which axis? | Given: **one worker growing** — one task manager container capped at N cores, parallelism N, N slots. |
| 9 | Which API level? | Given: **Flink DataStream API, hand-written operators**. No SQL, no Table API. |

Sizing questions I also had to answer alone, and got wrong twice before the
tiny proof measured the rate for me. Final values: suite backlog
**400,000,000** records (≈ 26 GB, far larger than the ~2 GB of page cache this
Docker VM can hold), completeness backlog **16,000,000** (drains to the last
record and spans ~10 checkpoint intervals at the baseline rate; the kill landed
at 5,799,124), tiny-proof backlog **160,000,000**, checkpoint interval 10 s,
8 partitions held constant across every case.

I first sized these for ~450k rec/s at 4 cores, from nothing but a guess. The
4-core case actually runs at ~970k. The first `all --quick` was aborted when
the tiny proof revealed it (160M would very likely have drained under the
4-core window); the second failed the tiny proof outright with 6.9 s of
headroom against a 10 s checkpoint interval. Both were my sizing, both were
caught before the fill, and both are the reason the run took 1 h 51 m rather
than 40 min. **Measure the rate before sizing the backlog** — the tiny proof
is where that number is free.

## Build

One jar, four classes, and nothing that measures:

* `Trades` — the domain. A block trade is a pure function of its id and the
  seed, and the **allocation rule** (pro-rata on basis points, rounding
  remainder to the last leg, so the legs sum to the block quantity exactly)
  lives here.
* `GenerateBacklog` — deterministic fill, one thread per partition group,
  record *i* always on partition *i mod 8* with the content `Trades.fill(i)`
  gives it. It also writes the **manifest**, which replays the allocation rule
  over the input ids: the expected answer comes from the input and never from
  the pipeline.
* `PositionsJob` — the job. `Source: kafka-source -> allocate` in one vertex;
  the main output is hashed by symbol into `position-by-symbol -> sink-by-symbol`
  and the side output is hashed by (account, symbol) into
  `position-by-account -> sink-by-account`. Three vertices, two HASH edges, the
  same shape at parallelism 1, 2 and 4 — so the baseline is not a chained
  special case, which the harness re-checks off the running plan every case.
* `VerifyCompleteness` — reads both sinks to their end offsets, takes the last
  value per key (one key is produced by one keyed instance and hashed to one
  partition, so partition order is the key's order) and compares to the
  manifest with **no tolerances**: key counts, every position, every update
  count, the two paths agreeing per symbol, and the totals.

The measurement is entirely the shipped harness. I wrote no sampler, no suite
runner, no spread rule and no report table.

## Decisions forced by this rig

* **Docker Desktop has 7.65 GiB.** `harness/README.md` says to give the broker
  4 GiB, measured against a 2 GiB arm that thrashed. Raising the VM's memory
  was blocked by this environment, so the stack had to fit as it is:
  broker `mem_limit` 5 g with a **1 G heap** (Kafka wants page cache, not
  heap), task manager 2560 m process / 4 g limit, job manager 2 g (fixed by
  the harness). That keeps anonymous memory near 5 GB and leaves ~2 GB of page
  cache — about what the README's clean arm measured — and puts the broker's
  own cgroup limit far above anything it reaches, so the limit-hit guard is
  measuring the broker and not my container sizing. **This is a difference
  from the configuration the README measured, and I did not test it against
  the 4 GiB / 3 G arm; the guard was live in every case and did not fire.**
* Cases 1, 2, 4 with baseline 1, because the user asked for **both** step
  ratios. §5 of the skill would otherwise have dropped the one-core case.
* `passes: 3` in `pipeline.json`; `--quick` overrides it to 1, which is what
  makes the table unpublishable.
