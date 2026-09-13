# Journal — block-trade allocation & position keeping, proved across 1/2/4 cores

## Step 0 — the interview that could not happen

The skill says: ask one question at a time, wait for the answer. There is no human
in this run. So the questions are written down here with the assumption taken in
place of each answer. Every assumption below is a decision the report is judged
against, not a detail.

| # | question I would have asked | assumption taken |
|---|---|---|
| 1 | What is the input event, and what comes out? | **A block trade** (`seq, symbol, qty, price, four account ids`) arrives on Kafka. The job **allocates** the block across those four accounts, then maintains **two running positions**: net position per **symbol**, and net position per **(account, symbol)**. Both are written back to Kafka. |
| 2 | Does one input become several outputs? | **5 outputs per input**: 1 symbol-position update + 4 account-position updates. Fan-out 5 — the write side is five times the read side, and it is what fills the disk. |
| 3 | What are the keys, and how many distinct ones? | 64 symbols, 128 accounts, 4 accounts per trade → **64 symbol keys** and **8,192 (account, symbol) keys**. Small and fixed, so the completeness answer is exact arithmetic, not a tolerance. |
| 4 | What has to be exactly right? | A position is a running sum, so a replayed record is a **wrong number**, not a duplicate. Two settings, not one: **exactly-once checkpointing** for the keyed state, **at-least-once sink** made idempotent by emitting the *absolute* position per key (the last value per key is the answer, so a replayed suffix rewrites the same final value). Tested by killing the worker mid-drain. |
| 5 | Who watches, and what must they believe? | Engineers. They must believe (a) nothing was lost or double-counted, and (b) the throughput number was taken while the worker — not the broker, not memory — was the constraint. |
| 6 | Where does it run? | This laptop, in Docker. 7,838 MiB VM, 8 CPUs. |
| 7 | What claim do you want to make? | Verbatim: **"This block-trade allocation and position-keeping pipeline scales linearly from 1 to 2 to 4 cores on one Flink task manager: each doubling of cores and parallelism returns at least 95% of double the throughput."** |
| 8 | Which axis? | **One worker growing** — a single task manager container capped at N cores, parallelism N, N slots. Not a second JVM. Recorded in the results header. |
| 9 | Which API level? | **DataStream API, hand-written operators.** No SQL, no Table API — the caller asked for it, and the job graph is then a graph I own rather than a plan a planner may change between versions. |

Two more assumptions the interview would have surfaced:

- **Cases 1, 2 and 4.** §5 of the skill says that if the claim is a step from two
  units up, the one-unit case should not be run at all — it is the structurally
  weakest and noisiest case. The task explicitly asks for 1, 2 and 4, so the
  one-core case is run, and the report leads with **2→4** and quotes **1→2**
  beside it.
- **`--quick`.** The task asks for `prove.py all --quick`. The harness stamps
  every table that mode produces `publishable: false` — two passes per case
  instead of three. The report says so at the top and does not launder it.

## Step 1 — build

One Maven module, `job/`, producing one jar used three ways (job, generator,
verifier) so every number in the table comes from one build hash.

- `scaletest.PositionsJob` — the DataStream job.
- `scaletest.GenerateBacklog` — deterministic generator + manifest writer.
- `scaletest.VerifyCompleteness` — reads both sinks, compares to the manifest
  with no tolerances.

Job graph (read back off the running plan by the harness, identical at every
parallelism because `keyBy` forces a hash exchange even at parallelism 1 — the
baseline is a case, not a chained special build):

```
V1  Source: kafka-source -> parse -> allocate
V2  position-by-symbol  -> sink-by-symbol   (HASH from V1)
V3  position-by-account -> sink-by-account  (HASH from V1)
```

I predicted four vertices — that `parse` having two outgoing edges would stop
Flink chaining `allocate` onto it. **Wrong**: Flink 1.20 chains on the
*downstream* operator having a single input, not on the upstream having a
single output, so `allocate` chained into the source task. The plan was read
back off the running job rather than assumed, which is the only reason the
prediction was corrected instead of published. Three vertices, and both keyed
edges are HASH exchanges at parallelism 1 as well as 2 and 4 — so the baseline
is a case, not a chained special build, and the harness's shape guard confirmed
the signature was byte-identical across all seven measured cases.

## Step 1b — three probes before the chain, because a guess costs a chain

The harness README is explicit that a backlog sized by guess, a starved broker
and a worker capped on memory are what clean-room runs lose attempts to. So
before `all --quick` was launched, four short probes were run against the real
stack with the real jar. They measure nothing that is reported; they exist to
pick numbers.

| probe | what it measured | what it decided |
|---|---|---|
| drain rate, 4 cores | 658–672k rec/s steady | backlog 200M covers ~920k rec/s; `tinyCount` 130M |
| cap ownership, 4 cores | **worker 85.4% of cap**, broker hitting its 4 GiB limit 5,139 times, 288 B of sink per input | the broker was the constraint — not publishable |
| same, after `compression.type=lz4` on the sink | worker **99.3%** of cap, sink 167 B/input | one change; the fan-out-5 write side moved off the broker and its cost onto the worker, which is the component under test |
| same, after `kafkaHeap` 3G → 1G inside the same 4 GiB container | broker limit hits **0**, page cache 1.9 GB (was 0.96 GB) | a 3 GiB heap in a 4 GiB cgroup left the broker no page cache |
| GC, 1 core | **11.0%** of capacity — exactly the harness ceiling | `tmMemoryBase` 1024m → 2048m, `tmMemoryPerCore` 768m → 512m; this leaves the 4-core process size at 4096m **unchanged** and only gives the small cases back the fixed overhead they cannot amortise. Re-measured: **8.6%** |

One change per probe, effect asserted by re-measuring, never by the exit code.

**A note on the GC number.** Flink 1.20 publishes `Status.JVM.GarbageCollector.All.Time`
alongside `G1 Young Generation.Time` and `G1 Old Generation.Time`, and the
harness sums every metric ending in `.Time`. Young + Old + All = 2 × the real
GC time, so every GC figure in this report — the 11% ceiling included — is
twice the wall-clock GC. Read 8.6% as ~4.3% of one core actually spent in GC.
The harness is used verbatim, so the reported figure is left as the harness
computes it, and it is the figure the guard judges.

## Step 1c — the correctness gate, rehearsed

`prove.py completeness` was run on its own before the chain: a 10M-record
backlog drained twice at the baseline, once cleanly and once with the task
manager killed at 35%. Both arms passed every assertion with no tolerance. The
killed arm wrote **10,055,523** records to `positions-by-symbol` for
**10,000,000** trades — 55,523 replayed duplicates — and the final position of
every one of the 64 symbols and 8,192 (account, symbol) keys was still exactly
the manifest's. That is the at-least-once sink and the absolute-value payload
doing what §1 q4 said they would.

## Step 2 — measure

`harness/prove.py all --quick`, detached, waiting on `results/DONE`. The harness
is used verbatim; nothing in `lib.py` was edited.
