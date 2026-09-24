# Fixes — clean-room run 48

Every change made to chase a number, with its prediction written **before** it was measured, and whether it was kept or reverted (SKILL.md section 6, "When there is no human to say yes").

None yet: the first chain was launched at 06:00:50 on build eed5ecdeb058cc6e with the configuration in PLAN.md.

## Build fixes (not tuning — nothing here was chasing a number)

### B1. 06:33 — the generator hung for 29 minutes filling the completeness backlog; chain 1 stopped at completeness

What was seen, from the generator's own stderr once it was stopped: `Retried
waiting for GCLocker too often` from the producer network threads and four
generator threads 7.5 s in, then `BufferExhaustedException: Failed to
allocate 524288 bytes within the configured max blocking time 60000 ms` in
four generator threads, which died. The main thread then waited forever in
`KafkaProducer.flush()` with no producer network thread left. 12,952,490 of
20,000,000 records had been written. Why the heap ran short on this fill and
not on the identical manual fill 50 minutes earlier: **not known**; the host
had 1.9 GB of swap in use and 85 MB of free pages at the time, which is a
hypothesis, not a measured cause.

Changed (build eed5ecde → new hash): one shared producer callback instead of a
lambda per record; producer buffers 128 MB → 48 MB and batches 512 KB →
256 KB; per-symbol key bytes precomputed; any generator thread that dies, any
failed send, or 120 s with no acknowledged record now **halts the process with
a non-zero exit** instead of hanging; the price feed is sent then flushed
instead of waiting on each record (it took 6.7 minutes the first time).
Manifests are byte-identical to before (sha256 d7e5fb0a0383… at 200,000
records, seed 20260924).

Checked before relaunching: 40,000,000 records to a scratch topic in 28 s
(1.43 M records/s), no GCLocker warning, 72 bytes per record on the broker.
The scratch topic was deleted and the deletion read back.

## Tuning (SKILL.md section 6a), after chain 2 missed the claim

Chain 2's suite (build 12d4d5de99ecf590, 3 passes + sentinel): **1→2 = 1.814×**
(range across passes 1.731–1.925×), **2→4 = 1.944×** (range 1.887–2.006×; lower
bound judged 1.88×). Every kept case at 98–100% of its CPU cap, GC 1.0–3.3%,
Kafka using 5–20% of its cores, source idle 0–2.6%.

Measured before touching anything, as section 6 requires:

| measurement | result |
|---|---|
| `prove.py probe --repeats 9` (no pipeline) | simple arithmetic 1→2 **1.93×** (middle half 1.92–1.95), 2→4 1.97×; memory-heavy 1→2 **1.73×** (1.66–1.90), 2→4 1.55× (1.40–1.64) |
| `prove.py ceiling` (4 cores, broker capped 2.5 → 1.0 → 0.5 cores) | 457,050 / 451,840 / 453,661 orders/s, pipeline at 99.0 / 99.2 / 99.7% of cap, broker at 19% / 47% / 92% of its cap. Starving the broker to half a core did not move the rate: **the broker is not the ceiling; the pipeline's own cores are** |

What the probe can and cannot say: the pipeline's 1→2 (1.81×) sits between the
host's two arms (1.93× and 1.73×), and the memory arm's middle half (10%) is
wider than the shortfall (5%). So the machine can be neither ruled in nor out
as the cause of the short 1→2. **The cause of the 1→2 shortfall is not known.**
(Side note, not explained: the three ceiling cases read 452–457k/s, about 4%
below the suite's 4-core passes 20–40 minutes earlier, 471–478k/s, on the same
build and configuration.)

Which rows of §6a match a symptom here:

| row | applies? |
|---|---|
| read the input once | already built that way (parse once, side output) |
| memory per subtask | already per subtask; GC is 1.0–3.3%, highest on the 1-core case — making the baseline faster would *shrink* 1→2 |
| broker page cache | 1-core and 2-core passes hit the broker's limit, but hits and no hits gave the same 2-core rates (249k with 2,762 hits; 246k and 238k with none); the tiny proof says 8g does not fit this VM; the ceiling run says the broker is not the constraint |
| compress the sink writes | already lz4 |
| fewer subtasks for the same cores | no hook in the harness |
| make the pipeline cost something per record | every case is already CPU-bound |
| **spread the keys evenly** | **the only row with a matching symptom**: at maxParallelism 128 the symbol stage lands 2073/2023 at 2 cores and 1065/1008/979/1044 at 4 |

### T1. `pipeline.max-parallelism: 440` (row: spread the keys evenly)

Preflight's key row passed and suggested nothing ("no maxParallelism divides
these keys" exactly). I scanned 128–32,768 myself with Flink 1.20.1's own
`KeyGroupRangeAssignment` (scratch program; it reproduces preflight's layout at
128 exactly). 440 is the smallest value that brings every stage above 0.99 of
linear: symbol 2048/2048 and 1017/1031/1021/1027, account 8206/8178 and
4099/4107/4081/4097 — worst stage ceiling 0.993 against 0.962 at 128. It is
fitted to these key names, which is also what the harness's own suggestion
does.

**Prediction, written before measuring:** 1-core rate unchanged (one subtask
has every key either way). 2-core rate up by 0 to 1.2%; 4-core rate up by 0 to
3.4% — the upper ends only if the symbol stage alone sets the pace, which it
probably does not (every subtask thread shares the same cores). So **1→2 moves
from 1.814× to at most 1.836× and does not reach 1.90×**; 2→4 moves from
1.944× to at most 2.00×, and its lower bound (1.88×) may clear 1.90× or may
not. If either step gets worse by more than the noise, revert.

Measured with a full chain (its tiny proof is the lever's measurement, its
suite the confirmation), because `flinkProperties` only reaches the job
manager when the stack is brought up.

**Measured (chain 3, 08:06–09:16, same build 12d4d5de99ecf590, maxParallelism read back as 440 by the design diff):**

| | chain 2 (128) | chain 3 (440) | predicted |
|---|---:|---:|---|
| tiny proof 1 core | 128,415 | 124,680 (thrown out: Kafka memory rule at 98.7% of cap) | unchanged |
| tiny proof 2 cores | 242,477 | 230,713 | +0 to +1.2% |
| tiny proof 4 cores | 482,200 | 457,095 | +0 to +3.4% |
| tiny proof 2→4 | 1.989× | 1.981× | |
| suite 1 core (mean) | 134,762 | 130,030 | unchanged |
| suite 2 cores (mean) | 244,437 | 240,684 | +0 to +1.2% |
| suite 4 cores (mean) | 475,278 | 466,878 | +0 to +3.4% |
| **suite 1→2** | **1.814×** (1.731–1.925) | **1.851×** (1.799–1.899) | ≤ 1.836× |
| **suite 2→4** | **1.944×** (1.887–2.006), judged at 1.88× | **1.940×** (1.840–2.049), judged at 1.87× | ≤ 2.00× |

**The prediction was wrong in direction for every case**: all three per-case
rates came back 1.5–3.5% *lower*, including the 1-core case the lever cannot
touch. So the two chains did not run on the same machine conditions: chain 3's
host load average reached 15.6–16.2 on 8 cores during the 4-core passes (above
the 7.7 cores this run's own containers are capped at), and two of its passes
were thrown out for reading under 95% of their cap. The chains were not
interleaved, so the lever's own effect **cannot be separated from that drift**.
What can be said: 2→4 did not move (1.944× → 1.940×, inside the ±4% a two-pass
ratio carries); 1→2 rose 0.037×, but because the 1-core case read lower, not
because the 2-core case read higher — which is not the improvement the lever
was for.

**Kept, not reverted**: neither step got worse by more than the noise, and
the layout is arithmetically more even (0.993 against 0.962 of linear at the
worst stage). Stated plainly: T1 had **no measurable effect**.

### Stopping here

§6a has no other untried row whose symptom matches (table above), so the
tuning loop ends after one change of the four allowed. Neither step meets the
claim: 1→2 is short, and 2→4's lower bound is short. The cause of the 1→2
shortfall is **not known**. The host's own cores give 1.93× on pure arithmetic
and 1.73× on memory-heavy work at that step, so the machine can be neither
ruled in nor out.
