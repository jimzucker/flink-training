# Journal

## Step 0 — the interview that did not happen

No human was available. The skill says to interview one question at a time before
building; these are the questions I would have asked, the default I offered myself,
and the assumption I took in place of an answer. Every one of them is a stated
assumption, not an answer.

| # | question | assumption taken |
|---|---|---|
| 1 | What is the input event and what comes out? | **A block trade arrives on Kafka; the job allocates it across the four accounts of its allocation group and emits the running net position for the trade's symbol and for each account it touched.** |
| 2 | Does one input become several outputs? | **Yes, 5: one position-by-symbol record, plus one position-by-account record per allocation (4).** Fan-out lands on the write side, so the broker carries 5× the record rate it reads. |
| 3 | What are the keys and how many distinct ones? | **32,768 symbols and 16,384 accounts.** Large key counts on purpose: with 128 key groups over 4 subtasks, a few hundred keys hash unevenly enough (±10%) to hold the largest case below its CPU cap, and that is indistinguishable from a scaling shortfall. |
| 4 | What has to be exactly right, and which two settings? | **Positions are running sums, so a replayed record is a wrong number.** State: exactly-once checkpointing. Sink: at-least-once, made idempotent by emitting the *absolute* position per key, keyed so a key's records stay in one partition and in order (producer idempotence on). |
| 5 | Who watches, and what must they believe? | **Engineers**: correctness first (completeness verified with no tolerances, including a worker killed mid-drain), capacity second. |
| 6 | Where does it run? | **This laptop**, everything in Docker, one broker, one job manager, one task manager per case. |
| 7 | What claim do you want to make? | **"This pipeline's throughput scales with the cores given to one worker: 1→2 and 2→4 core step ratios."** The user asked for a *fast look*, not a publishable table, so the claim is explicitly not being proven here. |
| 8 | Which axis? | **One worker growing**: one task manager container capped at N cores, parallelism N, N slots. Not workers multiplying. |
| 9 | Which API level? | **Flink DataStream API, hand-written operators.** No SQL, no Table API — the user said so. |

Two further decisions taken without an answer:

- **Cases 1, 2 and 4 with the baseline at 1.** §5 of the skill says not to run the
  one-unit case when the claim is a step from two units up, and the record says the
  one-core case is the noisiest. The user asked for both step ratios by name, so the
  one-core case is in.
- **Backlog sized from someone else's number.** The rates in the harness's own
  `record/` (a different implementation of a similar pipeline) were used only to pick
  a first backlog size, never as evidence about this build. The tiny proof measures
  this build's rate and refuses a backlog that is short.

## Step 1 — build

`job/` is one Maven module producing one jar used three ways: the Flink job, the
deterministic generator, and the verifier. Flink is `provided` (it is in the image);
the Kafka connector and client are shaded in, because the host-side generator and
verifier run with `-cp` on that same jar.

Verified: two manifests from one seed at 200,000 records are byte-identical
(sha256 `4e5ce33fae3cbcb3`).

## Step 2 — calibration, and two refusals that cost an hour

The first tiny proof refused both cases on the broker's memory guard. What followed
is the only part of this run worth reading twice, because the first explanation was
wrong and the measurement said so.

| attempt | change | 1-core result | 4-core result |
|---|---|---|---|
| tiny proof 1 | kafka 3 GiB / heap 1 GiB, no compression | REFUSED, 2,964 limit hits | REFUSED, 10,648 hits |
| tiny proof 2 | + lz4 on input and both sinks; digits written without `String`; kafka 3.5 GiB / heap 1.5 GiB | REFUSED, 1,136 hits | OK, 615,011 rec/s, 99.4% of cap |
| tiny proof 3 | kafka 3.75 GiB / heap 1 GiB (more cache headroom) | REFUSED, 995 hits | OK, 618,936 rec/s |

The obvious story — "the broker is short of page cache, give it more" — predicted that
750 MiB more cache headroom would help. It moved the count from 1,136 to 995. The
story was wrong.

What the counters actually said: the broker cgroup sat pinned at its limit
(anon 1.23 GiB + file 2.34 GiB against 3.75 GiB), and the case that hit the limit was
the *small* one. The 4-core case, moving four times the bytes, hit it zero times.
The mechanism that fits every observation: at one core the rest of the VM is idle,
so the kernel lets the broker's page cache grow until it meets the broker's **own
cgroup limit**; at four cores the task manager's heap takes that memory first, global
pressure keeps the broker's cache below its cgroup limit, and the counter never fires.
The guard reads "the broker's cache was allowed to grow to its limit", which on a
small case is a sign of spare memory, not of starvation.

Prediction from that model: raise the limit above anything the VM can ever hand the
broker and the hits go to zero. Tested on one rig, one build, one 120M-record backlog,
one variable:

| broker cgroup limit | limit hits in the window | refaults | rate | worker |
|---|---:|---:|---:|---:|
| 3,840 MiB | 995 | 36,448 | — (refused) | 100.0% of cap |
| 6,144 MiB | **0** | 29,689 | 170,642 rec/s | 100.0% of cap |

Then the second measured problem: the one-core case was spending **45.8%** of its only
core in garbage collection. One variable again — the worker's fixed memory base, the
term that covers metaspace, JVM overhead and the network-buffer floor and does not
scale with cores:

| worker fixed base | 1-core rate | 1-core GC |
|---|---:|---:|
| 768 MiB (+448 MiB per subtask) | 170,642 | 45.8% |
| 1,216 MiB (+448 MiB per subtask) | **200,456** | **31.3%** |

Re-checked at four cores with the same change, because raising memory on a 7.8 GiB VM
can starve the largest case instead: 675,406 rec/s at 100.6% of cap, GC 9.3%, zero
broker limit hits. Per-subtask memory is still 448 MiB in every case, which is what
the harness's memory rule requires.

Both changes went into the chain together. Each was measured on its own before it did.

## Step 3 — the chain

`prove.py all --quick`, launched detached at 17:44:29.

Result: **PASS in 44.1 min**. up 2 s, preflight 45 s, completeness 440 s, tiny proof
431 s, fill 108 s, suite 1,623 s, report 0 s. Six of seven suite passes accepted; the
1-core sentinel pass was refused at 94.5% of its cap against a 95% floor, which is a
case-scope refusal, so the suite carried on and the 1-core case reported on its two
accepted passes.

1→2 = 1.959x (range 1.919–1.999), 2→4 = 1.865x (range 1.816–1.916), both stamped
`publishable: false` because `--quick` is two passes, not the configured three.

## Step 4 — teardown

`prove.py down`: two anonymous volumes removed, 6.6 GiB trimmed back to the host,
"nothing with prefix bt22 survives" asserted, and re-checked by hand afterwards —
no container, volume or network with the token, and no host process naming the
project directory or `prove.py`.
