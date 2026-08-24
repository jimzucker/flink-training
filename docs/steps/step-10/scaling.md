# Step 10 — what could and could not be measured here

## The two cases the requirements specify: both pass

| | parallelism | orders/s asked | prices/s | orders through | allocations | order latency p50 | p99 | checkpoint |
|---|---|---|---|---|---|---|---|---|
| baseline | 2 | 10 | 1000 | 8/s | 33/s | 519 ms | 1023 ms | 8 ms |
| **case 1** | 2 | 1000 | 1000 | **816/s** | **3269/s** | **522 ms** | 1011 ms | 14 ms |
| case 1 | 4 | 1000 | 1000 | 812/s | 3250/s | 527 ms | 1015 ms | 17 ms |
| **case 2** | 2 | 10 | **20000** | 8/s | 33/s | **513 ms** | 966 ms | 6 ms |

**Case 1 — orders to 1000/sec.** Throughput rose by a hundredfold, from 8 to 816
orders a second and 33 to 3269 allocations. Order latency did not move: 519ms at
the median before, 522ms after. The requirement allows latency to rise; it did
not need to.

**Case 2 — a very high price rate.** Prices went from 1000 to 20000 a second, and
order latency went from 519ms to 513ms — unchanged within the noise.

That second result settles a question left open in step 05. Prices are broadcast
to every subtask, because the account side is keyed on account/sub-account/symbol
and cannot be joined to a symbol-keyed stream by key alone. The concern was that
broadcasting would put the price rate through the same threads doing order work,
which is exactly what case 2 is designed to catch. At twenty times the price rate
it does not. The decision to measure rather than pre-reduce the price stream was
the right one, and pre-reducing would have traded away the exact price-at-close
for a problem that is not there.

## What could not be measured here: parallelism scaling

Raising parallelism from 2 to 4 changed nothing — 816 orders/sec against 812.
That is not evidence that the pipeline does not scale. It is evidence that it was
never the constraint.

The generator is. Asked for 1000 orders a second it delivers about 816, and
asked for 5000 it delivers about 3000: its pacer sleeps between records, and
sleep granularity puts a ceiling well below what the pipeline can absorb. With no
queue to work through, more parallelism has nothing to do.

Measuring the pipeline directly means removing the producer from the
measurement — filling the topic first and then starting the job, so what is timed
is Flink draining a known backlog. `scripts/scale-catchup.sh` does that, and on
this machine it does not produce a number worth reporting:

- identical runs at parallelism 1 returned **5212** and then **2223** orders/sec
- one parallelism-2 run reported zero, because the submission had not produced a
  running job at all — twenty-four cancelled jobs had accumulated from repeated
  cancel-and-resubmit cycles

Three attempts produced three different failure modes, all of them environmental:
a laptop thirty-four days into its uptime, running Docker, a browser, Kafka,
Flink, Prometheus and Grafana, is not an instrument. The honest conclusion is
that this machine cannot measure scaling, not that scaling does not happen.

## Where to measure it instead

The AWS step. Dedicated resources, a cluster not competing with everything else
on a development machine, and the ability to change task manager count rather
than only slots. The script is written and works; it needs somewhere quiet to
run.

Until then the claim is unproven, and it is better recorded as unproven than
supported by a number that changed by a factor of two between identical runs.
