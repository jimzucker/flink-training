# Step 11 — AWS: where the ceiling actually is

Step 10 ended unable to prove that parallelism buys throughput. On one laptop the
pipeline was pinned by a single broker's write path at ~750,000 records/sec, and
since each order becomes five records it topped out near 142,000 orders/sec at
every parallelism tried. The claim needed hardware where Kafka and Flink are not
fighting over the same eight cores.

## What was built

| | |
|---|---|
| MSK | 3 × `kafka.m5.xlarge`, Kafka 3.9 KRaft, plaintext, RF=1 |
| Client | EC2 running the same `compose` stack: job manager, task manager, Prometheus, Grafana |
| Network | default VPC, brokers reachable only from the client's security group |

Replication factor 1 matches the laptop's single broker exactly. RF=3 would
triple the write volume and confound the very thing being measured — that is a
durability question, and a separate one.

## The first result inverted the problem

`c5.2xlarge`, 4 partitions, parallelism 4, draining 88,678,174 orders:

| | laptop | AWS `c5.2xlarge` |
|---|---|---|
| time | 623 s | 1068 s |
| orders/sec | **142,340** | **83,031** |
| records written/sec | 711,700 | 415,155 |
| bottleneck | broker at ~95% of capacity | **client CPU: 92% user** |
| broker | pinned | **0.66 cores, 99.7% request-handler idle** |

AWS came in at 58% of the laptop, and that is a sizing error rather than a
finding: `c5.2xlarge`'s eight vCPUs are four physical Cascade Lake cores with
hyperthreading, against eight real and faster cores on the laptop. Flink was
given roughly half the compute.

What it does establish cleanly is that **moving Kafka off the box removed the
broker as the constraint**. Locally the broker was at 95% of what it would
accept; here MSK never exceeded 0.66 cores across three brokers while the client
saturated. The ceiling moved from Kafka to Flink, which is exactly what step 10
predicted and could not demonstrate.

## The measurement, once the breakages were fixed

Same 88,678,174-order backlog, 16 partitions, checkpoint interval 10s, producer
stopped, on `c7i.8xlarge` — 32 vCPUs of Sapphire Rapids:

| | orders/sec | allocations/sec | records written/sec | steady-state |
|---|---|---|---|---|
| **parallelism 8** | **260,053** | 1,040,212 | **1,300,265** | ~405,000 |
| parallelism 16 | 208,654 | 834,616 | 1,043,270 | ~314,000 |

And against everything that came before:

| | orders/sec | vs laptop |
|---|---|---|
| laptop, 8 cores shared with broker, p=4 | 142,340 | 1.00× |
| AWS `c5.2xlarge` (4 physical cores), p=4 | 83,031 | 0.58× |
| AWS `c7i.8xlarge` (32 vCPU), p=16 | 208,654 | 1.47× |
| **AWS `c7i.8xlarge` (32 vCPU), p=8** | **260,053** | **1.83×** |

Two findings, and the second is the more useful one.

**Removing the broker as the constraint is worth a lot.** The pipeline went from
711,700 records/sec against a single local broker to **1,300,265 records/sec**
against three MSK brokers — and MSK was still not working hard. The step-10
conclusion was right: the ceiling was Kafka, and it moves when Kafka does.

**Parallelism 8 beats parallelism 16 on the same 32 vCPUs, by 25%.** More
subtasks did not find more capacity; they added coordination. Each sink subtask
opens its own transactional producer, so doubling parallelism doubles the
transactional producers the broker must track and the checkpoint must commit,
and past some point that costs more than the extra concurrency returns. The right
parallelism is not "as many as there are cores".

The average understates both runs. Each began with a flat period while the first
checkpoints established themselves — 84 seconds at parallelism 16 — so the
steady-state rates (~405,000 and ~314,000) are the honest picture of what the
pipeline sustains once running, and the averages are what it achieves from a
cold start.

## Three things that broke, and what they teach

**Exactly-once at a one-second checkpoint interval does not survive a remote
broker.** At parallelism 16 the pipeline stopped dead at 106,355 records with the
machine 99% idle, both jobs reporting RUNNING and no exception anywhere. The
checkpoint counters told the story: 101 of 112 failed. Flink opens a fresh
transactional producer per sink subtask per checkpoint, so at parallelism 16
across four sinks a one-second interval asks a remote transaction coordinator for
64 `InitProducerId` round trips every second. It cannot keep up, checkpoints
fail, transactions never commit, and the pipeline halts holding everything it has
written. One second was tuned against a broker on localhost at parallelism 2.
**The checkpoint interval has to grow with parallelism and with the distance to
the broker.**

**Task slots must scale with parallelism, and the failure is silent.** Two jobs
each want `parallelism` slots. Eight slots fitted two jobs at parallelism four
exactly; carrying that number to parallelism 16 gave one job all sixteen and left
the other unable to schedule. Both reported RUNNING. Nothing was logged.

**MSK exposes an allow-list, not `server.properties`.** `queued.max.requests` was
rejected outright as "not supported by at least one Apache Kafka version" — a
setting that was part of the local tuning. Worth knowing before planning a
migration around a specific knob.

## Observability across two very different brokers

The dashboard could not query the broker directly and work in both places: a
local Kafka runs this project's own JMX exporter config, which flattens names
deliberately to keep the series count at 15, while MSK ships its own and produces
`kafka_server_BrokerTopicMetrics_OneMinuteRate` with a `name` label. Same beans,
different names.

Both environments now record into one `ft:` vocabulary, and the panels ask for
that. Adding a third environment is one rules file and no dashboard changes.

One panel is honestly degraded rather than ported: MSK does not publish per-topic
log size, so on AWS the disk panel shows the brokers' filesystem usage — how much
disk, but not which topic is spending it. Locally that panel shows the account
topic at four times the symbol topic, which is the write amplification made
visible.

## Cost and teardown

Compute came to about **$3.75**: three MSK brokers and their storage for an hour
and three quarters, a `c5.2xlarge` for the first hour and a `c7i.8xlarge` for the
last forty minutes.

**Data transfer probably cost more than the compute did, and that was avoidable.**
The client sat in one availability zone while the three brokers spread across
three, so roughly two thirds of Kafka traffic crossed an AZ boundary at a cent
per gigabyte in each direction — and these runs moved hundreds of gigabytes on
purpose. At an estimated 200–400GB that is **$4–8**, putting the exercise nearer
**$8–12** in total.

For a throughput test the brokers belong in the client's zone. Spreading them was
reflexive habit from durable deployments, where it is right; here it bought
nothing, cost more than the servers, and added a little cross-AZ latency to every
number above. Worth fixing before this is repeated.

These figures are computed from resource lifetimes and published rates. AWS
billing data lags about a day, so Cost Explorer showed nothing for the run when
checked immediately afterwards.

Everything was destroyed and verified: no clusters, no instances, no addresses,
empty state.

MSK cannot be suspended. There is no pause: you either have a cluster and pay for
it, or delete it and wait 25–30 minutes to rebuild. Only the client instance can
be stopped cheaply, and it keeps its disk. That asymmetry is worth knowing before
planning to "leave it up over lunch".

## What still needs doing

**The doubled fill is diagnosed but the fix is untested.** An orphaned
`run-drain.sh` kept running after its local wrapper was killed -- SSH does not
propagate signals -- so two generators and later two whole drains ran
concurrently, writing to one output file and managing the same topics. The fill
came out at exactly twice what was asked for. Launching detached on the instance
with a real PID is the fix; it has not been exercised from a cold start.

**Parallelism between 8 and 16 is unexplored.** Eight beat sixteen by 25% on 32
vCPUs, but nothing was run at 4, 12 or 24, so where the curve actually turns is
unknown -- only that it turns below 16.

**The `1 -> 2` figure from step 10 is still from small cache-warm runs** and is
not comparable to anything measured here.

---

**Resolved in step 12.** This step made Flink the constraint by overwhelming
everything around it, which works but makes a poor demo. Capping Flink's cores
instead reproduces the whole result on a laptop, and the same script run against
a smaller MSK cluster gives the AWS comparison this step was reaching for --
without a 32-vCPU instance or three brokers. See
[`docs/steps/step-12/scaling-demo.md`](../step-12/scaling-demo.md).
