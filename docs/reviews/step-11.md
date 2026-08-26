# Step 11 — AWS: review

## Round 1

### Asked

Whether the scaling claim step 10 could not prove could be proved on AWS, and
whether MSK removes the broker ceiling that pinned the laptop.

### Feedback

> lets do a test of p=4 on aws and compare that to the laptop same scenerio we
> just ran

> lets run terraform appy w/o prompting get to the results, tear down when
> stopping so we dont run up the bill

> if P-4 results are favorable, we want to run P=2 to see if it cuts perf by
> half, but only if P-4 is fully tunned meeting expectations 2x laptop

> what can we do to take a bigger step to improve flink? (we can always back
> down)

> after this finishes if favorable scallin try p=8 / recorder verythig, update
> memory / tear downs

### Outcome

The step-10 conclusion held: **the ceiling was Kafka, and it moves when Kafka
does.** Against three MSK brokers the pipeline reached **1,300,265 records/sec**,
against 711,700 on a single local broker — and MSK was still barely working.

| | orders/sec | vs laptop |
|---|---|---|
| laptop, 8 cores shared with broker, p=4 | 142,340 | 1.00× |
| AWS `c5.2xlarge`, p=4 | 83,031 | 0.58× |
| AWS `c7i.8xlarge`, p=16 | 208,654 | 1.47× |
| **AWS `c7i.8xlarge`, p=8** | **260,053** | **1.83×** |

The gate was 2× the laptop, or 284,680. On averages nothing cleared it; on
steady-state rates p=8 reached ~405,000 and p=16 ~314,000, both past it. The
averages include a cold start of up to 84 seconds while the first checkpoints
establish themselves, which is real and worth reporting rather than trimming.

**Parallelism 8 beat parallelism 16 by 25% on the same 32 vCPUs.** More subtasks
did not find more capacity. Each sink subtask opens its own transactional
producer, so doubling parallelism doubles what the broker must track and the
checkpoint must commit. The right parallelism is not "as many as there are
cores", and p=2 was never run because the interesting boundary turned out to be
above it, not below.

### What went wrong, and it was mostly me

**The first sizing was wrong.** `c5.2xlarge` was chosen to match the laptop's
eight cores; those eight vCPUs are four physical cores with hyperthreading
against eight real and faster ones, so Flink got about half the compute and came
in at 58% of the laptop. Matching vCPU count is not matching CPU.

**`pkill` on a local wrapper does not kill a remote process.** SSH does not
propagate signals, so a drain script kept running on the instance after I
believed it dead. Two generators produced exactly twice the requested backlog;
later two whole drains interleaved into one output file, and my "fix" deleted the
topics under a run that was working at 519,573 orders/sec.

**Terraform replaced the instance rather than resizing it**, despite the plan
saying `will be updated in-place`, taking the disk and the built jars with it. I
reported the plan's promise as fact instead of re-planning after an earlier
apply had errored.

**Three claimed edits did not land.** A `docker compose up` line kept starting
without Grafana across two separate "fixed" reports, because the replacement
string did not match the file's indentation and I did not check.

### What the failures taught, which is the useful part

**Exactly-once at a one-second checkpoint interval does not survive a remote
broker.** At parallelism 16 the pipeline stopped dead at 106,355 records, both
jobs RUNNING, no exception anywhere, machine 99% idle. 101 of 112 checkpoints had
failed. Flink opens a fresh transactional producer per sink subtask per
checkpoint: at parallelism 16 over four sinks that is 64 `InitProducerId` round
trips per second to a remote coordinator. **The checkpoint interval must grow
with parallelism and with the distance to the broker.**

**Task slots must scale with parallelism, and the failure is silent.** Two jobs
each want `parallelism` slots; sixteen slots gave one job everything and left the
other unschedulable, with both reporting RUNNING and nothing logged.

**MSK exposes an allow-list, not `server.properties`.** `queued.max.requests` was
rejected outright — worth knowing before planning a migration around a tuning.

**One dashboard now serves both brokers.** Local Kafka and MSK publish the same
JMX beans under different names, so each environment records into a shared `ft:`
vocabulary and the panels query that. Per-topic disk is the one honest
casualty: MSK does not publish it.
