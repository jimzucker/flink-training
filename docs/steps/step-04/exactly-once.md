# Step 04 — exactly-once, and the proof

## Why not at-least-once

A position is a running sum, so a record replayed after a failure is not a
harmless duplicate: it is a wrong number on a dashboard that nobody can explain.
A missing number is obvious; a wrong one is not.

## What changed

| | |
|---|---|
| Delivery | `EXACTLY_ONCE`, one transactional id prefix per sink |
| Transaction timeout | 5 minutes — must outlast a checkpoint, must stay under the broker's 15-minute maximum |
| Checkpoint interval | 5 seconds |
| Readers | must use `read_committed` |

Two consequences, both real and both visible rather than hidden:

- **The sinks advance once per checkpoint.** A record is not readable until the
  checkpoint that produced it commits, so sinks 3 and 4 move in five-second steps
  rather than continuously. This is also a floor under end-to-end latency, which
  matters for the latency step.
- **Reading uncommitted shows records that may still abort.** Every consumer in
  the verification path now reads `read_committed`, or it would be claiming more
  than is durable.

It also invalidated the offset-based record counter. Every committed transaction
appends a marker that advances the partition offset without being a record, so
end offsets over-count — and a wait loop keyed on them can finish before the data
is actually there. Counting now consumes with `read_committed`, stopping as soon
as the target is reached.

## The proof

Exactly-once is a claim about what happens when something fails. Without a
failure the setting is untested and at-least-once would look identical, so
`scripts/chaos-exactly-once.sh` kills a task manager mid-run.

```
== rerunning, killing the task manager mid-flight ==
   killing ft-taskmanager
   restarting the task manager
   generator finished; waiting for the job to recover
   job recovered from checkpoint

== results after the failure ==
   sink 3: 100  (expect exactly 100)
   sink 4: 400  (expect exactly 400)

  updateCount runs 1..n per key            0            OK
  reconciliation: symbol totals match account totals    OK
all checks passed, with no tolerances
```

Under at-least-once, every record written between the last checkpoint and the
kill would have been re-emitted on recovery: the counts would exceed 100 and 400,
and a key's `updateCount` sequence would repeat values. Both checks are exactly
the ones that would have caught it.

## Parallelism

The demo raises parallelism from 2 to 4 to show throughput scaling with it, so it
is a runtime setting rather than a build-time one. The job graph is built in the
submitting client, so it is passed there:

```bash
PARALLELISM=4 docker compose -f docker/compose.yml --profile submit run --rm submit
```

Confirmed against the running graph:

```
parallelism=4  Source: orders -> (by symbol, split by allocation)
parallelism=4  aggregate by symbol -> positions-by-symbol: Writer -> Committer
parallelism=4  aggregate by account -> positions-by-account: Writer -> Committer
```
