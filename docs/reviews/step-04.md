# Step 04 — Part 1, positions: review

## Round 1

### Asked

1. Is at-least-once acceptable through the demo, deferring exactly-once to AWS?
2. Parallelism is 2 with 4 task slots — size it for the scale case instead?
3. Sinks 5 and 6 sit empty at this step — confirming that is what you see?

### Feedback

> 1/no 2/the demo scales form 2 to 4 to show linear scaling so not clear on this
> question 3/yes

### Actions

| Feedback | Action |
|---|---|
| At-least-once not acceptable | Both sinks switched to `EXACTLY_ONCE`, one transactional id prefix each, transaction timeout set to outlast a checkpoint and stay under the broker maximum |
| Demo scales 2 → 4 | Parallelism made a runtime setting and passed to the submitting client, where the job graph is built. Verified against the running graph: all three vertices report parallelism 4 |
| Sinks 5 and 6 empty | Confirmed — nothing writes them until Part 2 |

Question 2 was badly framed on my part: it asked whether to *change* the default,
when the useful question was whether parallelism needs to be changeable at run
time. It does, so it is.

### What exactly-once cost, and what it caught

Two consequences, both recorded rather than hidden:

- **The sinks advance once per checkpoint**, not continuously, because a record
  is unreadable until its checkpoint commits. That is also a floor under
  end-to-end latency and will matter at the latency step.
- **Every reader must use `read_committed`**, or it sees records from
  transactions that may still abort — claiming more than is durable.

It also broke the record counter, which is worth recording because the failure
would have been silent: every committed transaction appends a marker that
advances the partition offset without being a record, so summing end offsets
over-counts. A wait loop keyed on that can finish *before* the data is there, and
the verification would then have measured a half-written topic. Counting now
consumes with `read_committed` and stops at the target.

### Proving it

A delivery guarantee is a claim about failure, so `scripts/chaos-exactly-once.sh`
kills a task manager mid-run. The job recovered from checkpoint and the sinks
held exactly 100 and 400 records, with each key's `updateCount` still running
1..n with no repeats. Under at-least-once everything written between the last
checkpoint and the kill would have been re-emitted — the counts would exceed 100
and 400 and update counts would repeat, which are precisely the two checks
already in place.

The first attempt failed for an unrelated reason worth noting: the task manager
did not come back, because `restart: unless-stopped` applies only to containers
created after it was added. The script now restarts it explicitly rather than
depending on a policy, so the test measures a recovery rather than a stopped
cluster.

### Outcome

_Pending._
