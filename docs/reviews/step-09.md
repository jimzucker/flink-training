# Step 09 — Latency: review

## Round 1

### Asked

1. Which checkpoint interval should the demo use? 5s is the default; 1s makes
   latency five times better but checkpoints five times as often.
2. Is a median of 2.5s acceptable for "latency for orders is low"?
3. Market value latency is not reported, since its delay is the window rather
   than a cost. Is that the right call?

### Feedback

> 3 we should show it in will like steps that's expected/ 1 - change to 1 sec/
> do we chart checkpoints?

### Actions

| Feedback | Action |
|---|---|
| Show market value latency; steps are expected | Measured from the window close and reported. It does step: p95, p99 and the maximum are the same number, because every key in a window is emitted at the same boundary and shares an age |
| Change the checkpoint interval to one second | Now the default, in the job and in the local stack. Order latency drops from p50 2488ms to **518ms**, with the maximum inside one interval as before |
| Chart checkpoints | Duration and completed count are charted next to the latency they explain, with the aborted count alongside |

### What charting checkpoints turned up

Two things, immediately.

A checkpoint takes about **13 milliseconds** against a one-second interval, which
settles what the cost actually is: the interval between checkpoints, not the
checkpoints themselves.

And the dashboard showed one **aborted** checkpoint per job. With a one-second
interval the first checkpoint triggers before every task is running and is
aborted; the count then stays put while completed checkpoints climb into the
hundreds. Harmless — but the panel had been written to say that any non-zero
count meant the guarantee was being retried, which would have raised a false
alarm in the middle of a demo. The panel now says what the number means and the
runbook has an answer ready.

Question 2 went unanswered. At a one-second interval the median is about half a
second, which is a more comfortable answer to "is latency low" than 2.5s was.

### Outcome

Approved. Squash-merged to `main`, tagged `step-09`.
