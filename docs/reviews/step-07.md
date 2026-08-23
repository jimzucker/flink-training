# Step 07 — Correctness demo: review

## Round 1

### Asked

1. Does the runbook match how you actually present?
2. Should the demo run at 10 trades/sec, or slower so a specific trade is easier
   to follow live?
3. Is a ten-second window acceptable for the live demo? At sixty you wait a full
   minute before sinks 5 and 6 show anything.

### Feedback

> 3 - yes 10s ok

### Actions

| Feedback | Action |
|---|---|
| Ten-second window is fine for the demo | `scripts/demo.sh` submits both jobs with a ten-second window and prints what to open. The job still defaults to the specified minute; the demo overrides it |
| (1 unanswered) | Runbook kept as written, to the requirements' rules rather than to a personal style |
| (2 unanswered) | Kept at 10 trades/sec, which is the stated input |

The window is the one deviation from the specification that will be visible on
screen, so the runbook says to volunteer it when reaching sinks 5 and 6 rather
than let it be noticed. It is a presentation choice and not a difference in the
calculation — the verification runs against both settings.

Confirmed by running it: market value appears within the first ten seconds
instead of after a minute of dead air.

### Outcome

Approved. Squash-merged to `main`, tagged `step-07`.
