# Step 08 — Local Docker demo: review

## Round 1

### Asked

1. Should the generators auto-start, or wait to be started?
2. First build takes two to three minutes. Keep a host-built path as the
   documented default?

### Feedback

> For 1 yes I should trigger's maybe options auto, manual, an delay start in min
> when I launch/ add to ci

### Actions

| Feedback | Action |
|---|---|
| The presenter should trigger it, with options | `GENERATOR_START` takes `auto`, `manual`, or a number of minutes |
| Add the cold start to CI | A `Cold start` job brings everything up from nothing and checks it came up green; added to the required checks so it gates merges |

### The three modes

| Value | Behaviour |
|---|---|
| `auto` (default) | starts with the stack |
| `manual` | container stays up and idle; `scripts/start-generators.sh` starts it on cue |
| a number | starts that many minutes after the stack comes up |

Manual keeps the most persuasive moment in the demo — graphs going from flat to
flowing while people watch — at the cost of one command. The delay gives both:
bring the stack up, talk through the design, and have data arrive on cue without
touching a terminal.

Each was tested rather than assumed:

```
manual   idle with 0 records, then 144 after the trigger
1 min    0 records at 30s, producing once the minute elapsed
soon     rejected at start, exit code 2
```

A bad value fails loudly instead of being guessed at, because a generator that
silently chose a default would be discovered mid-demo.

### CI

The cold-start job is separate from the numbers job on purpose: the numbers job
starts services individually and builds on the host, which is why it did not
catch the generator exiting on start. Running the deliverable the way it is
actually delivered is the only thing that would have.

Question 2 went unanswered. The host-built path is documented as the way to work
on the code; `docker compose up` is documented as the way to run it.

### Outcome

Approved. Squash-merged to `main`, tagged `step-08`.
