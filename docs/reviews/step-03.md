# Step 03 — CI: review

## Round 1

### Asked

1. Should CI gate merges? Nothing enforces it today.
2. Are ~2.5 minute runs acceptable, or should the numbers job be PR-only?
3. Do you want a nightly scheduled run?

### Feedback

> 1 yess / 3 no

### Actions

| Feedback | Action |
|---|---|
| Yes, gate merges | Branch protection on `main` requiring all three checks |
| No nightly run | No schedule trigger added |
| (2 unanswered) | Kept as-is: the numbers job runs on every push, not PR-only |

### Consequence: step merges now go through a pull request

Gating only means something if a failing check can actually stop a merge. That
requires the commit landing on `main` to have been checked, and a squash commit
created locally and pushed straight to `main` has never been seen by CI — it is
a new SHA with no check runs against it.

So the merge step changes shape, while the result stays identical:

| Before | After |
|---|---|
| `git merge --squash` locally, then `git push origin main` | open a PR from the step branch, let CI run, then squash-merge it |

GitHub's squash-merge produces exactly the same thing the workflow has produced
all along: one commit per step on a linear `main`, with the step branch kept for
inspection. What changes is that the commit is now *provably* green before it
lands rather than tested immediately afterwards.

### Outcome

Approved. Squash-merged to `main`, tagged `step-03`, and branch protection
enabled afterwards so it governs step 04 onward.
