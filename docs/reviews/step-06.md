# Step 06 — Observability: review

## Round 1

### Asked

1. Test the dashboard on the actual presenting machine before the demo.
2. Anonymous access is on, so there is no login screen mid-demo. Keep it?
3. Latency panel is a placeholder until the latency step — leave it visible?

### Feedback

> 2 & close gap

Read as: keep anonymous access, and close the gap that CI did not exercise
Grafana.

### Actions

| Feedback | Action |
|---|---|
| Keep anonymous access | No change. There is no login between the demo and the numbers |
| Close the CI gap | `scripts/verify-dashboard.sh` runs in CI: the dashboard is provisioned and well formed, every panel query returns data, the key counts read 4 and 16, and it renders to a non-trivial image |

### Proving the checks bite

A guard nobody has seen fail is worth very little, so all three defects were
reintroduced and the check was required to catch them.

| Defect | Result |
|---|---|
| `lastNonNull` instead of `lastNotNull` | `unknown stat reducers 10` |
| a row panel | `row panels 1` |
| an operator renamed out of existence | `panel queries returning nothing 1` |

The run exited 3. Restored, and the check passes again.

The third is the one worth having. Panel queries name Flink operators, and those
names come from `.name()` calls in the jobs, so a rename blanks a panel with
nothing failing and nothing logged. It is discovered by looking at the dashboard,
which during a demo is too late.

### Outcome

Approved. Squash-merged to `main`, tagged `step-06`.
