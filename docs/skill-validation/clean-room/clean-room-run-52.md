# Clean-room run 52 — the defaults, after the dashboard and verdict changes

Every interview default (DataStream, Apache Kafka), the same prompt as run 51
apart from the API. The first run after the skill gained the four-row
dashboard with plain titles (scalable-flink-skill #109), harness checks on the
dashboard (#110, #111) and the "not settled" verdict with extra settling passes
(#112). 1 h 55 min, 89 tool calls, 294k tokens. Files: [run-52/](run-52/),
including the job, [PositionsJob.java](run-52/PositionsJob.java).

## What it produced

[The suite](run-52/results/suite.txt), build `47546d835381037c`:

| cores | records/s | passes | how far apart |
|---:|---:|---:|---:|
| 1 | 128,202 | 4 | 6.3% |
| 2 | 251,986 | 6 | 7.5% |
| 4 | 468,178 | 6 | 8.0% |

1 → 2 **1.97×, met** (range 1.85–2.00×). 2 → 4 **1.86×, not settled**: its
range across 8 pairs, 1.77–1.90×, spans the 1.80× target, after the suite had
spent all six extra settling cases on it. Verdict: `STOPPED at report: not
settled`. Every pass ran at 96–100% of its CPU cap; nothing was thrown out.
Completeness passed on both arms, clean and with the worker killed.

The dashboard was built from section 7 on the first try and passed all three
of the harness's new checks; the report reads "opens on the suite,
22:12–23:18; every panel shows data over the suite".

## What went wrong on the way

- **The completeness check stopped the agent's first smoke run** on
  `outputsPerInput = 5.0, read back 4.987`. The check compares the declared
  fan-out exactly (`lib.py`, `str(want) == str(got)`), so the deliberate
  repeated records section 4 asks for fail it; section 4 promises a 5%
  tolerance. The agent got through by declaring 4.975.
- `job.args` in the `--key={value}` form did not parse with Flink's
  `ParameterTool`; one submit.
- Both cost about 10 minutes, caught only because the agent smoke-tested
  completeness before launching the chain.

## What the agent reported

13 findings in [SKILL-FEEDBACK.md](run-52/SKILL-FEEDBACK.md). On section 7:
good on plumbing (the exporter, provisioning, the checks and the suite's
range all worked first time), weak on content (no metric names; two "always"
panels need gauges the job publishes itself). Others: a dead "§1, question 8"
reference in section 7, the tiny proof's self-test overwriting the live
progress file with "finished", two different intervals both called "the
passes", no instruction for an unattended agent on "not settled", and the
dashboard check summing points across a panel's queries.
