# Clean-room run 51 — the defaults, in Flink SQL, with the SQL guidance

The same as run 50 — the defaults, question 5 answered **SQL** — after the skill
gained an "if the answer is SQL" section (#94), a garbage-collection rule judged
by what it cost (#95), and no PASS while a claimed step is missing (#92).
3 h 56 min, 131 tool calls. Files: [run-51/](run-51/), including the job,
[SqlPositionsJob.java](run-51/SqlPositionsJob.java).

## What it produced

[Suite A](run-51/suite-A/suite.txt), build `fa1f233378a9cfaa`:

| cores | records/s | |
|---:|---:|---|
| 1 | 20,266 | every pass ruled out: GC 6.7–8.2% **and** 11.5% less work per core than 2 cores |
| 2 | 45,778 | passes 1.2% apart |
| 4 | 97,108 | passes 4.4% apart |

2 → 4 **2.121×** (low end 2.044×); 1 → 2 **not measured**. Verdict: a step the
claim needs has no number — the first run to get that verdict rather than a PASS.
About a sixth of the DataStream builds' throughput per core. Completeness passed
on both arms in both chains.

## What went wrong on the way

- **Two chains stopped at the tiny proof on single-pass noise.** The same
  unchanged build read 2 → 4 at 1.49×, 1.91× and 2.14× in three tiny proofs;
  78 minutes went to re-running checks that had already passed.
- **A harness bug (#95), fixed in #97:** the GC ruling was made after the first
  1-core pass, before any larger case existed, and never revisited, so the pass
  was ruled out "with no larger case under the limit to compare it with".
  Re-judged with the fix, the case is still out, for the right reason.
- More worker memory (+30% at 1 core) did not move its GC; reverted.

## What the agent reported

21 findings in [SKILL-FEEDBACK.md](run-51/SKILL-FEEDBACK.md). It rated the SQL
guidance as working: the job was correct on its first run. The rest are the parts
of the skill still written for DataStream (vertex matching, design names, the key
check's FAIL), the tiny proof's noise, and repeats of runs 48–50.
