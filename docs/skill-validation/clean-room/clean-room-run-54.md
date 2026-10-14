# Clean-room run 54 — the defaults, on the skill after #154 and #155

Every interview default (DataStream, Apache Kafka), run 53's prompt with only
the folder, the free disk and "macOS was upgraded yesterday" changed. The first
clean-room run on scalable-flink-skill `df12dcf`: whole-name README check,
self-test messages in their own file, one rule in §6 for a step not met
(#154), the reporter's spelling, quoted compose values, `{arm}` in the
examples, completeness never stopped by a busy Mac and the probe's file kept
(#155). 1.54 h from launch to `results/DONE` (21:53 to 23:25), 78 tool calls,
254k tokens. Files: [run-54/](run-54/), including the job,
[PositionsJob.java](run-54/job/src/st54/PositionsJob.java).

The agent handed back at 22:13, 71 minutes before the chain finished, and
sent three more reports as steps passed, the last at 22:41 while the suite's
backlog was filling. It never saw the result: there was no scorecard to
report, and the probe and ceiling were not run. It had started the watcher in
the background and ended its turn to wait (scalable-flink-skill #158).

## What it produced

[The suite](run-54/results/suite.txt), build `d7e14e7505068416`, 250,000,000
orders:

| cores | records/s | passes marked OK | how far apart | counted |
|---:|---:|---:|---:|---|
| 1 | 108,433 | 4 | 16.0% | yes |
| 2 | 217,213 | 3 | 4.2% | yes |
| 4 | 358,160 | 3 | 20.3% | no — over the 20% limit |

1 → 2 **2.00×, met**: the low end of its range, 1.919×, is 6.6% over 1.80×.
2 → 4 **has no number**: the 4-core case was thrown out because its three
readings were 20.3% apart. Verdict, as the harness printed it then:
`STOPPED at report: a step the claim needs was not measured — 2→4: the 4-core
case does not count: its readings are 20% apart and the limit is 20%` (71.7
min). Every pass ran at 97–100% of its CPU cap. Completeness passed on both
arms, clean and with the worker killed.

The 4-core readings were 333,487, 406,141 and 334,852/s. The two that agree
give 2→4 about 1.54× against the 2-core mean; that is not a reported step.

**Not explained:** the second pass read high at every core count — 4 cores
406,141/s against 333,487 and 334,852; 1 core 119,562/s against 102,230–108,628;
2 cores 222,730/s against 213,594 and 215,316. Other programs used 0.08–0.12
of the 8 cores at the start and end of the two high 4- and 1-core passes, but
the 2-core third pass and the closing 1-core pass read normally with the same
0.08–0.12, so those figures do not separate the passes. The high passes are
the three descending ones, back to back from 22:58 to 23:08, and in them the
source was held back by the later stages for less of its time on the same
cores (4 cores 16% against 25–30%). Cause not measured.

Other readings, not tested as causes:

- the tiny proof (one short pass per case) read 1→2 1.898× and 2→4 1.634×;
- Kafka hit its memory limit 6,821–7,931 times per 4-core pass, against
  3,611–4,019 at 2 cores;
- the Mac had 5.5–7.0 GB swapped out at every window;
- chain 2's preflight measured the Mac's own cores on memory-heavy work at
  82.6% of linear for 2→4 (76.8–87.4% over 3 repeats), about 1.65×; run 53's
  probe read 1.65× for the same. Chain 1's preflight had read 80%.

## What went wrong on the way

- **Chain 1 STOPPED at completeness after 6.5 minutes**, correctly: the
  agent's verifier crashed on a Jackson version clash (it asked for
  jackson-databind 2.17.2; the Kafka connector brings jackson-core 2.15.2). The
  agent pinned 2.15.2, proved the verifier by hand against the outputs still on
  the broker, and started chain 2, which passed completeness in 558 s.
- **Preflight's CPU row said PASS beside "busy: 2.20 of 8 cores"** in chain 1.
  Renamed in #156.
- **The agent handed back before the chain finished**, so the run's report is
  its interim one and the probe and ceiling were not run.

## What the agent reported

Five findings in [SKILL-FEEDBACK.md](run-54/SKILL-FEEDBACK.md), all fixed in
scalable-flink-skill #156: question 2's default means exactly 4 allocations
(F1); a table of every fill-in name for commands (F2); the memory sum written
down, and the README's out-of-date "spare gigabyte" rule corrected (F3); what
to do in §2 with nobody to review (F4); and `watch.sh` writing `watch.log`
in the run's folder (F5).

Reading the run's output found four more, also fixed in #156: "claim" in the
messages (now "target"), the spread rounded to look equal to its limit,
`harness.log` calling the table valid, and `headline.md` printing two
different ranges for 1→2. Re-rendered with #156, the stop line reads:
`STOPPED at report: a step the target needs was not measured — 2→4: the
4-core case does not count: its readings are 20.3% apart, over the 20% limit`.
