# Clean-room run 56 — the defaults, on the skill after #159–#165

Every interview default (DataStream, Apache Kafka), run 55's prompt with three
changes: the directory, the free disk (105 GB), and feedback asked for on the
dashboard the harness now builds, the machine sleeping or on battery, and the
disk check. The skill was `60a789c` at launch (#159–#165: the harness-built
dashboard, the sleep check, run 55's findings, the disk row). Launched 13:06,
the agent reported about 16:20: 3.26 h, 95 tool calls, 266,502 tokens. Files:
[run-56/](run-56/), including the job,
[PositionsJob.java](run-56/job/src/main/java/st56/PositionsJob.java).

## What it produced

**No scaling result.** Two suites on the final build (`e35f29848e13`), both
stopped at the report because readings of one case were more than 20% apart.

| suite | 1 core | 2 cores | 4 cores | stopped because |
|---|---:|---:|---:|---|
| [chain 3](run-56/results-chain-3/suite.txt), 14:39–15:29 | 120,080 | 214,065 (24.4% apart) | 375,851 (28.8% apart) | 2- and 4-core cases thrown out |
| [re-run](run-56/results/suite.txt), 15:30–16:13 | 116,161 (21.0% apart) | 200,254 | 341,760 (28.9% apart) | 1- and 4-core cases thrown out |

Records per second, the mean of each case's passes. Completeness passed both
arms, clean and with the worker killed, on all three chains. Every case used
95–100% of its cores.

**The slow passes coincide with the Mac's memory moved out to disk.** In the
re-run, passes opened with 4.5–5.5 GB moved out read 301,274 and 323,899 at 4
cores and 195,952 and 184,726 at 2; passes opened with 1.6–2.2 GB read 400,108
and 220,085. The agent names Chrome, Numbers, ChatGPT and Mail as holding the
memory. This is a match inside one run, not a controlled comparison: **cause
not measured.** The wait before each case checks other programs' CPU only.

## What went wrong on the way

- **Chain 1 STOPPED at the tiny proof, 25.5 min in**: the prices source ran at
  parallelism 1, and every operator must run at the case's core count. The
  agent's fault; the harness was right to stop it, but found it 25 minutes in,
  and the rule sits in one table cell (finding F1).
- **Chain 2 STOPPED at the tiny proof on a harness fault**: the self-test
  "input already on the broker is not asked for twice" assumed a backlog of at
  least 250,000,000 records, and this run had 180,000,000. The agent raised its
  backlog to 250,000,000 to get past it. Fixed in scalable-flink-skill #166,
  which reached the installed skill at 14:38, after chain 3 had started.
- **The suite order changed after this run**: on the author's instruction the
  suite now runs the largest case first (#167). Run 56 ran 1, 2, 4 first.

## What worked first time

From the agent's report: the job diagram (4 tasks, 13 operators, key counts,
both broadcasts); the dashboard (rows in order, a lag panel on every topic,
the broker network as a share of its link, every panel with data); the
README check; evidence in git; naming of operators and sinks; the power row
(mains, 95%) and no sleep; the disk row (a guess at preflight, 66 bytes a
record measured by the tiny proof); and waiting on the chain with `watch.sh`.

## What the agent reported

Ten findings in [SKILL-FEEDBACK.md](run-56/SKILL-FEEDBACK.md):

| # | finding | state |
|---|---|---|
| F1 | every operator at the case's core count is one table cell; found 25 min in, and the tiny proof then still ran the smaller cases | open |
| F2 | the fill-stall check watches only the scaled input, so a generator that writes its second input first looks stalled | open |
| F3 | the job diagram draws `prices` without its partition count | open |
| F4 | the disk self-test depends on `backlog.count` | fixed in #166 |
| F5 | host memory decided both suites; the wait checks CPU, not memory moved out to disk; no instruction for when nobody can close programs | open |
| F6 | `headline.md` shows means for cases the table threw out, unmarked | open |
| F7 | the ceiling advice ("about 8192m" of Kafka memory) cannot fit this machine and does not name the three ways out | open |
| F8 | the self-test's output is not plain English | open |
| F9 | re-running one step writes no `DONE`, and silently overwrites the previous suite's files | open |
| F10 | SKILL.md and the README are about 40,000 tokens before the first command | open |
