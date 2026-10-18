# Skill feedback — clean-room run 56

Skill commit `60a789c` at `~/.claude/skills/scalable-flink-skill`, level with
`origin/main` when the run started. Findings in the order they cost time.

## F1. Every vertex must run at the case's parallelism, and nothing says so until the tiny proof — 25 minutes lost

The default business case has a second input, prices: 16,384 small records
broadcast to both aggregations. I read it with a source at parallelism 1, which
is the natural way to read a small broadcast feed. Preflight passed, and
completeness passed both arms (the job ran at parallelism 1 there, so nothing
could differ). Then the tiny proof's 4-core case stopped:

> `STOPPED (rig): vertex parallelism != 4: [('Source: kafka-source-prices', 1)]`

The only place the rule is written is §6's table: *"parallelism ≠ cap ≠
allocated slots | all three read back from the engine on every case"*. That
reads as "the job's parallelism equals the cap", not "every vertex, including
a side input's source, must run at the cap". §1 q2 says of the price feed only
*"A price feed is a topic you create, fill and read, named in neither list,
and the harness leaves it alone"* — but the harness does not leave its source
alone.

Cost: chain 1 ran 25.5 minutes (up, preflight, both completeness arms, a
tiny-proof fill of 80,000,000 records and three tiny cases) to find a rule a
static check could have found in the first minute.

**Should say**, in §1 q2 next to the price feed: *"Read it with a source at the
job's parallelism, `{par}`, like every other operator. The harness checks that
every vertex in the running plan runs at the case's parallelism, and a
side-input source at parallelism 1 stops the tiny proof."* And the check
should run where the plan is first read — the design diff after completeness
already reads the running plan and could flag any vertex whose parallelism is
not the job's. It would not have stopped there (the completeness case is the
1-core case), so better still: preflight or completeness should submit at the
largest case's parallelism once, or the design diff should report "a vertex
pinned to a parallelism of its own" from the plan's `parallelism` field.

Also: after the 4-core and 2-core tiny cases had both stopped for a fault that
is the same at every case (a rig fault, by the harness's own label), the tiny
proof still ran the 1-core case for 2 m 46 s. §6 says *"A failed check stops
the suite — when it is about the rig"*; the tiny proof should do the same.

## F2. The fill-stall guard reads only the scaled input; a generator that writes its own second input first looks stalled

The skill tells the generator to write the price feed once (harness README:
*"If the generator writes it, write it once and do not let it depend on
`{count}` or `{seed}`"*). Mine wrote the 16,384 prices before the orders, with
an acknowledged send each — my bug, 5.5 minutes. During those minutes the
harness logged *"fill: 0 of 8,000,000 records written to orders-small (0%), 1
min in"* five times. It would have stopped the fill at 10 minutes
(`fillStartS` 600) saying *"the generator has written nothing"*, which would
have been true of the orders and false of the generator. Not a harness defect
as such; the README could say *"the stall guard watches `topics.in` only:
write anything else the generator owns quickly, or after the input"*.

## F3. The job graph draws the prices topic without its partition count

`graph.md` draws `orders` as *"orders 8 partitions"* and `prices` as just
*"prices"*. `graph_context` sets `partitions: None` for every input other than
the scaled one. The harness knows `partitions` and could read the real count
off the broker for each `design.inputs` topic.

Otherwise the picture was right first time: one box per task, every operator
inside, keyBy edges labelled with the key set and count, both broadcast edges,
every output tied to its Writer, "every 10 s" on both market values. The
sink-naming rule (*"Name every sink so its operator name contains the topic it
writes"*) and the `keySets` stage-name rule were clear enough to follow without
a second completeness run.

## F4. A harness self-test depends on the pipeline's own `backlog.count`, and stops every tiny proof whose backlog is under 250,000,000 — 25 minutes lost

Chain 2 passed completeness on both arms and measured all three tiny cases
(1→2 1.855×, 2→4 1.851×, inside the bounds), then stopped:

> `BAD disk: input already on the broker is not asked for twice (must not fire) -> WRONG ERROR AssertionError: (125719476736, 89719476736)`
> `355/356 guards fired as expected` / `TINY PROOF STOPPED`

The self-test `disk_input_already_there` (prove.py ~line 3581) calls
`projected()`, which stubs the topic sizes to give 200 bytes a record, and
asserts that 50 GB already on the broker lowers the need by exactly 50 GB. But
`disk_verdict` (lib.py ~line 931) caps the credit at the suite's own input size:
`already = min(inp, ...)` with `inp = in_bytes_per_rec * backlog`, and
`backlog` is **this run's** `backlog.count`. At 180,000,000 × 200 B = 36 GB
the credit is 36 GB, and the observed gap is exactly that: 125.7 − 89.7 =
36.0 GB. So any `backlog.count` under 250,000,000 fails a test that is
supposed to be pure. My measured need was 96,579,045 (the tiny proof's own
line: *"backlog: 180,000,000 configured, 96,579,045 needed"*).

The guard being tested is right; the test is wrong. **Should be**: the
self-test passes its own backlog to the projection (or asserts
`cold − warm == min(50 GB, backlog × 200 B)`), so it does not read the user's
config. And a self-test failure inside `tinyproof` should say *"this is a
fault in the harness's own self-test, not in your pipeline"* — the message as
printed gives a pipeline author no way to tell, and the only way round it
without forking the harness is what I did: raise `backlog.count` to
250,000,000, which writes 4.6 GB more Kafka log than the run needs and costs a
third chain from the top (the build hash covers the generator settings).

## F5. Host memory decided both suites, and the skill has no route for it when nobody can close the programs holding it — two suites, 94 minutes, no table

Both suites on build e35f29848e13 ended with no scaling result. Every case
pinned its cap (95.0–100.4%), vantage under 1.6%, GC under 3.1%. The report
tied every slow pass to the host's swap:

> `4 cores: the slower pass (313,830/s) was measured while the machine had 3.8-3.9 GB of its memory moved out to disk, the faster passes (391,822/s, 421,902/s) at 1.6-1.6 GB. The machine was short of memory, not the pipeline. Close what else is using memory and measure again.`

and in the re-run, 4.4–5.1 GB for the slow passes against 1.6–2.5 GB for the
fast ones. The harness was right to throw the cases out and right to name the
swap: that is the most useful line it printed all day. But:

1. **Preflight saw it coming and pointed at the wrong step.** Its row said
   *"the Docker VM takes 12.7 GB, leaving 3.3 GB for macOS, this harness and
   the generator ... Run 45 swapped 3 GB and stalled its generator with 3.3 GB
   left over, so watch the fill if this figure is near that."* The fill was
   fine (292 s). The suite is what the swap broke. Should say: *"With 3.3 GB
   left for macOS, any program the user opens during the suite pushes memory
   out to disk, and a pass measured then reads 20–25% slow (run 56: 2 suites,
   5 of 6 thrown-out passes). Close memory-heavy programs before starting."*
2. **The before-case wait checks CPU only.** It waited correctly for
   `mediaanalysisd 284%` and `corespotlightd 148%`, then started cases while
   swap was climbing. A pass measured during a swap rise is thrown out after
   it has cost 4–10 minutes. The case already records `hostSwapOpenMB`; a wait
   on swap rising across the 30 s re-check would cost minutes instead of
   suites. What threshold, I do not know: in this run fast passes sat at
   1.5–2.5 GB and slow ones at 2.7–5.1 GB, two suites on one machine, which is
   not enough to set one. Replay it against the record first.
3. **"Close what else is using memory" has no branch for nobody-to-ask.** §6
   gives that branch for a busy CPU (*"With no human to ask, wait"*) and for
   broker memory (*"With nobody to ask, take the third and carry on"*), but
   not for host memory. The holders here were the user's own programs
   (Chrome, Numbers, ChatGPT, Mail). I re-ran the suite once, unchanged, with
   the prediction in `FIXES.md`, and stopped there. The skill should say how
   many re-measurements are worth it and when to stop.

## F6. `headline.md` publishes means for cases the table threw out

> `| 1 | 116,161 | 3 |` … `| 4 | 341,760 | 3 |`

Both rows are marked UNREPORTABLE in `suite.txt` (21.0% and 28.9% spread), and
the 1-core "3 passes" leaves out that one of them was a CEILING and the
sentinel made four. A reader of the file the README is told to link sees two
clean-looking numbers. Should mark the row: *"not counted: readings 21.0%
apart"*, as the step table below it already does.

## F7. The ceiling advice names a broker size this machine cannot hold

The re-run's 1-core pass at 95.0% of cap was classed a ceiling with:

> `Raise caps.kafkaMemory from 5120m to about 8192m.`

Worker 4,560m at 4 cores + broker 8,192m + job manager 2,048m + dashboard
960m = 15,760m against a 12,955 MiB VM. SKILL.md §6 says *"when the advice
does not fit the machine, the harness says so and names the three ways out"*;
this line did not. The pass also read 125,258/s, faster than the two 1-core
passes kept beside it, so "the likely reason" did not slow it.

## F8. A self-test failure is printed as code, not plain English

> `BAD disk: input already on the broker is not asked for twice (must not fire) -> WRONG ERROR AssertionError: (125719476736, 89719476736)`

"BAD", "WRONG ERROR", "AssertionError" and a tuple of byte counts. SKILL.md §6:
*"Every message a person reads is plain English"* and `doccheck.py` enforces
the table — but not on the self-test's own output. Should read: *"The
harness's own check of its disk projection did not pass: with 50 GB already
on the broker it expected the need to fall by 50 GB and it fell by 36 GB.
This is a fault in the harness, not in your pipeline."*

## F9. Re-running one step has no `DONE` and nothing to wait on

`harness/README.md` says *"Type the steps yourself only when one of them needs
re-running"*, and SKILL.md §5 says to wait with `watch.sh` on `results/DONE`.
`prove.py suite` and `prove.py report` on their own write no `DONE`, so
`watch.sh` cannot wait on them. I wrote a wrapper that ran `suite` then
`report` and touched a file in `scratch/`, and a loop on that file — the
exact thing the skill tells agents not to write. Should: every step writes
`results/DONE` (or `watch.sh` takes the step's name), and re-running `suite`
should warn that it overwrites the previous `suite.json` / `suite.txt` /
`headline.md` (chain 3's table survives only in git because I committed it
first).

## F10. SKILL.md is 1,504 lines, and most of it is history

SKILL.md (108,902 bytes) plus `harness/README.md` (55,373 bytes) — required
reading, about 40,000 tokens before the first command. Most paragraphs carry
the run that paid for the rule (*"Clean-room run 45 ..."*, *"Measured
2026-09-05 ..."*). That history belongs in the project's findings; the skill
needs the rule, the reason in one sentence, and the check that enforces it.
The two harness-README cells for `platform` and `topics.in` are each a page
long. A shorter skill would have made F1 easier to find, because the rule
that cost me a chain was one cell of a table.

## How well the skill supported each thing I was asked about

| area | how it went | were the harness's checks right |
|---|---|---|
| the pipeline's own README | The section table in §2 was enough to write it first time; preflight's row *"README.md names all 15 topics and operators"* passed on the first run | yes |
| keeping the evidence in git | `results/.gitignore` written for me; every stop printed *"evidence: not committed yet: ..."* with the file list, which I followed at each stop (5 commits of results). Gap: F9 (a re-run overwrites the last table) | yes; the reminder was useful |
| the job diagram (`graph.md` / `graph.png`) | Right first time: 4 tasks, 13 operators, keyBy edges with 4,096 and 16,384 keys, both broadcasts, "every 10 s" on both market values. Only gap: F3 (no partition count on `prices`) | yes |
| the dashboard | Built at `up` with no work from me: rows 1–6 in order, 27 panels, lag panels on every topic, broker network bytes and share of a 10 Gbit/s link, distinct keys per output (read back: 4,096 / 16,384 / 4,096 / 16,384), opens on the suite's range. *"every panel shows data over the suite"* | yes. The `prices` lag line is empty because my job reads it without committing offsets; that is my choice, and the skill could say a broadcast side input read without a group has no lag to show |
| naming operators and sinks | The rule *"Name every sink so its operator name contains the topic it writes"* and the `keySets` stage-name rule were clear; the design diff found all 9 operators and all 6 topics first time | yes |
| the machine sleeping or on battery | Preflight: *"on mains power, battery 95%"*; `caffeinate` held it awake; no case slept | yes |
| the disk check | Preflight's placeholder said 120 B/record and called itself an assumption; the tiny proof replaced it with the measured 66 B/record and said *"need 54.8 GB ... 111.4 GB: FITS"*. Clear about which way it went | yes |
| waiting for the chain | `watch.sh` with `WATCH_MINUTES=8`, from the parent directory, through a two-line script, as §5 says. Never lost a shell; 17 waits across 3 chains. Gap: F9 | yes |
