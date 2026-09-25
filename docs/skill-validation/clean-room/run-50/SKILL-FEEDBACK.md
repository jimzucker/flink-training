# Skill feedback — clean-room run 50 (SQL)

Every place the skill or the harness was wrong, unclear, out of date, or cost
time. Quoted wording first, then what it should say. Findings are numbered in
the order they were hit.

## F1. SQL gets one sentence in the whole skill, and none of the build advice applies to it

The only mention of SQL in `SKILL.md` is question 5:

> "**Which Flink API should we use?** DataStream, where the developer has more
> control over the execution graph, or SQL, where the optimizer makes more of
> the decisions. Neither is more valid, but the claim differs, and it is said
> next to the numbers."

`grep -i sql` over `SKILL.md`, `harness/README.md`, `prove.py` and `lib.py`
finds nothing else (the example config's `apiLevel` says "no SQL"). Everything
that tells a builder *how* to build is DataStream-only:

- §1 q1: "Broadcast the prices to both aggregations instead." Streaming SQL has
  no broadcast join hint. The SQL answer is a regular join on `symbol`, which
  re-partitions the account-level positions by symbol — and that is fine for
  per-key order, because every account key carries exactly one symbol. The
  skill should say so.
- §6a "read the input once": "Parse once and fan out through a side output —
  not a second source read, and not two chained operators". In SQL this is the
  planner's source and sub-plan reuse inside one `STATEMENT SET`
  (`reuse_id=[1]` in `EXPLAIN`), which the builder does not write and can only
  check. The skill should say: *in SQL, put every INSERT in one STATEMENT SET
  and confirm one source scan in the plan.*
- §1 q2 "throttle it to a configurable interval": SQL has no timers. I used a
  10-second processing-time `TUMBLE` window of deltas, then a running `SUM` of
  the window deltas, then the price join. That took reasoning the skill could
  have given in two lines.
- §4 ordering: in SQL, a `GROUP BY` whose new value equals the old one emits
  nothing (Flink's group aggregate suppresses unchanged results when no state
  TTL is set — from my knowledge of Flink, not verified in this run). A signed
  quantity can net to zero, so "one output per input" is only guaranteed if
  the aggregate carries something that always changes. I added `COUNT(*)`,
  which also serves as the ordering witness. A DataStream build never meets
  this.

**Should say**, somewhere in §1 q5 or §6a: a short "if the answer is SQL"
block — one STATEMENT SET, check the plan with `EXPLAIN CHANGELOG_MODE`,
upsert-kafka sinks keyed by the aggregation key, a window for the throttle,
a count column so every input changes the row, and a regular join on symbol
instead of the broadcast.

## F2. The key-layout preflight row hashes key strings; a SQL job hashes key rows

`harness/keycheck/KeyCheck.java` calls
`KeyGroupRangeAssignment.assignToKeyGroup(key, maxPar)` on each key as a Java
`String`. A SQL `GROUP BY` keys its state on a binary row
(`BinaryRowData`), whose hash is not `String.hashCode()`. So for a SQL job the
row "the keys divide evenly across subtasks" is a prediction about a different
job. **Measured** (`scratch/keyhash/KeyHash.java`, run against the image's own
`lib/` jars): for the 4,096 symbol keys, the key group Flink assigns a
one-field `BinaryRowData` holding the symbol matches the group for the plain
string in **39 of 4,096** cases — 1 in 105, which is chance at 128 key groups.
At four subtasks the string layout is 1035/1004/1032/1025 and the row layout
1026/1028/1011/1031. Both happen to be even at this cardinality, so it did no
harm here; with the 16 keys the skill uses as its worked example, the row's
suggested `pipeline.max-parallelism` (1115) would be tuned for a layout the SQL
job never uses. (The row as I built it is my construction of what the SQL key
selector produces; that it is byte-identical to the planner's key was not
verified.)

**Should say:** "For a SQL or Table API job, this row predicts where the key
*strings* land, not the key rows the planner hashes. Treat it as unchecked."
Better: KeyCheck could take a mode that builds the `BinaryRowData` key.

## F3. The scratchpad this session was given sits inside a directory the task forbids, and other runs' files are in it

The session's scratchpad directory is under `<tmp>/...`,
which the task's boundaries forbid reading. I wrote two of my own files there
before noticing (a copy of the Flink image's `lib/` jars and an `EXPLAIN`
output), and then **read a file another run had left there**: I wrote
`m1.json` to that directory for a determinism test, my command failed before
writing it, and the `head` that followed printed the first 600 bytes of a
manifest someone else's generator had written (field names such as
`orderCount`, `predictedSymbolKeys`, `priceRecords: 16384`, symbols named
`SYM0000`). My own design was already written in `ASSUMPTIONS.md`, `PLAN.md`
and the code at that point, and I changed nothing because of it, but the
reader should know it happened. From then on all scratch files went to
`scratch/` in the run directory.

**Should say (to whoever runs the clean room):** give the agent a scratch
directory that is not shared between runs and not inside a forbidden tree.
The skill's own advice — "a two-line script in a scratch directory with the
path written into it" — sends agents to exactly this shared place.

## F4. The chain said PASS with half the claim missing, and the scorecard quietly renamed the baseline

`pipeline.json` says `"baseline": 1` and the plan (§1a, which the harness
checks) promises two steps, 1→2 and 2→4. In chain 1 every one of the four
1-core passes came back a ceiling (garbage collection 8.2–9.7% of the case's
time, limit 5.5%). The result:

- `results/DONE`: `PASS 70.5 min`
- the scorecard: "**2 cores is the baseline.** Do not tune it: making the
  baseline faster makes the step off it smaller … The only thing worth fixing
  here is a way it differs from the other cases"

Both are wrong for this run. The baseline is 1 core; it was thrown out, and the
fix for it (memory for the 1-core case) is exactly what the scorecard tells
the reader not to do. **Should say:** "PASS for 2→4 only. 1→2 was not
measured: all 4 one-core passes were thrown out as ceilings (garbage
collection 8–10%, limit 5.5%). Give the 1-core case more memory and measure
again." `DONE` should not say PASS when a step the plan promised has no
number.

## F5. The tiny proof knew the baseline was a ceiling and let a 43-minute suite start anyway

§3: "run every case the suite will run … Every step the suite will report
gets bounded now". Tiny proof 1 read the 1-core case as a ceiling (GC 10.4%)
at 22:28. The chain carried on into the fill and a 43-minute suite, which
could never report 1→2. The skill's own reason for the tiny proof — "a bad
one-unit case costs ten minutes rather than a suite" — is exactly this case,
and the harness did not act on it. **Should:** stop at the tiny proof when the
configured baseline is a ceiling, with the sentence from F4. At the least,
put it in `PROGRESS.txt`.

## F6. The ceiling message asks for more memory and gives no number

> "The pipeline ran short of memory, not cores. Give it more memory instead of
> more cores."

The harness names the number for the backlog ("23,370,052 needed at the
measured 103,867 rec/s") and for the broker, but not here, although it has
what it needs: GC was 10.4% / 3.5% / 1.1% at 1 / 2 / 4 cores, the classic
fixed-overhead shape its own README describes ("at 1280m per core with no
base, the rig read GC 17.4% at one core against 3.4% at two"). **Should say:**
"raise `caps.tmMemoryBase`; GC falls steeply with size here, so the fixed part
is too small", with a figure. I spent two tuning rounds (see `FIXES.md`)
guessing it; my first guess, +672m at 1 core, took GC from 10.4% only to 6.7%.

## F7. The headline prints a range whose low end misses the target beside the word "met"

`suite.md`: "**2→4 cores: 1.87× (target 1.80×), range 1.75–1.96× across
passes.**" and the scorecard: "2->4 cores: doubling gave 1.87x, target 1.80x ->
met". The claim is judged on `ratioLowCI` = 1.818 (in `suite.json` only), not on
the 1.747 a reader sees. **Should print the number it is judged on:**
"1.87× (judged on 1.82×, the low end of its interval; target 1.80×) — met.
Passes ranged 1.75–1.96×."

## F8. Design operators are matched after characters are stripped, and the skill does not say so

`design_diff` compares declared operators with `vertex_label()` output, which
deletes every character outside `[A-Za-z0-9 ()/,.:+_<>-]`. A SQL plan is made
of brackets and equals signs: the running vertex
`GroupAggregate(groupBy=[sym], …)` is matched as `GroupAggregate(groupBysym, …)`,
so declaring the natural `GroupAggregate(groupBy=[sym]` would fail the design
diff for a build that is right. I avoided it by declaring bare words
(`WindowAggregate`, `InnerJoin`, the sink table names). **Should say** in the
`design` field note: "operators are matched as lower-case substrings of the
vertex description after `[`, `]`, `=`, `$`, `*` and similar are removed; for
a SQL job, name the sink tables and the operator kinds."

## F9. Something else loaded the machine during the suite, and it was reported but not acted on

Host load average reached 12.0 on 8 cores during the 4-core p1 pass and 8.7–9.1
at the open of two p3 passes (from `suite.json`). The scorecard says so ("the
machine was busy with something else … Close what else is running and measure
again") and kept the passes. That is a fair call — the 4-core pass under load
12.0 read 102,707/s against 106,728 and 106,517 for the other two, a 3.7%
difference inside the noise a two-pass ratio carries — but I cannot say what
the load was (this agent's own session, other sessions on the host: not
measured). Not a skill defect; recorded so the numbers are read with it.

## F10. Small frictions

- `harness/README.md`: "Wait with `until [ -f results/DONE ]; do sleep 30;
  done` and nothing more." The same README and §5 then say a command line
  naming the project directory can be killed by the sweep, and to use
  `watch.sh` from a scratch script. The first instruction should go, or say
  "from a script outside the project".
- `SKILL.md` §3 lists "the keys divide evenly across subtasks" as a preflight
  PASS for this SQL build; see F2 — it passed on a layout the job does not use.
- The completeness step and the design diff worked first time for a SQL job:
  the fan-out (5.0 read back), both market values, every declared operator,
  and the kill test (at 46%, one backward step per key, totals exact). The
  contract in `harness/README.md` was enough to build against without the
  example. That part of the skill is good.

## F11. The 5.5% garbage-collection ceiling comes from other pipelines, and it threw out every 1-core pass of this one

`harness/README.md`: "A case whose garbage collection takes more than 5.5% of
its capacity is a ceiling, not a result. That figure is measured, not chosen:
across fourteen recorded runs every case that behaved sat at 0.35-4.8%". Those
fourteen runs were DataStream builds. This SQL build's 1-core case sat at
6.7–10.4% across 3 tiny proofs and 8 suite passes, at 99–100% of its CPU cap,
and **did not respond to memory**: +672m took it from 10.4% to 6.7%, a further
+624m left it at 6.8% (`FIXES.md`). Why it stopped responding: I do not know;
not measured.

What the thrown-out passes read, for the record only (the harness is right not
to count them, and these are **not** reportable numbers): suite 1's four 1-core
passes averaged 28,357/s against a 2-core mean of 56,231/s (1.98×); suite 2's
averaged 27,953/s against 57,925/s (2.07×). So the rule costs this run its 1→2
step while the 1-core rate lines up with half the 2-core rate.

**Should say:** that the ceiling was set on DataStream jobs; that a SQL job
allocates differently and may sit above it at one core with memory no longer
helping; and what to do then — the skill's lever table (§6a) has only "memory
per subtask" for this symptom, so a run that has tried it has nothing left
and ends with 1→2 unmeasured. Either a garbage-collector row in §6a (and a
statement of which JVM flags the harness allows beside the G1 pin), or say
plainly "claim 2→4 only".

## F12. Two suites of one build, 2.7% apart, landed on opposite sides of the target

Suite 1: 2→4 = 1.873×, judged on 1.818× → met. Suite 2 (worker memory changed,
fix 1): 1.823×, judged on 1.759× → missed. Their intervals overlap. The
`ceiling` run afterwards (broker squeezed from 2.5 to 1.0 to 0.5 cores at 4
pipeline cores: 103,298 → 101,230 → 104,536/s, pipeline 99–100% of cap) says
the broker is not the limit, and `probe --repeats 9` says this host's own
memory-heavy work returns 1.54× on 2→4 (middle half 10%) and simple arithmetic
1.97×, so the pipeline sits between the two arms and the host cannot be ruled
in or out. The skill's rule — revert a change that reads worse — made me revert
a change whose effect was smaller than one suite's own uncertainty. **Should
say:** when both arms' intervals overlap, the change is "no measurable effect";
revert it for simplicity, but do not record it as having made the step worse.
