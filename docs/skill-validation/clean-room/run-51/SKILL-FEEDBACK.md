# Skill feedback — clean-room run 51 (SQL build)

Each finding quotes the skill or the harness verbatim, says what it cost, and
says what it should say instead. Ordered roughly by what it cost.

## How well the skill supported the SQL choice

**Well, and much better than I expected.** The six-row table under question 5
("If the answer is SQL…") was the single most useful thing in the skill. I
followed it literally — one `STATEMENT SET`, `CROSS JOIN UNNEST`,
`upsert-kafka` sinks keyed by the aggregation key, a `COUNT(*)` column in
every aggregate, a 10-second processing-time `TUMBLE` then a running `SUM`,
and a regular join on symbol — and the job was correct on its first run:
the verifier passed on 1,000,000 orders with every assertion, before the
chain started. `EXPLAIN` showed the orders topic scanned once
(`reuse_id=[1]`). What the table left out is in findings S1–S5 below: every
one of them is a place where the skill's *other* sections still assume
DataStream.

## Findings

### S1. `job.sourceVertexMatch` has no SQL guidance

The only example is DataStream's: `"sourceVertexMatch": "kafka-source"`, and
`harness/README.md` says only *"substring of the source vertex name in the
running plan (busy/idle/back-pressure are read for it)"*. A SQL job has no
operator you name: the planner names the source vertex
`Source: orders[38] -> (Calc[39], Calc[43] -> Correlate[44] -> …)`, after the
**table** name. Nothing in the skill says so. I had to run the job once to
find out. If the match finds nothing the source idle column silently reads
empty (lib.py: `src = bp[src_name[0]] if src_name else {}`) — no message.

**Should say** (in the question-5 SQL table): *"the source vertex is named
`Source: <table name>[n]` — set `sourceVertexMatch` to `Source: <your input
table>`."* And the harness should stop the run when the match finds no vertex,
rather than recording `sourceIdle: null`.

### S2. The `design` block's operator names do not work the obvious way in SQL

§4 says *"the design is declared as lists that can be diffed … the operators,
the topics read, the topics written — and the completeness run looks for each
one in the running plan"*. The example names DataStream uids (`"parse"`,
`"positions-by-symbol"`). In SQL the obvious declaration is the fully
qualified table name the `EXPLAIN` prints, `default_catalog.default_database.positions_by_symbol`
— and it **fails**: the running plan describes a sink only as
`positions_by_symbol[42]: Writer`, and a source as
`TableSourceScan(table=[[default_catalog, default_database, orders]], …)`,
which `vertex_label` then strips of `[`, `]` and `=` before matching. I found
this by running `L.design_diff` against a plan I saved from a smoke run; a run
that did not do that would have lost a completeness drain to it.

**Should say:** *"In SQL, declare each sink by its table name alone
(`positions_by_symbol`), each source as `default_database, <table>` (the
brackets are stripped before matching), and operators by the planner's own
words: `correlate`, `windowaggregate`, `rank`, `join`."*

**Also:** the match is a plain substring test (`op.lower() in flat`), so a
declared `join` is satisfied by *any* vertex containing "join". It is a loose
check and the table the harness prints (`built: True … in the running plan`)
reads as a strict one. Say it is a substring match.

### S3. The key-spread preflight row is DataStream-only, and still gates a SQL run

The question-5 text says: *"The key-spread check in preflight hashes keys the
way DataStream does, which is not how a SQL job lays out its keys: treat its
PASS as unchecked for SQL."* That covers a PASS. It says nothing about a
FAIL — and §6 says *"an uneven one stops it when a `pipeline.max-parallelism`
exists that would even it out"*. So a SQL run can be **stopped** by a check
the skill itself says does not describe a SQL job, and then be told to add a
`pipeline.max-parallelism` to fix a layout nobody measured. It did not bite
here (4,096 symbols spread evenly by any hash), but a small key set would.

**Should say:** when `apiLevel` names SQL, the row reports *"not checked: SQL
keys on a binary row, not the key string"* and never stops the run. Better
still, hash the key the way the SQL runtime does.

### S4. The generator and the verifier run on the host with only the job jar — the contract never says so

`harness/README.md`: `generator.cmd` *"fills `{topic}` with `{count}`
records…"*, `{java} -cp {jar}` in the example. It does not say that `{jar}` is
the **only** thing on that classpath. A Flink job jar is normally built with
the Flink and Table APIs `provided` and SLF4J excluded (they are in the
image). Run on the host, that jar has neither. I lost two rebuilds to it:

1. `ClassNotFoundException: org.apache.flink.table.api.StatementSet` — my
   generator reused an argument parser that lived in the job class, so the
   JVM loaded the Table API. A DataStream build would hit the same thing with
   `StreamExecutionEnvironment`.
2. `ClassNotFoundException: org.slf4j.LoggerFactory` — `kafka-clients` needs
   `slf4j-api`, which I had excluded as "provided by Flink".

**Should say:** *"The generator, the verifier and any second-vantage command
run in a host JVM with your jar as the whole classpath. Keep them free of
Flink classes and bundle `slf4j-api` (and a JSON library if they parse
JSON)."*

### S5. The watcher advice cannot be followed as written by an agent told to keep its files in its own directory

§5: *"put the path inside a file, not on a command line … a two-line script
in a scratch directory with the path written into it, invoked by its scratch
path"*, with the example `printf '…' > /tmp/scratch/w.sh` and `sh
/tmp/scratch/w.sh`. My brief (and any sensible clean room) forbids `/tmp`,
so the scratch directory is inside the project — and then *"invoked by its
scratch path"* puts the project directory on the command line, which is
exactly what the teardown sweep kills. I got round it by `cd`-ing to the
parent and invoking it by a **relative** path
(`cd ~/code/GitHub && sh flink-skill-test-51/scratch/w.sh`), so neither the
wrapper nor `sh` carries the absolute project path.

`harness/README.md` is worse: its command list shows
`PROJECT_DIR=$PWD sh $(dirname $H)/watch.sh`, which only works from a shell
already sitting in the project — the very thing the same file says an agent
cannot reach (*"'a shell whose working directory is the project' is
unreachable by that route"*).

**Should say:** *"If your scratch directory is inside the project, invoke the
script by a path relative to the project's parent. The sweep matches the
project's absolute path."* And drop the `$PWD` line.

### S6. Question 2's default does not say every order has four allocations

*"If the order has 4 allocations it emits 5 records."* is conditional. The
harness needs a **constant** fan-out (`outputsPerInput`), and §5 forbids a
fan-out that is a property of the test data. So the default only works if
every order has exactly four allocations — which is what the example's
`outputsPerInput: 5` assumes and never states. I wrote it down as assumption
A1.

**Should say:** *"Every order carries exactly 4 allocations, one per account /
sub-account, so it emits 5 records."*

### S7. "Duplicates have to be handled" contradicts "a unique id" unless you read §4 a particular way

Question 1: *"an order arrives with a unique id"*. Question 4: *"Duplicates
have to be handled and not double counted, in all cases."* Question 4's note
says *"How the duplicates are handled is a build decision, not a question:
§4"* — and §4 only discusses **replay** duplicates (exactly-once state plus an
absolute-value sink). A reader has to infer that "duplicates" means
redelivery, not repeated order ids. If it meant repeated ids, a SQL build
would need de-duplication state over every order id ever seen, which changes
the design and the throughput materially. I assumed redelivery (A4).

**Should say:** *"Duplicates here means records redelivered after a failure;
order ids in the input are unique."* — or, if input duplicates are meant,
say so and say how many.

### S8. The SQL table says "then the latest price" and stops

*"a 10-second processing-time `TUMBLE` window of the position changes, a
running `SUM` of those per key, then the latest price"*. There are at least
three ways to get "the latest price" in Flink SQL — `ROW_NUMBER() … ORDER BY
ts DESC` = 1, an `upsert-kafka` source keyed by symbol, or a temporal join —
and they behave differently at startup and on replay. I used `ROW_NUMBER`
(the plan shows `Rank(strategy=[AppendFastStrategy])`). One more row in the
table would save the choice.

### S9. The tiny proof stops the chain on one 30-second window per case, and its stop message blames things it had already checked

Chain 1 stopped with:

> *"STOPPING: 4 cores did only 1.49x the work of 2. It should be about 2x.
> The rig is not set up the way you think. Check three things: the CPU cap,
> the partition count, and the backlog size."*

All three had already been checked by the harness, in the same log: the cap
read back at 99.3% of 4 cores, preflight passed "8 partitions / parallelism
[1, 2, 4]", and the tiny proof itself printed "60,000,000 configured,
15,765,615 needed". The message sends the reader to three things the harness
knows are fine.

What actually happened is in `tinyproof.json` and nowhere in the message: the
4-core case's four warm-up intervals read 88–98k rec/s, and then its 30 s
window held one checkpoint interval at **48,866/s**. One interval in three at
half rate took the case from ~94k to 70k and the step from ~2.0x to 1.49x. A
longer measurement at the same size (`prove.py ceiling`, 60–70 s windows) read
85–91k.

§5 says *"Every case at least twice, and report the spread"* and *"One
measurement is a guess"* (the project's own rule). The tiny proof measures each
case **once**, over three commit boundaries, and stops the whole chain on it.

**Should say / do:**
1. When a step misses its bound, print the per-interval rates of both cases
   and say whether one interval sits far off the others.
2. Re-measure the short case once before stopping (as §6 already says for a
   plainly transient failure), or widen the tiny window to more boundaries.
3. Name only the checks that did *not* pass. If cap, partitions and backlog
   are all confirmed, say so and say the cause is not yet known.

### S10. `prove.py ceiling` silently needs the suite backlog, which a stopped chain never wrote

§6: *"`prove.py ceiling` — is the largest case already against a ceiling? …
A few short cases on the stack that is already up."* The skill tells you to
run it exactly when a step falls short — which, in the tiny proof, is before
the fill. `cmd_ceiling` starts with `load_json("manifest.json")`, which only
the fill writes. I had to run `prove.py fill` by hand (2 minutes, 13.7 GB)
first. Say so: *"ceiling drains the suite backlog; after a tiny-proof stop,
run `prove.py fill` first."*

### S11. The tiny proof's high load average is recorded and never mentioned

The 4-core tiny case recorded `hostLoadOpen 8.55`, `hostLoadClose 10.0` on 8
cores; §5 says a load of 7.18 on 8 cores voided run 34. Nothing in the tiny
proof's output mentions it. (The ceiling cases ran at 7.7–13.3 and did not dip,
so I do not claim the load caused the dip — but a reader should be told.)
Much of this load may be the pipeline's own threads: the SQL job runs 11
vertices, so 44 threads at 4 cores. Not measured.

### S12. The tiny proof's single pass per case is noisier than the bounds it enforces — and `all` cannot resume past it

Three tiny proofs of **one unchanged build** (FIXES.md):

| tiny proof | 1 core | 2 cores | 4 cores | 1→2 | 2→4 | verdict |
|---|---:|---:|---:|---:|---:|---|
| chain 1 | 20,791 | 47,150 | 70,069 | 2.268x | **1.486x** | stopped (2→4 under 1.50) |
| chain 2 | 18,503 | 47,739 | 91,020 | **2.580x** | 1.907x | stopped (1→2 over 2.50) |
| alone, quiet host | 20,163 | 43,065 | 92,272 | 2.136x | 2.143x | passed |

The same build stopped the chain twice and passed once. Each stop cost a full
`all` (27 and 31 minutes: up, preflight, completeness, tiny proof), because
`all` has no way to start from a step — and completeness had already passed
for that exact build both times. §3 says the bounds are *"0.75×–1.25× of the
ideal"*; on this pipeline one pass per case moves more than that.

**Should do:** measure each tiny case twice and judge on the pair, like the
suite; and let `all` skip a step that has already passed for the same build
hash (`completeness.json` already records the build).

What I did instead, and a reader should know it: after the standalone tiny
proof passed, I ran `fill`, `suite` and `report` as separate harness commands
in one detached script, not `all`. The suite's own gate (completeness and tiny
proof passed for the same build hash) was satisfied.

### S13. `phases.log` is appended across chains, so waiting on a phase line matches the last chain

`mark()` opens `phases.log` with `"a"`. The skill tells agents to wait on
`results/DONE` (which is removed at chain start) — fine — but anyone waiting
for a *step boundary*, which §2 asks for (*"pass it on at each step
boundary"*), naturally greps `phases.log` for `phase=tinyproof end` and
matches the previous chain's line. I did exactly that and lost a wait. Start
`phases.log` fresh per chain, or say in the README that it accumulates.

### S14. The obvious way to wait on a standalone step kills itself

`harness/README.md` lists `python3 $H tinyproof` as a step you can type, and
the skill warns about loops that name `prove.py`. The natural wait for a
detached `tinyproof` — `while pgrep -f "prove.py tinyproof"; do sleep 20; done`
— matches **its own shell**, whose command line contains that string, so it
never ends. `watch.sh` only knows about `results/DONE`, which a standalone step
never writes. Say how to wait for a standalone step (a line the step prints,
or a DONE-style file per step).

### S15. The host's own background scans are invisible until they cost a case

At 09:03, with the stack idle, macOS's XProtect remediator was using 71–84% of
a core and the load average was 7.8 on 8 cores. Chain 2's 1-core window opened
at load 12.5. Preflight's "nothing else is using the cores" row is checked
once, at preflight, 30 minutes before the case it would have protected. The
per-case load is recorded (`hostLoadOpen`), but the tiny proof never looks at
it before stopping the chain on that case. When a case that stops the chain
opened at a load well above its own cores, say so in the stop message — it is
the first thing to rule out.

### S16. The report says the same step both "met the target" and was "never reported as met"

Suite A's report printed, for one number (2→4 = 2.121x):

> *"2→4 met the target, but the claim covers every step in cases, so it is not met."*

and, twenty lines lower:

> *"2->4 cores: doubling gave 2.12x, target 1.80x  ->  above 2.00x, so the smaller case reads low"*

§6 says *"A step above its ideal is never reported as met."* The first
sentence breaks that rule. Pick one: either a step above 2.00x is not met
(then the first line should say "2→4 read above 2.00x, which is not a result
on its own"), or it is.

### S17. A 1-core pass is thrown out with a reason that is only true because of the order the passes ran in

Suite A, first pass (ascending, so no 2-core pass existed yet):

> *"ceiling: garbage collection used 6.7% of this case's time and the limit is
> 5.5%, with no larger case under the limit to compare it with."*

The other three 1-core passes were thrown out with *"it did 10.6% less work
per core than the 2-core case"*. The first pass's reason reads as if no 2-core
case could be compared — there was one, three minutes later. Judge every pass
against the case means at the end of the suite, so one pass's verdict does not
depend on when it ran.

### S18. The SQL section's own warning about the 1-core case is correct — and it cost this run its first step

§1 q5: *"a 1-core case that spent 7–10% of its time on garbage collection,
over the 5.5% limit … a case that does as much work per core as the case above
it is now kept."* This run's 1-core case spent 6.7–8.2% on GC **and** did
10–12% less work per core than 2 cores, so all four passes were thrown out and
1→2 had no number. The skill warned about the GC but gave no memory setting
for SQL. The example's `tmMemoryBase 1088m` is a DataStream figure.

**Should say:** "For SQL, start with a larger base — this run needed about
1728m base (2240m at one core) to bring one-core GC near the limit." (Attempt 3
in FIXES.md; verdict in the final table.)

### S19. The build hash covers only the jar

`build_hash()` hashes the job jar and nothing else. `completeness` and
`tinyproof` are gated on it, so a change to `pipeline.json` — memory, caps,
`flinkProperties`, the job's arguments (including the market-value interval)
— leaves the gates "passed for this build". For memory that is reasonable.
For `job.args` it is not: a changed argument is a different pipeline, and
completeness would not re-run. Hash the job arguments in with the jar.

### S20. "Give it more memory instead of more cores" was the wrong advice here, and the harness said it four times

Every thrown-out 1-core pass carried: *"The pipeline ran short of memory, not
cores. Give it more memory instead of more cores."* I did (attempt 3: 1-core
process size 1728m → 2240m, +30%). Garbage collection did not move: 6.7–8.2%
before, 6.6–8.1% after. So on this pipeline the 1-core GC share is not a
shortage of memory, and the sentence states a cause the harness did not
measure. The skill's own SQL note already says a GC cut *"made it no
faster"* on run 50.

**Should say:** "Garbage collection took X% of this case's time. That is
over the limit and this case did less work per core than the next. More memory
may help; on SQL builds it has not (runs 50, 51)." — a finding, not a cause.

### S21. The throttled output is correct but the harness cannot see its cost

The market values run on a 10-second processing-time window. Their vertices
were busy 13–61% of the time at one core (`WindowAggregate[67]` for the
account level at 50–61%), second only to the source. Nothing in the scorecard
separates the throttled path from the per-input path, and §6a has no row for
it. Not a defect — a gap worth a sentence for SQL builds, where the window
aggregate is the operator doing the throttling.

## Summary for whoever reads this first

- **The SQL build was correct first time** and passed completeness with no
  tolerances on both arms in both chains (3,000,000 orders; killed arm stepped
  each key back at most once and ended exact). The skill's SQL table is why.
- **2→4 scaled: 2.121x (range 2.067–2.186x) with every case at its cap.**
  That clears the 1.80x target; the harness reads a step above 2.00x as the
  2-core case reading low.
- **1→2 has no number**: every 1-core pass was thrown out for garbage
  collection (6.7–8.2% against a 5.5% limit, with 10–12% less work per core
  than 2 cores). More memory did not change that. So the claim, as the
  harness states it, is not met.
- **Most of the lost time was the tiny proof**: one pass per case, 30-second
  windows, and bounds narrower than its own noise on this pipeline. It
  stopped two full chains on the same build it then passed.
