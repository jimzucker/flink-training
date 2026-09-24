# Skill feedback — clean-room run 49 (Confluent)

Every place the skill or the harness was wrong, unclear, out of date, or cost
time. Quotes are exact. Written as I went; the times are wall-clock.

## F1. `prove.py replay` crashes outside a project directory

`harness/README.md` lists it as
"`python3 $H replay          # thresholds vs the recorded runs — seconds, no stack`".
Run from the harness directory (before writing `pipeline.json`, to see the
thresholds) it dies with a Python traceback:
`FileNotFoundError: ... harness/pipeline.json`, from `build_table` calling
`cfg().per_case`. Cost: two minutes. It should either run without a
`pipeline.json`, or the README line should say "run it from the project
directory, after `pipeline.json` exists" and the command should print that
sentence instead of a traceback.

## F2. Two different instructions for how to wait

`harness/README.md`: "Wait with `until [ -f results/DONE ]; do sleep 30; done`
and nothing more." Two paragraphs later the same file says a command line that
names the project path will be killed by the reaper, and SKILL.md section 5
says "Use `harness/watch.sh` to do the waiting, rather than writing a loop" and
"If you are an agent, put the path inside a file, not on a command line." The
first instruction, followed literally from the project directory, is the one
that gets an agent's shell killed. Delete the `until` line from the README or
replace it with the two-line `watch.sh` script SKILL.md gives.

## F3. Question 2's default does not say every order has four allocations

"If the order has 4 allocations it emits 5 records." The harness needs a
**constant** fan-out (`outputsPerInput`), and question 3's default gives exactly
four account / sub-account pairs, so the only reading that works is "every
order has exactly one allocation per account / sub-account pair". But "if" reads
as one example among several allocation counts, and a generator built on that
reading has no constant fan-out and cannot use `constantFanOut`. It should say:
"Every order has one allocation per account / sub-account pair — four with the
default keys — so every order emits 5 records."

## F4. "Duplicates have to be handled" has two readings, and the skill picks one silently

Question 4's default: "Duplicates have to be handled and not double counted, in
all cases." Then: "How the duplicates are handled is a build decision, not a
question: §4." Section 4 only covers duplicates **from a replay after a
failure** (exactly-once state, absolute-value sink). It never says whether the
generator should also inject duplicate order ids and the job de-duplicate them —
which is what "duplicates ... in all cases" most naturally means to a trading
reader, and which would need state that grows with the backlog. I took the
replay reading and wrote it in `ASSUMPTIONS.md`. The default should say which
one it means, e.g. "Duplicates from a restart must not be double counted" — or,
if it means duplicate inputs, say so and say how many.

## F5. Confluent: how well the skill supported it (so far: well)

Question 6's table gave exactly what was needed — `confluentinc/cp-kafka:7.7.0`
and `/usr/share/java/kafka` — and nothing else had to change. Measured:

- `prove.py up` brought the Confluent broker up in 18 s on the first try. The
  harness already sets `CLUSTER_ID` (which cp-kafka requires and apache/kafka
  does not) and finds the tools at `/usr/bin/kafka-topics` rather than
  `/opt/kafka/bin/kafka-topics.sh`. Both fixes are dated today (2026-09-24) in
  `lib.py`; a run a day earlier would have hit "kafka never answered".
- The offset sampler compiled against the image's `kafka-clients-7.7.0-ccs.jar`.
- Preflight's "the build runs on either Kafka" row read the jar and reported
  "no Confluent-only classes in the job jar".
- The job, generator and verifier use Apache `kafka-clients` 3.4 (from the Flink
  connector) against the Confluent 7.7 broker with no problem.

What is missing: SKILL.md never says **what choosing Confluent changes and what
it does not**. It says "the answer picks which Kafka is measured", but not that
cp-kafka 7.7.0 *is* Apache Kafka 3.7 with Confluent's packaging, so a reader
expecting a different broker engine does not know the table measures the same
broker code at a different version (3.7 against the Apache default's 3.9 — from Confluent's published version mapping, **not verified in this run**; the image's client jar is named `kafka-clients-7.7.0-ccs.jar`, which does not say). That
is a version difference between the two arms, not a vendor difference, and
anyone comparing an Apache run with a Confluent run should be told. One
sentence under the table would do it. Also: `pipeline.example.json`'s
`_images` comment is the only other place the Confluent pair is written, and
the README's field table row for `images.kafka` does not mention `kafkaLibs` at
all — the field is absent from the field table.

## F6. The completeness drain gives a throttled output 2 seconds of margin, and says so nowhere

`cmd_completeness` waits until every record is committed, then sleeps
`checkpointMs + 2` seconds, then cancels the job and runs the verifier. A
throttled output — the default business case's market values, every 10 s —
emits its final value up to one throttle interval after the last input. With
the default 10 s throttle and a 10 s checkpoint that is 2 s of margin, and a
pipeline whose throttle is longer than `checkpointMs + 2` s will fail its own
market-value assertion for a reason that has nothing to do with the pipeline.
It passed here, but by construction, not by design. The README's
`topicsAlsoWritten` row should say: "the completeness drain cancels the job
`checkpointMs` + 2 s after the last input is committed, so a timer-driven
output must emit within that time" — or the drain should take a configurable
settle time.

## F7. The design diff is a substring match, so a sink name can stand in for a missing operator

`design_diff` checks `op.lower() in flat`, where `flat` is every vertex label
joined. My operator is named `positions-by-symbol+market-value-by-symbol` and
my sink `sink-positions-by-symbol`. If the aggregation were missing, the sink's
name alone would satisfy `positions-by-symbol`. The check is loose in exactly
the case it exists for (runs 36 and 37: a missing aggregation next to a sink
that still exists). Matching whole operator names (split on the chain
separators) would close it; failing that, the README should tell runs to name
operators so no declared name is a substring of another.

## F8. Reading cost before writing a line

SKILL.md is 1,138 lines (78 KB) and `harness/README.md` 407 lines (32 KB). Both
must be read in full first. Much of it is the history of which run broke what
("clean-room run 43 ... run 45 ... run 36"), repeated in both files and again
in the example JSON's comments — the `flinkProperties`-before-`up` warning
appears three times, the ENABLE_BUILT_IN_PLUGINS warning twice, the backlog
sizing rule four times with different numbers (SKILL 3: "hundreds of millions";
README: "240 + 70 + 20 s" for the suite; preflight: "180s x 1.5"; the example:
"120 + 40 + 20 s" for the tiny proof). About 10 minutes of this run went to
reading. A one-page "what you must supply, in order" at the top of SKILL.md,
with the history moved to an appendix, would halve it.

## F9. PROGRESS.txt says the tiny proof runs "two cases"; it ran three

`results/PROGRESS.txt` during the tiny proof read: "step 4 of 7: the tiny
proof: two cases end to end, and every guard broken on purpose". It measured
1, 2 and 4 cores — three cases — which is what SKILL section 3 requires ("run
every case the suite will run"). The string in `cmd_all`'s `say` table is out of
date; it should read "every case end to end". The same file also sat on that
one sentence for 12 minutes with no case number or time left, unlike the suite
step, so a watcher cannot tell a stalled tiny proof from a running one.

## F10. The broker-memory advice fires as a warning on a case that was at its cap

The tiny proof's 4-core case read 100.1% of cap, and the harness still printed
a 600-character paragraph: "kafka memory: ran out 381 times in a 30s window at
5376m ... 8448m is the size to try -- but this machine cannot give it that ...
Three ways out, in order: raise the virtual machine to about 13 GB; or drop the
4-core case ...". SKILL section 6 says the opposite is the rule now: "when the
cores are at their cap, the hits are reported and nothing is thrown out", and
"hits do not separate the two outcomes". Telling a reader to drop a case that
just measured at 100% of cap is advice the harness's own guard would not act
on. When the worker is at cap, this line should be one sentence: "the broker
hit its memory limit 381 times; the cores were at their cap, so this did not
limit the case."

## F11. The tiny proof took 12 minutes, and 2.5 of them were a fill 1.5x bigger than needed

I guessed `tinyCount` 160,000,000 ("guess high", as the README says). The
tiny proof then measured 487,826/s at 4 cores and said the *suite* needed
109,760,805. It does not say what the tiny proof itself needed, so the next
run cannot size `tinyCount` from its own figure either. Print "the tiny proof
needed N" beside the suite figure.

## F12. The "machine was busy" note counts the run's own containers

The scorecard said: "the 4-core p3-asc: the machine was busy with something
else: load reached 8.3 on 8 cores while measuring ... Close what else is
running and measure again." Nothing else was running. At 4 cores the run itself
occupies about 5.4 cores (worker 4, broker 0.43, job manager, dashboard), and
preflight's own row admits the load average "also counts this run's own
containers". The same pass read 492,775/s at 100.3% of cap, between the other
two 4-core passes (506,832 and 480,246), so the load had no visible effect.
The note should subtract what the run's own caps allow before calling the
machine busy, or say "load 8.3, of which up to 5.4 is this run".

## F13. Steps passed by hand are run again by `all`, and there is no way to say so

The skill says to prove things at the cheapest level first, so I ran `up`,
`preflight` and `completeness` by hand (13:38-13:52) until they passed. Then
`prove.py all` ran them again — preflight 149 s and completeness 722 s — about
15 minutes spent twice on an unchanged build hash. `all` could skip a step that
already passed for the same build hash, as `suite` already checks for
completeness; or the README should say "go straight to `all`; it stops at the
first failure anyway".

## F14. PROGRESS.txt is left at 86% after the chain has finished

After `results/DONE` said "PASS 70.9 min", `PROGRESS.txt` still read
"[#################...]  86%  step 7 of 7: writing the report". Anyone watching
the file rather than DONE sees a run stuck on its last step. It should end on
"finished: PASS, 70.9 min".

## F15. Confluent, summed up

Nothing in this run had to change for Confluent beyond the two lines in
SKILL question 6's table, and nothing failed because of it: the broker came up
first time, completeness passed first time, the table met the target first
time. The harness support is real, but it is **one day old** (`lib.py` comments
dated 2026-09-24 for the tool path and the `CLUSTER_ID`), and SKILL.md
describes Confluent in one table and one sentence. What a reader still has to
work out alone: that the measured broker is probably Kafka 3.7 against the Apache
default's 3.9 (F5, not verified here), and that `images.kafkaLibs` exists at all — it is in the
table in SKILL.md and the example's comment, but not in the README's field
table, which is the contract.
