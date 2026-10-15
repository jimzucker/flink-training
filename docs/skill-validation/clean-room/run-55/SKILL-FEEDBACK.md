# Skill feedback — clean-room run 55

Skill commit `e0e13f7` at `~/.claude/skills/scalable-flink-skill`. Each entry
quotes the wording that was wrong, unclear or cost time, and says what it
should say instead. Numbered in the order found.

## F1. `keySets` stage names are matched to topic names by substring, and nothing says so

`harness/README.md`, `keySets` row: *"which manifest fields hold a keyed stage's
key set ... Also, as `{"stage name": "manifest field"}`."* It does not say what
a "stage name" must be. In `lib.py` `graph_context` the stage name is matched
against **output topic names** (`s in t or t in s`, dashes and underscores
equal) to put a key count on each output in `graph.md`, and `graph_tasks`
matches it against **operator names in the running plan** to label the keyBy
edge. My first choice, the operator names (`position-mv-by-symbol`), would
have matched the vertices but no topic, so the outputs would have been drawn
with no key count and nothing would have said why. I found this only by
reading `lib.py`. I renamed the stages to the topic names
(`positions-by-symbol`), which happen to be substrings of my sink operator names.

**Should say:** "The stage name labels the job graph: it must appear in the
name of the keyed operator (or an operator chained with it) and in the name of
the output topic it feeds, or that edge and that output are drawn without a key
count." (Correction after reading the rendered `graph.png`: the two
market-value topics did get their key counts, 4,096 and 16,384, apparently
carried from the keyed edge into the Writer's task rather than from the topic
name. So the harness copes better than the field description suggests; the
description is still silent on what a stage name has to match.)

## F2. The `keySets` error message lists only dictionary fields

`prove.py` `keys_per_subtask`: when the field is missing it says *"the manifest
has no field … it has [fields whose value is a dict]"*. A list of keys is
accepted (`sorted(m[field])` works on a list), and a list is the natural shape
for "the key set", but the message would not offer list fields as candidates.
**Should say:** list both dictionary and list fields.

## F3. The README's own launch line puts the project path where the reaper looks

`harness/README.md`: *"`nohup python3 $H all > results/all.log 2>&1 &`"* —
relative paths, so it only works from inside the project, and the same file
then warns that *"the chain stops any process that names the run directory on
its command line or sits in it"*. An agent's tool has to `cd` there, which puts
the absolute path on the command line. I wrote a two-line script
(`scratch/step.sh`) that `cd`s to its own directory and ran it by a relative
path from the parent, as SKILL.md §5 shows for `watch.sh`. **Should say:** give
the same "script run by a relative path from the parent" form for launching
`all` and the single steps, not only for watching.

## F4. Where the generator must put a second input is left to inference

SKILL.md §1 q2 says *"A price feed is a topic you create, fill and read, named
in neither list"*, and the example's `_topics` says *"the generator is the
natural place: fill it once at build time"*. But the harness runs the generator
three times (tiny proof, completeness, fill) with three seeds, and deletes and
recreates only `topics.in`. If prices depended on the seed, the price topic
would accumulate three different price sets and "latest price" would depend on
which fill ran last. Nothing warns about this. I made the price feed use a fixed
seed and write only when the topic is empty. **Should say:** "a second input
written by the generator must not depend on `{seed}` or `{count}`, and must be
written once (the harness calls the generator for every backlog it fills)."

## F5. Nothing in the skill or the harness notices the Mac going to sleep

The chain was paused from 20:01 to 20:06 by **system sleep**: the laptop was on
battery with `sleep 1` (one minute idle) and nobody at the keyboard
(`pmset -g log`: *"Entering Sleep state due to 'Maintenance Sleep' … Using
Batt"*). It landed between the completeness drain and its verifier, so nothing
was harmed, but a sleep inside a measured window would have stopped every clock
and the case would have read as slow. Preflight checks other programs' CPU, swap,
disk and the VM's memory — not power. My `WATCH_MINUTES=9` watcher, started at 19:53:20,
had still not exited at 20:07 and so overran my tool's 10-minute limit; that
fits the five minutes asleep, but why is not measured. I held the
machine awake with `caffeinate -i -m -w <pid of prove.py all>` and read the
assertion back with `pmset -g assertions`.

**Should say**, as a preflight row and in §5 "Hold the machine still too":
"The machine must not sleep and should be on mains power. `prove.py all`
holds an idle-sleep assertion for its own life (`caffeinate -i -w <pid>`), and
preflight reports the power source and battery charge; a case whose window
spans a wake is thrown out." Running a 2–3 hour chain on battery also risks the
battery running out mid-suite; this one started at 85%.

## F6. The tiny proof passes with its largest case as a ceiling, and the advice that follows contradicts the outcome

The tiny proof kept the 4-core case as a ceiling (93.3% of cap, 2,954 broker
memory-limit hits in a 30 s window), wrote *"2->4: not bounded — 4 cores was a
ceiling"*, and passed. That is what SKILL.md §3 says (*"a case that is not the
constraint is a ceiling, not a failure"*). But the broker advice printed next to
it says *"8960m is the size to try -- but this machine cannot give it that …
Three ways out, in order: raise the virtual machine to about 14 GB; or drop the
4-core case …; or keep the broker where it is"* — and with nobody to ask there is
no rule for choosing. The suite's first 4-core pass then read 455,338/s at
99.4% of cap with no ceiling, so the tiny proof's 30 s window was the wrong
place to judge broker memory. **Should say:** "With nobody to ask, take the
third: carry on. The tiny proof's 30 s window over a fresh backlog is not the
suite's; let the suite decide whether the case is a ceiling." And the tiny
proof should say plainly that the step it could not bound will be measured for
the first time in the suite.

## F7. A Monitor or `tail -F` of `results/harness.log` is killed by the chain's start sweep

I watched the first, hand-run completeness with a background `tail -F
<project>/results/harness.log`. It was killed (exit 144) the moment `prove.py
all` started, by the sweep at chain start. The skill does warn about this
(§5 and harness/README "Watching a run"), and it was my mistake — but the
warning talks about `tinyproof` and `down`: *"During `tinyproof` and `down` the
reaper kills any host process whose command line names the project
directory"*. **Should say:** "at the start of `all`, during `tinyproof`, and at
`down`".

## F8. The rule that ties an output topic to its sink is in SKILL.md §4 only, and cost a completeness run

SKILL.md §4: *"An output is tied to the Writer whose name holds the topic's
name, never to a Committer."* I named my sinks `sink-mv-by-symbol`; the first
completeness drew both market-value topics with no line (*"drawn without a line,
no operator names them"*) and I renamed them and ran completeness again (about
10 minutes). The `design` row of `harness/README.md` — the contract I was told
to write `pipeline.json` from — says only that a declared operator is *"found if
its text appears … anywhere in the running plan's vertex names"*. **Should
say**, in that row and in preflight: "name each sink so its operator name
contains the topic it writes (`sink-market-values-by-symbol`), or the job graph
cannot connect them" — and preflight can check it before anything runs, since
both lists are in `pipeline.json`.

## F9. The tiny proof judged 1→2 at 1.778× as passing, then the suite spent 25 minutes failing to settle it

The tiny proof's bound for a doubling is 1.5×–2.5× (*"bound the ratio to
0.75×–1.25× of the ideal"*, §3); its 1→2 read 1.778×, already under the 1.80×
target, and it passed silently. The suite then read 1.830× with a range of
1.763×–1.847×, ran all six extra cases (about 25 minutes) and still could not
settle it. **Should say:** the tiny proof should print "1→2 read 1.778×, under
the 1.80× target; expect the suite to stop on it" — not stop, since it is one
short pass, but a reader should learn it 70 minutes earlier.

## F10. Where the skill was right, on the four things this run was asked about

- **The pipeline's README.** §2's section table was enough to write it, and
  preflight's check (*"README.md names all 16 topics and operators"*) held me to
  it. It checks names are present, not that they are right: a README that names
  a topic with the wrong role would pass. Good enough; say so in the row.
- **Evidence in git.** `results/.gitignore` written by the harness decided what
  to keep, and *"evidence: not committed yet: … Commit them with the build hash"*
  after each command was the reminder I needed. No absolute user paths appeared
  in the kept files I checked (`suite.json`, `ceiling.json`, `hostprobe.json`).
  `harness.log` is not kept, so the per-case lines with the busy-machine waits
  exist only on this disk; `suite.json` holds the numbers, but not the waits'
  sentences.
- **The job diagram.** `graph.png` was right first time once the sink names
  matched (F8): 4 tasks, 14 operators, both broadcasts, keyBy edges with their
  key counts, partitions on the input, intervals on the throttled outputs.
- **Waiting for the chain.** `watch.sh` with `WATCH_MINUTES` in a script with the
  path inside it, run by a relative path from the parent, worked every time
  (exit 3 then rerun, exit 1 at the end). SKILL.md's *"a time limit shorter than
  your tool's"* with an example of 9 against a 10-minute tool limit is too tight:
  one 9-minute wait overran 10 minutes (F5). **Should say:** leave at least two
  minutes (`WATCH_MINUTES=8` for a 10-minute tool).
