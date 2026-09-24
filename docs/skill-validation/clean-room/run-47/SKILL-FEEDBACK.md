# Where the skill and the harness were wrong, unclear, out of date, or cost me time

Clean-room run in `~/code/GitHub/flink-skill-test-47`, 2026-09-23.
Every finding quotes the exact wording and says what it should say instead.
Ordered by what it cost.

---

## 1. `docs/interview-wording.md` says it is verbatim from `SKILL.md` §1. It is not, and it contradicts it.

**Where:** `docs/interview-wording.md`, the heading "## Current wording".

**What it says:**

> ## Current wording
>
> Verbatim from `SKILL.md` §1.
>
> 1. **What is the input event, and what comes out?** One sentence, in domain
>    language. This is the spec.
> 2. **Does one input become several outputs?** The fan-out ratio decides where
>    the load lands…

**What `SKILL.md` §1 actually says:**

> 1. **What goes into the pipeline, and what comes out?** A few sentences, in the
>    user's own words. This becomes the spec

and it has **six** questions, not the eight this file lists. Question 5 in the
live copy is "Which Flink API should we use?"; in this file question 5 is "Who
watches the demo, and what must they believe at the end?". None of the six
offered defaults that this run was told to take appear in this file at all.

**Why it matters:** my task was "take the default for every question". A file
that announces itself as the current wording, and is not, is the single most
dangerous kind of stale document in a skill whose whole first section is an
interview. An agent that opens it first answers a different interview.

**What it should say:** replace the heading with

> ## Wording as it stood on 2026-09-20, before the revision below
>
> **This is not the live copy.** `SKILL.md` §1 is, and it has since been cut
> from eight questions to six. Read `SKILL.md`. This file is kept only so the
> change can be diffed.

---

## 2. The harness's own plain-English guard does not cover the word its most-read message uses

**Where:** `harness/doccheck.py`, `check_plain_english`, and `harness/prove.py`.

**What `SKILL.md` §6 says:**

> | never write | write |
> |---|---|
> | refused, a refusal | stopped the run, or threw the case out — then what it found |
> | FAIL at *step* | STOPPED at *step*, and why in the same line |
>
> …`harness/doccheck.py` enforces the table above against every message string,
> so this rule cannot quietly come undone.

**What the guard actually bans:**

> ```python
> banned = {"refus": "say stopped the run, or threw the case out",
>           "fail at ": "say STOPPED at <step>, and why in the same line",
>           "failed at ": "say stopped at <step>, and why in the same line",
>           "ran out of space": "say there is not enough disk space"}
> ```

`"fail at "` and `"failed at "` both carry a trailing space, so they match only
a phrase the harness never writes. The bare status word is not banned, and the
harness uses it in the line a person reads first:

> `prove.py:2715:        print(f"FAILED ({e.scope}): {e.msg}")`

and in twelve more places, including

> `prove.py:2131:        print("COMPLETENESS FAILED:", e.msg)`
> `prove.py:1987:    print("TINY PROOF " + ("PASSED" if rc == 0 else "FAILED"))`
> `prove.py:2089:            raise Refusal("rig", f"COMPLETENESS FAILED ({label}): verifier exit {r.returncode}")`

`doccheck` passes clean and prints *"every message a person reads is plain
English, and the skill says so"*, which is the claim the check cannot support.

**Why it matters:** the skill's own argument for the guard is that "a written
rule on its own does not hold" — and here the written rule has not held
*because the guard was written narrower than the rule*. This is the failure
mode the promotion rule exists to prevent, happening inside the promotion rule.

**What it should say / do:** ban the token, not the phrase. Something like

```python
banned = {"refus": …,
          r"\bfail(ed|s)?\b": "say STOPPED, and why in the same line",
          "ran out of space": …}
```

with an explicit allow-list for the engine's own job states (`"FAILED"`,
`"CANCELED"` come back from Flink's REST API and are quoted, not written), and
change `prove.py`'s top-level handler to `STOPPED ({e.scope}): {e.msg}`.

---

## 3. `pipeline.example.json` — the example for a pipeline shaped like mine — ships caps that do not fit the machine the skill is written for, and does not say so. The windowed example does.

**Where:** `harness/pipeline.example.json`, `"_caps"`.

**What it says:**

> Every configuration in record/configs.json that produced a usable table left
> 4.25-5.00 GB; these values leave 5.00 GB and are the pair behind the best
> recorded result (2->4 = 2.114x).

Its values are `tmMemoryBase 1088m`, `tmMemoryPerCore 640m`, `kafkaMemory 6g`.
At the four-core case that is 1088 + 4×640 = **3,648** of worker, **6,144** of
broker and the **1,600** the harness gives the job manager: **11,392 MiB**,
against the 9,937 MiB Docker VM this machine has. Over by 1,455 MiB, before
section 7's dashboard.

**The other example says exactly this, in capitals:**

> `pipeline.example.windowed.json`: "THESE CAPS NEED A DOCKER VM OF ABOUT 12 GB,
> and they are the first thing to check against your own machine. At the largest
> case here the limits add up to 3,648 MiB of worker … = 11,392 MiB … On the
> 9,937 MiB VM clean-room run 44 measured on, that does not fit."

**Why it matters:** `SKILL.md` tells the reader to "Read whichever is the shape
of yours". Mine has a constant fan-out, so I was routed to the file **without**
the warning, and had to derive the memory split myself from §6a's run-45
anecdote and the preflight source. `doccheck` has a check named
`check_windowed_example` that asserts the warning is present — in one example
only.

**What it should say:** put the same paragraph in `pipeline.example.json`, and
extend the `doccheck` check to both files.

---

## 4. `harness/README.md`'s command list refers to a variable it never defines

**Where:** `harness/README.md`, "Type the steps yourself only when one of them
needs re-running".

**What it says:**

> ```
> python3 $H replay          # thresholds vs the recorded runs — seconds, no stack
> …
> sh $HERE/watch.sh          # watch a detached run without being swept up in its teardown
> ```

`$H` is defined earlier in the file as the path to `prove.py`. `$HERE` is
defined nowhere. A reader who pastes the block gets `sh /watch.sh`.

**What it should say:** `sh $(dirname $H)/watch.sh`, or spell the path out the
way the block above it does.

---

## 5. "Set `retention.bytes` on them yourself at creation" is advice you cannot act on

**Where:** `harness/README.md`, the `topicsAlsoWritten` row.

**What it says:**

> Set `retention.bytes` on them yourself at creation; the harness only applies
> its own retention when it recreates them

**What actually happens:** the harness creates those topics itself, in
`recreate_output_topics(include_declared=True)`, which the completeness step
calls before either arm — and in `prove.py all`, completeness runs before
anything else has written to them. There is no "creation" for the run to get
in front of unless it goes behind the harness's back and creates the topics
before `prove.py` starts. My run left them to the harness and the retention was
applied correctly.

**What it should say:** "The harness creates these with its own retention the
first time it clears them, which is at the start of the completeness run. Set
`retention.bytes` yourself only on topics the harness never touches — a second
input, for instance."

---

## 6. Small things

- `harness/dashboard/` ships a `__pycache__` directory beside
  `docker_cpu_exporter.py`, so copying "the directory" as section 7 instructs
  copies compiled bytecode from the skill author's Python into a clean room.
  `.gitignore` it.
- `harness/prove.py`'s module docstring says "run from the directory holding
  pipeline.json"; the README's own launch line does `cd` into that directory
  implicitly and never says so. Worth one line in the README's code block.

---

## 7. The tiny proof knew the suite could not report a step, and let the chain spend 67 minutes finding out

**This one cost the most: about an hour of the run's two hours.**

The tiny proof classified the one-core case as a ceiling and said so plainly:

> `CEILING at 99,201 rec/s, tm 98.7% of cap: Kafka ran out of memory 2,034 times
> during the window and had to read the backlog back off disk (128,960 page
> refaults). Kafka was the bottleneck here, not the cores. — raise
> caps.kafkaMemory from 4608m to about 7168m`
>
> `keeping it: a case that is not the constraint is reported and left out of the
> steps, not a reason to stop.`

It then reported **one** step where `pipeline.json` asks for two:

```
steps [{'step': '2->4', 'ratio': 1.763, ...}]
```

`SKILL.md` §3 says the tiny proof exists so that *"every step the suite will
report gets bounded now, including the middle one — clean-room run 31 measured
only its smallest and largest cases here, so a 1→2 step that was
arithmetically impossible went unseen until the report, 45 minutes later"*.
The tiny proof did the measuring. What it did not do was **say that a step it
could not bound is a step the suite will not be able to report either**, and
`prove.py all` went straight on to a 4-minute fill and a 67-minute suite that
came back with exactly that hole in it.

**What it should do:** at the end of `tinyproof`, when a case the suite will
run came back as a ceiling, print

> `the 1-core case came back as a ceiling, so the suite will not be able to
> report the 1->2 step either. Fix the ceiling or drop the case from "cases"
> before spending the suite — this is the only cheap chance to.`

and, in `all`, stop there rather than filling. Run 31's lesson was "bound every
step here"; the lesson this run adds is "and act on it here too".

## 8. `harness/README.md` gives the waiting advice that gets a shell killed, then two paragraphs later says not to

**Where:** `harness/README.md`, the launch block, then the section "Watching a
run from an agent".

**What the launch block says:**

> Wait with `until [ -f results/DONE ]; do sleep 30; done` and nothing more.

**What the next section says:**

> a tool that runs `bash -c "cd /path/to/project && until ...; do sleep 30;
> done"` puts the path on the command line *because* it had to `cd`, so "a
> shell whose working directory is the project" is unreachable by that route —
> clean-room run 36 lost two waiting shells to exit 144 that way

An agent reads the launch block first, because it is the launch block. The
one-liner it recommends is the exact form the later section says cannot work
for an agent. I wrote the scratch-script form from the later section and lost
no shells, but I had to read past the advice to find it.

**What it should say:** put the working form in the launch block —

> ```
> printf 'PROJECT_DIR=/path/to/run\nsh /path/to/skill/harness/watch.sh\n' > /tmp/scratch/w.sh
> sh /tmp/scratch/w.sh
> ```
>
> Do **not** write `cd <project> && until [ -f results/DONE ]…`: the teardown
> kills any host process whose command line names the project directory, and
> that one does.

## 9. The completeness pass line states the kill fraction it asked for, not the one it got — and the skill already knows

**Where:** `prove.py`, the line the completeness step ends on.

**What it printed for this run:**

> `COMPLETENESS PASSED FOR BUILD 3f6c398464479ec2 (a clean run, and one killed
> and restarted at 35%)`

**What the log said two lines earlier:**

> `KILL: committed=3407817, killing the pipeline mid-run`

3,407,817 of 8,000,000 is **42.6%**, not 35%. The harness has the right number
— it stores it as `killedAtCommitted` in `completeness.json` — and prints the
wrong one in the sentence a person reads.

This is a **known** defect: `pipeline.example.json` says so itself.

> `"_killAtFraction": "Approximate by construction: offsets commit once per
> checkpoint interval, so the kill lands at the first commit past this
> fraction. Clean-room run 30 asked for 0.35 and it landed at 62.7%, while the
> log still said 35%."`

Run 30 reported it; three sections of prose now describe the approximation;
the line still says 35%.

**What it should say:** `(a clean run, and one killed and restarted at 42.6% —
asked for 35%, and the kill lands on the first commit past it)`.

## 10. The scorecard's "blocked by" column said *Pipeline CPU* on a table the harness itself blamed on the host

**Where:** `results/suite.txt`, the scorecard.

**What it printed:**

```
  cores        speed   scaling     pipeline CPU   pipeline memory    Kafka CPU        Kafka memory   blocked by      what to do
    1 *     97,863/s         —         1 / 100%      1664m / 1.7%     2.5 / 7%   4.5g, full 4,996x   Pipeline CPU    check it matches
    2 *    168,300/s         —         2 / 100%      2304m / 1.1%    2.5 / 25%   4.5g, full 5,050x   Pipeline CPU    no usable step
    4 *    242,923/s         —          4 / 98%      3584m / 0.4%    2.5 / 41%    4.5g, never full   Pipeline CPU    no usable step
```

and then, four lines below the same table:

> `the 4-core p1-asc: the machine was busy with something else: load reached
> 27.4 on 8 cores while measuring. A CPU cap is a share, not a promise of
> cycles — every case can read 100% of its cap and still do less work.`

Both are true and they point in opposite directions. `SKILL.md` §6 says *"The
verdict names a column, so a reader can check it against the numbers on the
same row"* — and here the verdict names the one column that cannot be checked,
because a case pinned at its cap on a host at load 27.4 is not blocked by its
own CPU in any sense a reader would mean.

The harness already records the load average at the open and close of every
window, and already prints the sentence. It is one step from putting it in the
cell.

**What it should say:** where a case's window opened or closed at a host load
materially above the cores it was promised, the `blocked by` cell should read
**`Host was busy`** and `what to do` **`measure again quiet`**, with the load
figure in the sentence underneath — ahead of *Pipeline CPU*, since it
invalidates it. Note also that two rows say `Kafka memory ... full 4,996x` and
`full 5,050x` while `blocked by` still says *Pipeline CPU*; the one-core row
was thrown out **by the broker-memory guard**, so *Kafka memory* is the column
that describes it.

## 11. Two things the skill and harness got exactly right, and should not lose

Feedback is more useful with the baseline in it.

- **The design diff is the best thing in the skill.** It read my declared
  operators and topics back off the running plan and the broker and printed
  eleven rows, including `market-values-by-symbol yes 44 records after the
  drain` and `market-values-by-account yes 176 records after the drain`. Two
  previous clean-room runs built one market value where the interview asks for
  two; this check makes that impossible to miss, and it cost nothing.
- **The report told me what to do next, and it was right.** After throwing the
  whole table out it printed: *"a guard that threw out a case whose worker was
  at its cap is worth doubting before the pipeline is. Run `prove.py ceiling`
  … and `prove.py probe` … Both are minutes, and neither changes the
  pipeline."* I ran both. The probe showed this machine returns **1.57x on
  2→4** for memory-heavy work with no pipeline in the way, against a 1.90x
  floor, and the ceiling run showed the broker using 0.99 of its 2.5 cores
  while the pipeline held 99.8% of its four. Those two measurements, twenty
  minutes together, are the only reason this run has an honest answer instead
  of a story.
