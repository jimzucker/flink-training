# When a test fails, a good engineer stops and fixes the problem

Draft of a public lessons-learned post about the 24-day autonomous build
experiment recorded in this directory. Figures come from the runs table in
[README.md](README.md) and the `suite.json` files under the run directories.
Graphic: [lessons-card.html](lessons-card.html), published at
https://claude.ai/code/artifact/dbcb5be3-f448-4792-bc26-7fb8c53b8811

---

**When a test fails, a good engineer stops and fixes the problem. AI runs the
rest of the tests anyway — on code that is already broken — wasting time and
money. That's where half my budget went.**

I had Claude build the same system from scratch, over and over, working alone.
One instruction each time: build the software, run it, prove the results are
right. 156 test runs. 24 days. 55.9 hours of computer time. $184 of real cloud
spend.

Half of that work I threw away.

Not because the job was too hard for it. Claude built the system, and the
system worked. What Claude could not do was recognize when it was wasting time.

**27 of the 30 wasted hours came from one habit.** Claude ran the full battery
of tests every time, even when the first test had already failed. And it always
started with the easiest case, so it took hours to reach the failure that the
hardest case would have shown in minutes. It's what an inexperienced engineer
does until someone shows them otherwise. Claude kept doing it for all 24 days,
and never realized anything was wrong — so it never told me.

Here is where the 30 hours went:

**16.9 h — runs that measured the wrong thing.** Nine builds in a row measured
half the job, then got compared against the full one. Days went into explaining
a gap that was really just a mismatched test.

**5.0 h — guessing instead of checking the facts.** Seven experiments hunting a
cause. Then I put every setting side by side, took five minutes, and found it.
A thing that never changed couldn't have been what changed.

**4.0 h — a new quality check that rejected good work.** Added without testing
it against results already on file. It threw out work that was fine.

**2.8 h — waiting on work that had already stopped.** Two jobs died within a
second of starting. I found out hours later, because "it's running" was
reported, not checked.

**1.3 h — a whole build lost to that same untested rule.**

None of this is exotic. It's the testing discipline an experienced engineer
already has: stop at the first failure, compare like with like, look before you
guess, probe the limit first, check the job is actually running.

What struck me is that the cheapness is the trap. A person running a two-hour
test feels the two hours. Claude doesn't. It will happily run the battery
again, and again, because nothing in the loop pushes back. The judgment I was
paying for wasn't in the code it wrote — it was in deciding which of its runs
were worth starting.

Half my spend bought nothing. The other half built a working system in 24 days
with almost none of my time. Same tool. The difference was supervision —
because another attempt costs AI nothing, and knowing when to stop is still a
manager's job.

---

## The figures, and where they come from

| | | source |
|---|---|---|
| span | 2026-08-19 → 2026-09-12, 24 days | session transcripts |
| timed test runs | 156 | `suite.json` `runs[]` across the recorded builds — runs 1–10 kept none, so this is a floor |
| full builds with a complete record | 29 | [README.md](README.md) runs table |
| machine time | 55.9 h | sum of the `wall` column, same table |
| useful / wasted | 25.9 h / 30.0 h | itemisation below |
| real cash | **$184** | AWS bill |
| metered-equivalent API value | ~$2,400 | $1,994 orchestration transcripts + $400 recorded agent cost on runs 1–8; subscription, so not out of pocket |
| model turns | 6,542 | session transcripts |

### The 30 wasted hours

| cause | h | lesson |
|---|---:|---|
| runs that measured the wrong thing | 16.9 | apples to apples |
| experiments run before the configurations were compared | 5.0 | analyse before you experiment |
| a new guard put in use untested, refusing valid work | 4.0 | replay a rule against the record |
| waiting on jobs that had already died | 2.8 | verify the launch |
| a whole run lost to that same untested rule | 1.3 | replay a rule against the record |

27.2 of those 30.0 hours — everything but the idle waiting — are runs started
before the previous problem was understood.

## The five lessons

1. **Stop at the first failure.** Don't run a battery of tests and sort it out
   at the end.
2. **Compare apples to apples.** Same inputs, same job, same instrument. And
   when the shape of the data changes, check the code and tests front to back.
3. **Analyze before you experiment.** Put every setting side by side first — a
   constant can't be the cause. Don't explain a surprising number until you've
   tested the explanation, and know the machine's ceiling before you blame your
   code.
4. **One measurement is a guess.** Measure twice. Test a fix the cheapest way
   it can break, and check a new rule against old results before you let it
   block anything.
5. **If a rule matters, make the tool enforce it.** Write down what changed,
   confirm the job is actually running, and report the answer — not the
   journey.
