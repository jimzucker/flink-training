# LinkedIn writing

Public posts about this project, their graphics, and the rules for writing
them. This file replaces what had been kept only in Claude Code's working
memory.

## Pieces

| piece | audience | files | state |
|---|---|---|---|
| Lessons learned from the 24-day AI build | senior managers | `lessons-post.md`, `lessons-article.html`, `lessons-card.html` | merged (#91, #92, #93) |
| Introducing the prove-it-scales skill | post: senior managers · article: engineers who will use it | `skill-post.md`, `skill-article.md`, `skill-post-card.html`, `skill-article-card.html` | branch `linkedin-skill-post`, no PR |

## Tone rules

Each was a correction from the author during revisions of the lessons post.

- **Write for senior managers who do not know this work.** No cores, passes,
  harness or "measurement runs" in a manager-facing post.
- **Lead with the point.** The rules were the point of the lessons post and
  were moved to the top.
- **Attribute mistakes to the AI, not the author.**
- **No floating pronouns** — name the subject; never a chain of "it".
- **No cleverness.** "Then the next." and "so those failed too" were cut as
  writerly. Say plainly what it cost: "wasting time and money".
- The inexperienced-engineer analogy stays, but **"in their first month" is an
  exaggeration** — say "an inexperienced engineer".
- **Keep methodology caveats out of the post.** They swallow the point; the
  figures and their sources go in an appendix or the repo.
- **The author's own text wins.** When the author pastes a rewrite, it is used
  verbatim. Approved wording is never reworded — it is copied from this repo.
- **Every figure is read back from the record before it is published.**

## The six rules

**The canonical wording is the published text in
[`lessons-post.md`](lessons-post.md). Copy it; do not reword it.**

How it got there, so the history is not lost:

1. **2026-09-11** — the author asked for key lessons "like 1. Don't blindly run
   a battery of test if one fails stop fix it them move forward 2. When changing
   the structure of the data check the code and tests front to back to ensure
   all effected areas covered", then "Remove the additional comments". That
   produced thirteen:

   1. Don't blindly run a battery of tests. If one fails, stop, fix it, then move forward.
   2. When you change the structure of the data, check the code and tests front to back.
   3. Don't explain a surprising number until you've tested the explanation.
   4. Before you start experimenting, put every setting side by side.
   5. Make sure comparisons are apples to apples — same inputs, same test conditions, same instrument.
   6. Write down what you changed, or you can't compare results later.
   7. Test a fix the cheapest way it can break first.
   8. Check a new rule against old results before you let it block anything.
   9. One measurement is a guess. Measure twice before you believe it.
   10. Know the machine's ceiling before you blame your code.
   11. If a rule matters, make the tool enforce it.
   12. Confirm the job is actually running before you say it started.
   13. Report the answer, not the journey.

2. **2026-09-12** — condensed to five in answer to "5 or 10". That condensation
   dropped the author's phrasing ("blindly", "fix it, then move forward") and
   was the cause of a long revision cycle the next day. **Superseded.**
3. **2026-09-13** — the author wrote seven, then cut "Compare apples to apples"
   (folded into rule 2) and "Consistency and reproducibility", and added a rule
   for input-shape changes. Six.
4. **2026-09-13, PR #92** — the author rewrote the six into the published
   wording.

The author's words each rule traces to:

| theme | the author's words |
|---|---|
| stop at the first failure | "We have to stop blindly running tests of one test failes stop and fix it before contibuibg" (2026-08-29) |
| fail fast | "using brute force testing instead of failing fast by testing the upper boundary first and backing off" (2026-09-11) |
| apples to apples | "How about ensured comparisons are apples to apples including test conditions" |
| input shape | "When changing the structure of the data check the code and tests front to back to ensure all effected areas covered" |
| diff configs | "When I say configs I mean all parameters of each component, data gen, Kafka config, flink runtime config"; "If they are identical r then results should be so ur missing something"; "This should be a lesson learned we wasted alot of days with trial and error vs a simple analysis" |
| unit test first | "Good pattern when we fix a defect unit test it before a full rerun remember that!" |
| guards | "shouldnt we have guards on all rules so we dont waste time breaking them, this is wasting my time adn $$" (2026-09-01) |

## Decisions

- **The $184 is out of the lessons post.** It was the AWS bill for step 11's
  rented cluster, which ran the job slower than the laptop (83,031 against
  142,340 orders/s) and was abandoned in step 12 — not the project's cost. The
  author called it a side note and had it removed from post, page and card on
  2026-09-14 (#93). Do not reintroduce it.
- **The skill pieces scope the skill as Flink on Kafka today.** The interview and
  measurement rules are general; the harness is not. Agreed 2026-09-14.
- **Speed is "two to three times", not "about three".** At four cores the
  Claude-built runs read 506,286 and 668,254 records/s against the demo's
  238,804 — 2.1× and 2.8×.

## Open on the skill pieces

- Runs 23 and 24 are described as measured at 4,096 keys. Their `suite.json`
  files do not record cardinality (`workload` is null); the figure comes only
  from [`demo-under-harness.md`](../skill-validation/demo-under-harness.md).
- The post opens "Last week I shared six rules…" — true only if the lessons post
  is published first.
- The post says "The first thing it did was report a result I didn't want"; the
  demo comparison actually came late in the validation work.
