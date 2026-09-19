# Post: scalable-flink-skill

Draft, 400-odd words. The short pitch for the skill: what it does for you,
forward-looking, no build history. **The link goes in the first comment, not
the body** — LinkedIn suppresses reach on posts carrying an outbound link, and
a bare URL in the body gets auto-linked even inside a code block, so the
install commands are left to the README rather than pasted here. Figures come from
[`docs/skill-validation/demo-under-harness.md`](../skill-validation/demo-under-harness.md)
and [`docs/skill-validation/payload-2k.md`](../skill-validation/payload-2k.md).

---

Ask Claude to build a data pipeline. An hour later you have the pipeline — and a scaling table you can put in front of a skeptic.

**scalable-flink-skill** is a free Claude Code skill. It interviews you first: what goes in, what comes out, how many distinct keys, what has to be exactly right, and the claim you want to be able to make. It builds it in reviewable steps. Then it verifies the results are correct and complete, and measures and compares scaling.

What lands on your desk looks like this:

| capacity | throughput | step |
|---|---:|---|
| 1 core | 58,326 records/s | |
| 2 cores | 120,115 records/s | **2.06×** |
| 4 cores | 238,804 records/s | **1.99×** |

Near-linear, with the range around each step, and the resource columns beside the throughput so anyone can see the machine was the limit and not something else. Six times the message size, and the same pipeline still returns 4.57× across the same range.

That table is the point. Capacity decisions — how many servers, what a platform costs at twice the load — usually rest on one run and a screenshot. This gives you a number that holds up when someone pushes on it, and the full record underneath it when they do.

**What you need:** Docker, Python 3, JDK 17. It targets Apache Flink on Kafka, runs on one laptop, and takes about an hour end to end. Installing it is a clone and a copy.

Then ask Claude to build your pipeline and prove it scales.

It's free and open, and every validation run behind that table is public. Link in the first comment.

---

**First comment:** "What it does, how to install it, and what a result looks like: https://github.com/jimzucker/scalable-flink-skill"


---

## The figures, and where they come from

| figure | source |
|---|---|
| 58,326 / 120,115 / 238,804 records/s | `demo-under-harness.md`, the 4,096-key arm |
| 2.06× and 1.99× | same arm's measured ratios, 2.059 [1.915, 2.179] and 1.988 [1.945, 2.032] |
| every case 96.3–98.1% of its CPU cap, GC 3.8 / 0.8 / 0.3% | same arm — the basis for "the machine was the limit" |
| 4.57× across 1→4 at six times the message size | `payload-2k.md`, the 2 KB arm's tiny proof of 2026-09-15 |
| about an hour end to end | `docs/skill-validation/rig-2026-09-04.md`: the full chain ran 66.2 min, first attempt |
| Docker, Python 3, JDK 17; Flink on Kafka | the skill's own requirements |

Note for editing: the 2.06× / 1.99× table is a 1→2→4 suite with three passes
per case; the 4.57× is a two-case tiny proof, a lighter instrument. Both are
stated as measured, and neither is presented as the other.
