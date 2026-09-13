# Six rules from letting AI build the same system 29 times

Draft of a public lessons-learned post about the 24-day autonomous build
experiment recorded in this directory. Figures come from the runs table in
[the validation README](../skill-validation/README.md) and the `suite.json` files under the run directories.
Graphic: [lessons-card.html](lessons-card.html), published at
https://claude.ai/code/artifact/dbcb5be3-f448-4792-bc26-7fb8c53b8811

---

**I let AI build the same system 29 times, working alone — 156 test runs over
24 days. It wasted half the budget: 30 of 55.9 hours produced nothing. Here is
what I would tell anyone about to do the same.**

None of these lessons are advanced. They are what an experienced engineer does
without thinking about it. The AI didn't, and never told me, because it never
noticed. Another attempt costs it nothing. Knowing when to stop is still a
manager's job. Half my spend bought nothing; the other half built a working
system in 24 days with almost none of my time. Same tool — the difference was
supervision.

**1. Stop at the first failure.** Don't blindly run a battery of tests. If one fails, stop, fix it, then move forward.

**2. Analyze before you experiment.** We wasted a lot of time with trial and error. Compare the configs before you start guessing. Every parameter of every component — data generation, Kafka, the runtime. If they're identical, the results should be too, so something you're not comparing is different. Ensure comparisons are apples to apples, including test conditions.

**3. Unit test all changes.** When you fix a defect, unit test it before a full rerun.

**4. When the shape of the data changes, check front to back.** Cardinality, record size, key format — every limit, expectation and test was set for the old shape. Re-check the whole chain, not just the part you changed.

**5. Verify initiation of tests and monitor them.** Confirm the job is actually running when launched and monitor its health, so you're not waiting for a job that died and will never complete.

**6. If a rule matters, make the tool enforce it.** When a rule is put in place, codify it so it is enforced — don't rely on AI memory to enforce it.

Rule 1 cost the most: 27 of the 30 wasted hours are runs started before the
previous problem was understood. AI runs the whole battery on code that is
already broken, and it starts with the easiest case, so it takes hours to reach
a failure the hardest case would have shown in minutes.

There was also $184 of cloud spend that bought nothing: the AI's answer to a
hardware limit was to rent a cluster, which ran the job slower than the laptop
had.

---

## The figures, and where they come from

| | | source |
|---|---|---|
| span | 2026-08-19 - 2026-09-12, 24 days | session transcripts |
| timed test runs | 156 | `suite.json` `runs[]` across the recorded builds - runs 1-10 kept none, so this is a floor |
| full builds with a complete record | 29 | [the validation README](../skill-validation/README.md) runs table |
| machine time | 55.9 h | sum of the `wall` column, same table |
| useful / wasted | 25.9 h / 30.0 h | itemisation below |
| cloud spend, wasted | $184 | AWS bill for the rented cluster ([step 11](../steps/step-11/aws.md)); abandoned, and the demo rebuilt on the laptop in [step 12](../steps/step-12/scaling-demo.md) |
| metered-equivalent API value | ~$2,400 | $1,994 orchestration transcripts + $400 recorded agent cost on runs 1-8; subscription, so not out of pocket |
| model turns | 6,542 | session transcripts |

### The 30 wasted hours

| cause | h | lesson |
|---|---:|---|
| runs that measured the wrong thing | 16.9 | 2 - apples to apples |
| experiments run before the configurations were compared | 5.0 | 3 - look at the facts first |
| a new guard put in use untested, refusing valid work | 4.0 | 4 - test the rule first |
| waiting on jobs that had already died | 2.8 | 5 - verify the launch |
| a whole run lost to that same untested rule | 1.3 | 4 - test the rule first |

27.2 of those 30.0 hours - everything but the idle waiting - are runs started
before the previous problem was understood, which is lesson 1.

### The cluster detour

| | laptop | AWS `c5.2xlarge` + 3-broker MSK |
|---|---:|---:|
| orders/sec | **142,340** | 83,031 |
| bottleneck | broker at ~95% of capacity | client CPU, 92% user |

Source: [step 11](../steps/step-11/aws.md). Step 12 discarded the cluster and
rebuilt the demo on one laptop, where it showed 2.15x and 1.96x on successive
doublings - the result the cluster was rented to produce.
