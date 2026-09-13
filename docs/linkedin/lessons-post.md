# Six rules from letting AI build the same system 29 times

Draft of a public lessons-learned post about the 24-day autonomous build
experiment recorded in this directory. Figures come from the runs table in
[the validation README](../skill-validation/README.md) and the `suite.json` files under the run directories.
Graphic: [lessons-card.html](lessons-card.html), published at
https://claude.ai/code/artifact/dbcb5be3-f448-4792-bc26-7fb8c53b8811

---

I let Claude build the same system 29 times over 24 days.

156 test runs. 55.9 hours of machine time. And 30 of those hours produced work I had to throw away.

My takeaway: autonomous execution still needs experienced engineering supervision. The AI could build the system, but I had to recognize when it was repeating mistakes, testing the wrong thing or spending time without making progress.

None of these lessons are advanced. They're habits experienced engineers develop from seeing what goes wrong. In this experiment, the AI repeatedly missed those signals. I had to catch them and turn the lessons into controls.

Experienced supervision made the difference in how much useful work came out of each run.

Deciding whether another run is worth doing—and building that judgment into the workflow—is part of the job.

The supervision it needed came down to six habits an experienced engineer brings to the work:

**1. Stop at the first failure.**
Fix and verify the problem before running the rest of the test suite. Start with the case most likely to expose it.

**2. Analyze before you experiment.**
Compare configurations, parameters and test conditions before trying another variation. Make sure you're measuring the same thing.

**3. Unit test every fix.**
A small test can catch a mistake before another full run burns hours.

**4. When the data changes, check the whole system.**
Record size, cardinality and key format affect assumptions throughout the pipeline.

**5. Confirm tests started—and keep monitoring them.**
I spent 2.8 hours waiting on jobs that had already died.

**6. Make important rules enforceable.**
Put them in the tools and workflow. Remembering an instruction isn't a reliable control.

The biggest waste: roughly 27 hours went into runs started before the previous problem was understood.

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
