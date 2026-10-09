# Five tips for running Flink SQL on Confluent Cloud

Draft, 2026-10-07. Audience: engineers. The angle, in the author's words:
"the tips we learned migrating sql to confluent, like the defaults we have to
override etc, postive and helpful stuff". Figures and their sources are in the
table under the post.

---

We ran one Flink SQL statement on a laptop and on Confluent Cloud. The SQL needed no changes, but several Confluent Cloud settings did. Here are five things we learned:

**1. If a statement needs its whole compute pool, set its baseline_cfu to the pool's size.** CFU is Confluent's unit of Flink compute. Left alone, the autoscaler used 10 CFU of a 20 CFU pool and still reported its status as "OK".

**2. Use a snapshot query for a one-off count.** A plain bounded GROUP BY returned two update rows per input record, about 500,000 records a minute through the REST API. As a snapshot query (`sql.snapshot.mode = now`), a count of 34 million records returned its four final rows in 72 seconds.

**3. Count records with a reader that only sees committed data, not with the topic's end offset.** Records from transactions that never committed stay in the log and still count toward the end offset. When we stopped data-loading jobs mid-run, those records were 42% of the log.

**4. Pick a partition count that divides evenly by the parallelism you run at.** In our job one CFU ran one subtask, so 24 partitions could not spread evenly over 20 subtasks. We used 40 for pools of 5, 10 and 20 CFU.

**5. Delete what a test creates, even when the test crashes, and watch spend somewhere current.** Our test tool crashed once and left a cluster running for nine hours. Confluent's cost list runs a day behind; a promo credit's balance updates the same day.

We've built all five into our open-source skill for measuring whether a Flink pipeline scales. It now runs Flink SQL on a laptop and on Confluent Cloud: https://github.com/jimzucker/scalable-flink-skill

---

## Where each figure comes from

| in the post | source |
|---|---|
| the same SQL on a laptop and on Confluent Cloud | [`cloud-sql-app/README.md`](../skill-validation/cloud-sql-app/README.md) |
| autoscaler used 10 CFU of 20, status "OK" | [findings §8](../skill-validation/findings.md), run 06 and the autoscaler finding |
| two update rows per record, about 500,000 records a minute; 34 million in 72 seconds as a snapshot | findings §8, run 23 (33,594,701 records) |
| 42% of the log from transactions that never committed | findings §8, run 24 (37,996,160 committed of a 66,092,231 end offset) |
| one CFU ran one subtask; 24 partitions over 20 subtasks | findings §8, run 13 and the partition finding |
| 40 partitions for 5, 10 and 20 CFU | [`cloud-sql-app/pipeline-cloud.json`](../skill-validation/cloud-sql-app/pipeline-cloud.json) |
| a cluster left running for nine hours | findings §8, run 25 |
| cost list a day behind, promo balance the same day | the skill's [`harness/README.md`](https://github.com/jimzucker/scalable-flink-skill/blob/main/harness/README.md) budget paragraph (scalable-flink-skill #141): $170.89 on the cost list against $382.02 of credit used, 2026-10-06 |

Left out on purpose: tips that apply only to benchmarking or to some jobs
(watermark alignment, a capped eCKU limit, a new pool per size; author,
2026-10-08: "we want to article to be most generic"); any scaling claim for
the cloud (the one 10→20 CFU step the harness judged read 1.73×, findings §8,
run 25); methodology caveats, per the tone rules.
