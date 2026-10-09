# Seven tips for benchmarking Flink SQL on Confluent Cloud

Draft, 2026-10-07. Audience: engineers. The angle, in the author's words:
"the tips we learned migrating sql to confluent, like the defaults we have to
override etc, postive and helpful stuff"; then, 2026-10-08, "remove #2 /
overall make context about running benchmarks" (watermark alignment removed).
Figures and their sources are in the table under the post.

---

We benchmarked one Flink SQL statement on a laptop and on Confluent Cloud, measuring how its throughput grew as we added compute. The SQL needed no changes, but several Confluent Cloud settings did. These are seven tips from that work:

**1. Set the statement's baseline to the pool size.** Left to the autoscaler, a statement in a 20 CFU pool stopped at 10 CFU and reported its scaling status as "OK". Set `baseline_cfu` to the pool's size, and read it back.

**2. Pick a partition count that divides by every pool size you'll run.** In our job, one CFU ran one subtask, so 24 partitions couldn't spread evenly over 20 subtasks. We used 40 for 5, 10 and 20 CFU.

**3. If you cap the Kafka cluster's eCKU to control cost, size the cap for your largest pool.** Capped at 10 eCKU, our 10→20 CFU step read about 1.4×. At 50 eCKU it read 1.77×. Each eCKU costs about $0.135 an hour.

**4. Count what a reader gets, not the topic's end offset.** Records from aborted transactions stay in the log. After a short load, 42% of the log was records that readers of committed data skip.

**5. Use snapshot queries for one-off counts.** A plain bounded GROUP BY streamed two change rows per input record, read through the REST API at about 500,000 records a minute. With `sql.snapshot.mode = now`, a count of 34 million records gave its four final rows in 72 seconds.

**6. Plan a new compute pool for each size.** A pool's maximum can't be lowered. Allow a few minutes after creating one: the metrics arrive about three minutes late.

**7. Make sure a benchmark deletes what it creates, even if it crashes, and check spend somewhere that is up to date.** Our test tool crashed once and left a cluster running for nine hours. Confluent's daily cost list runs a day behind; a promo credit's balance updates the same day.

We've built all of this into our open-source skill for measuring whether a Flink pipeline scales. It now runs Flink SQL on a laptop and on Confluent Cloud: https://github.com/jimzucker/scalable-flink-skill

---

## Where each figure comes from

| in the post | source |
|---|---|
| the same SQL benchmarked on a laptop and on Confluent Cloud | [`cloud-sql-app/README.md`](../skill-validation/cloud-sql-app/README.md) |
| autoscaler stopped at 10 CFU of 20, status "OK" | [findings §8](../skill-validation/findings.md), run 06 and the autoscaler finding |
| one CFU ran one subtask; 24 partitions over 20 subtasks | findings §8, run 13 and the partition finding |
| 40 partitions for 5, 10 and 20 CFU | [`cloud-sql-app/pipeline-cloud.json`](../skill-validation/cloud-sql-app/pipeline-cloud.json) |
| about 1.4× at 10 eCKU, 1.77× at 50 | findings §8, runs 17 and 18 |
| $0.135 per eCKU-hour | Confluent's cost list for 2026-10-06, the `price` field of the KAFKA_NUM_CKUS lines |
| 42% of the log was aborted records | findings §8, run 24 (37,996,160 readable of a 66,092,231 log end) |
| two change rows per record, about 500,000 records a minute; 34 million in 72 seconds as a snapshot | findings §8, run 23 (33,594,701 records) |
| a pool's maximum can't be lowered | findings §8, run 03 |
| metrics about three minutes late | the skill's [`harness/README.md`](https://github.com/jimzucker/scalable-flink-skill/blob/main/harness/README.md), the Confluent Cloud paragraph |
| a cluster left running for nine hours | findings §8, run 25 |
| cost list a day behind, promo balance the same day | the skill's `harness/README.md` budget paragraph (scalable-flink-skill #141): $170.89 on the cost list against $382.02 of credit used, 2026-10-06 |

Left out on purpose, per the tone rules: no scaling claim for the cloud (the
one 10→20 CFU step the harness judged read 1.73×, short of its 1.80× target;
findings §8, run 25), and no methodology caveats.
