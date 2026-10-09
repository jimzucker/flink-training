# Seven tips for benchmarking Flink SQL on Confluent Cloud

Draft, 2026-10-07. Audience: engineers. The angle, in the author's words:
"the tips we learned migrating sql to confluent, like the defaults we have to
override etc, postive and helpful stuff"; then, 2026-10-08, "remove #2 /
overall make context about running benchmarks" (watermark alignment removed).
Figures and their sources are in the table under the post.

---

We benchmarked one Flink SQL statement on a laptop and on Confluent Cloud, measuring how its throughput grew as we added compute. The SQL needed no changes, but several Confluent Cloud settings did. These are seven tips from that work:

**1. Make the job use the whole compute pool.** CFU is Confluent's unit of Flink compute, and a pool's size is the most CFU its jobs may use. Confluent's autoscaler decides how much a job actually uses: in a 20 CFU pool it used 10 and still reported everything as OK, so a benchmark of "20 CFU" was really measuring 10. Set the statement's `baseline_cfu` to the pool's size, and check that the setting took effect.

**2. Choose a partition count that divides evenly by every pool size you test.** In our job, each CFU ran one parallel worker, and each worker reads its share of the topic's partitions. 24 partitions can't be shared evenly among 20 workers, so some read more than others. We used 40 partitions, which divide evenly for pools of 5, 10 and 20 CFU.

**3. Give the Kafka cluster enough capacity for your largest pool.** Confluent measures Kafka capacity in eCKU, and you can cap it to control cost. We capped ours at 10 eCKU. With that cap, doubling the Flink pool from 10 to 20 CFU raised throughput only about 1.4 times. With the cap raised to 50 eCKU, the same doubling gave 1.77 times. Each eCKU costs about $0.135 an hour.

**4. Count your test data by reading it, not from the topic's size.** A benchmark needs to know exactly how many records it starts with. A Kafka topic's size also includes records whose write was cancelled, and Flink skips those. When we stopped our data-loading jobs partway, cancelled records were 42% of the topic.

**5. Count with a snapshot query.** A normal count query sends a new running total for every record, which is slow to read: over two hours for 70 million records. A snapshot query (`sql.snapshot.mode = now`) sends only the final total: it counted 34 million records in 72 seconds.

**6. Create a new compute pool for each size you test.** Confluent won't let you lower a pool's maximum size, so going back down needs a new pool. Give each new pool a few minutes before you measure: its usage figures only start a few minutes after it is created, and then arrive about three minutes behind, so you can't yet see what the job is doing.

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
| 42% of the topic was cancelled records; Flink skips them | findings §8, run 24: 37,996,160 committed of a 66,092,231 end offset; the Flink job read 38,005,960 |
| an updated total per record (two change rows each), about 500,000 records a minute, so over two hours for 70 million; 34 million in 72 seconds as a snapshot | findings §8, run 23 (33,594,701 records); the two hours is 70 million at the measured rate |
| a pool's maximum can't be lowered | findings §8, run 03 |
| metrics about three minutes late | the skill's [`harness/README.md`](https://github.com/jimzucker/scalable-flink-skill/blob/main/harness/README.md), the Confluent Cloud paragraph |
| a cluster left running for nine hours | findings §8, run 25 |
| cost list a day behind, promo balance the same day | the skill's `harness/README.md` budget paragraph (scalable-flink-skill #141): $170.89 on the cost list against $382.02 of credit used, 2026-10-06 |

Left out on purpose, per the tone rules: no scaling claim for the cloud (the
one 10→20 CFU step the harness judged read 1.73×, short of its 1.80× target;
findings §8, run 25), and no methodology caveats.
