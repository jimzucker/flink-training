# Eight tips for benchmarking Flink SQL on Confluent Cloud

Draft, 2026-10-07. Audience: engineers. The angle, in the author's words:
"the tips we learned migrating sql to confluent, like the defaults we have to
override etc, postive and helpful stuff"; then, 2026-10-08, "remove #2 /
overall make context about running benchmarks" (watermark alignment removed).
Figures and their sources are in the table under the post.

---

We benchmarked one Flink SQL statement on a laptop and on Confluent Cloud, measuring how its throughput grew as we added compute. The SQL needed no changes, but several Confluent Cloud settings did. These are eight tips from that work:

**1. Make the job use the whole compute pool.** CFU is Confluent's unit of Flink compute, and a pool's size is the most CFU its jobs may use. Confluent's autoscaler decides how much a job actually uses: in a 20 CFU pool it used 10 and still reported everything as OK, so a benchmark of "20 CFU" was really measuring 10. Set the statement's `baseline_cfu` to the pool's size, and check that the setting took effect.

**2. Choose a partition count that divides evenly by every pool size you test.** Each worker in the job reads a share of the topic's partitions (in our job, one worker per CFU). Say a topic has 6 partitions and the job has 4 workers: two workers get 2 partitions and two get 1. The job runs at the pace of its busiest workers, so 4 workers do the work of 3. Pick a count that every pool size divides into, such as 40 for pools of 5, 10 and 20.

**3. Give the Kafka cluster enough capacity for your largest pool.** A Flink benchmark needs Kafka to keep up, or it measures Kafka instead. Say Kafka can carry 100 records a second and your job does 60: double the job and it could do 120, but you'll measure 100, and the job looks as if it scales worse than it does. Confluent measures Kafka capacity in eCKU, and you can cap it to control cost, so set the cap for your largest test. Each eCKU costs about $0.135 an hour.

**4. Count your test data by reading it, not from the topic's size.** A benchmark needs to know exactly how many records it starts with. A Kafka topic's size also includes records whose write was cancelled, and Flink skips those. Say a loading job writes 1,000 records and is stopped before the last 400 are committed: the topic shows 1,000, but Flink reads 600.

**5. Check your results with a snapshot query.** After a benchmark you count records to make sure nothing was lost: how many went in and how many came out. A normal Flink SQL count doesn't send one answer. It sends a new running total every time a record arrives, so counting 100 million records sends 100 million updates, and you have to read through all of them to reach the final number. That can take hours. A snapshot query (`sql.snapshot.mode = now`) sends only the final total, in about a minute.

**6. Create a new compute pool for each size you test.** Confluent won't let you lower a pool's maximum size, so going back down needs a new pool.

**7. Wait for the usage figures before you measure.** A new pool's usage figures only start a few minutes after it is created, and then arrive about three minutes behind. Start measuring once they show the job using the whole pool.

**8. Make sure a benchmark deletes what it creates, even if it crashes, and check spend somewhere that is up to date.** Our test tool crashed once and left a cluster running for nine hours. Confluent's daily cost list runs a day behind; a promo credit's balance updates the same day.

We've built all of this into our open-source skill for measuring whether a Flink pipeline scales. It now runs Flink SQL on a laptop and on Confluent Cloud: https://github.com/jimzucker/scalable-flink-skill

---

## Where each figure comes from

| in the post | source |
|---|---|
| the same SQL benchmarked on a laptop and on Confluent Cloud | [`cloud-sql-app/README.md`](../skill-validation/cloud-sql-app/README.md) |
| autoscaler stopped at 10 CFU of 20, status "OK" | [findings §8](../skill-validation/findings.md), run 06 and the autoscaler finding |
| one worker per CFU in our job | findings §8, run 13 (1 subtask at 1 CFU, 20 at 20 CFU) |
| 6 partitions over 4 workers; 100 and 120 records a second | made-up numbers to illustrate; the measured case was 24 partitions over 20 subtasks (run 13), and the Kafka cap took a doubling from 1.77× to about 1.4× (runs 17 and 18) |
| 40 partitions for 5, 10 and 20 CFU | [`cloud-sql-app/pipeline-cloud.json`](../skill-validation/cloud-sql-app/pipeline-cloud.json) |
| $0.135 per eCKU-hour | Confluent's cost list for 2026-10-06, the `price` field of the KAFKA_NUM_CKUS lines |
| cancelled records in the topic's size, which Flink skips (1,000 and 600 are illustrative; measured: 42% of a short load) | findings §8, run 24: 37,996,160 committed of a 66,092,231 end offset; the Flink job read 38,005,960 |
| a running total per record, 100 million updates for 100 million records, hours to read; a snapshot query about a minute (100 million is illustrative) | findings §8, run 23: about 500,000 records a minute read as running totals (over two hours for 70 million), and 33,594,701 records counted in 72 seconds as a snapshot |
| a pool's maximum can't be lowered | findings §8, run 03 |
| metrics about three minutes late | the skill's [`harness/README.md`](https://github.com/jimzucker/scalable-flink-skill/blob/main/harness/README.md), the Confluent Cloud paragraph |
| a cluster left running for nine hours | findings §8, run 25 |
| cost list a day behind, promo balance the same day | the skill's `harness/README.md` budget paragraph (scalable-flink-skill #141): $170.89 on the cost list against $382.02 of credit used, 2026-10-06 |

Left out on purpose, per the tone rules: no scaling claim for the cloud (the
one 10→20 CFU step the harness judged read 1.73×, short of its 1.80× target;
findings §8, run 25), and no methodology caveats.
