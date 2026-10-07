# Moving Flink SQL from a laptop to Confluent Cloud: the SQL didn't change, the defaults did

Draft, 2026-10-07. Audience: engineers. The angle, in the author's words:
"the tips we learned migrating sql to confluent, like the defaults we have to
override etc, postive and helpful stuff". Figures and their sources are in the
table under the post.

---

We ran the same Flink SQL statement on a laptop and on Confluent Cloud, and proved it exact on both: every record in, every record out. The statement needed no changes. What needed attention were the platform's defaults, and how you measure. Here's what to set before you trust a number:

**1. Set the statement's baseline to the pool size.** Left to the autoscaler, a statement in a 20 CFU pool stopped at 10 CFU and reported its scaling status as "OK". Set `baseline_cfu` to the pool's size, and read it back.

**2. Check watermark alignment.** It's on by default, and it paused most of our input partitions. If your job doesn't need aligned event time, raise `sql.tables.scan.watermark-alignment.max-allowed-drift`.

**3. Pick a partition count that divides by every pool size you'll run.** One CFU ran one subtask, so 24 partitions can't spread evenly over 20 subtasks. We used 40 for 5, 10 and 20 CFU.

**4. Size the Kafka cluster's eCKU cap for your largest pool.** Capped at 10 eCKU, our 10→20 CFU step read about 1.4×. At 50 eCKU it read 1.77×. Each extra eCKU costs about $0.135 an hour.

**5. Count what a reader gets, not the topic's end offset.** Records from aborted transactions stay in the log. After a short load, 42% of the log was records no reader would ever see.

**6. Use snapshot queries for one-off counts.** A plain bounded GROUP BY streamed two change rows per input record, about 140 minutes for 70 million records through the REST API. With `sql.snapshot.mode = now` the same count gave its four final rows in 72 seconds.

**7. Plan a new compute pool for each size.** A pool's maximum can't be lowered. Allow a few minutes after creating one: the metrics arrive about three minutes late.

**8. Make cleanup unconditional, and check spend from a current source.** The daily cost list runs a day behind; a promo credit's balance is current.

We've built all of this into our open-source skill for proving that a Flink pipeline scales. It now runs Flink SQL on a laptop and on Confluent Cloud: https://github.com/jimzucker/scalable-flink-skill

---

## Where each figure comes from

| in the post | source |
|---|---|
| exact on both platforms | [`cloud-sql-app/README.md`](../skill-validation/cloud-sql-app/README.md): completeness PASS, 20,000,000 records on the laptop and 45,001,150 on Confluent Cloud |
| autoscaler stopped at 10 CFU of 20, status "OK" | [findings §8](../skill-validation/findings.md), run 06 and the autoscaler finding |
| watermark alignment on by default, paused most input partitions | findings §8, run 14 and the watermark finding |
| one CFU ran one subtask; 24 partitions over 20 subtasks | findings §8, run 13 and the partition finding |
| 40 partitions for 5, 10 and 20 CFU | [`cloud-sql-app/pipeline-cloud.json`](../skill-validation/cloud-sql-app/pipeline-cloud.json) |
| about 1.4× at 10 eCKU, 1.77× at 50 | findings §8, runs 17 and 18 |
| $0.135 per eCKU-hour | Confluent's cost list for 2026-10-06, the `price` field of the KAFKA_NUM_CKUS lines |
| 42% of the log was aborted records | findings §8, run 24 (37,996,160 readable of a 66,092,231 log end) |
| two change rows per record, ~140 minutes for 70M, 72 seconds as a snapshot | findings §8, run 23 |
| a pool's maximum can't be lowered | findings §8, run 03 |
| metrics about three minutes late | the skill's [`harness/README.md`](https://github.com/jimzucker/scalable-flink-skill/blob/main/harness/README.md), the Confluent Cloud paragraph |
| cost list a day behind, promo balance current | the skill's `harness/README.md` budget paragraph (scalable-flink-skill #141): $170.89 on the cost list against $382.02 of credit used, 2026-10-06 |

Left out on purpose, per the tone rules: no scaling claim for the cloud (the
one 10→20 CFU step the harness judged read 1.73×, short of its 1.80× target;
findings §8, run 25), and no methodology caveats.
