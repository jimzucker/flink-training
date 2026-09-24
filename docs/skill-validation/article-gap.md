# Why a skill build did not reproduce the published scaling

The published demo reads **2.06× from 1 to 2 cores and 1.99× from 2 to 4**. The
skill's builds kept landing lower — run 48, taking the interview's defaults, read
1.81–1.85× and 1.94–1.96× across three chains. This is what the difference
turned out to be, measured on 2026-09-24, one variable at a time.

## The answer

1. **The published table was a quick look.** Its own `suite.json`
   ([demo-under-harness/results](demo-under-harness.md)) says 2 passes per
   case, `quickLook: true`, `publishable: false`. The write-up did not say so.
2. **Its 1-core case runs a different garbage collector from the cases above
   it.** The published run did not record collectors; re-run with the same jars
   and configuration (E0 below), the 1-core case runs Serial and the rest G1 —
   Java picks Serial in a one-CPU container and G1 at two or more. The skill already said a different collector is a different program
   (run 35), but the harness only printed a note and passed the table.
3. **Measured like for like, the skill already matches it.** With G1 on every
   case and three passes, the published demo's own build reads **1.91× / 1.99×**;
   clean-room run 49, which took the skill's defaults, reads **1.94× / 1.93×**.

## The measurements

Every arm below is three passes per case plus the sentinel, on this laptop,
under the harness as it stood on 2026-09-24.

**E0 — the published build, re-measured at full strength.** Its own job and
generator jars, 4,096 symbol keys (`SYMBOL_COUNT=4096`, as published), its own
configuration except the broker, raised from 4 GB to 4,352 MB because today's
page-cache floor refuses 4 GB with a 1 GB heap.
[results](article-gap/e0-article-build-java-choice/suite.txt)

| case | collector | records/s | GC |
|---:|---|---:|---:|
| 1 | **Copy + MarkSweepCompact (Serial)** | 59,061 | 3.6% |
| 2 | G1 | 118,706 | 0.9% |
| 4 | G1 | 232,894 | 0.3% |

1→2 **2.01×** [1.92, 2.06], 2→4 **1.96×** [1.89, 2.07]. The published figures
reproduce, and the baseline ran Serial.

**E1 — the same, with only G1 forced on every case.**
[results](article-gap/e1-article-build-g1/suite.txt)

| case | records/s | GC |
|---:|---:|---:|
| 1 | 60,849 | 2.3% |
| 2 | 116,373 | 0.7% |
| 4 | 231,595 | 0.3% |

1→2 **1.91×** [1.85, 1.97], 2→4 **1.99×** [1.95, 2.10]. The baseline spent less
time collecting garbage and ran 3% faster; the step off it fell 5%. On this build
alone the two 1→2 ranges overlap, so E2 was run to check it on another.

**E2 — clean-room run 48's build, with only its forced G1 removed.**
[results](article-gap/e2-run48-build-java-choice/suite.txt)

| run 48's build | 1-core collector | 1 core | 2 cores | 1→2 |
|---|---|---:|---:|---:|
| G1 forced, as built ([live check](clean-room/run-48/live-check/suite.txt)) | G1 | 128,148 | 233,207 | **1.82×** [1.62, 1.88] |
| Java's own choice | **Serial** | 119,662 | 234,966 | **1.96×** [1.88, 1.99] |

The 2-core case did not move; the Serial baseline was 6.6% slower, and the step
off it rose 8%. The ranges do not overlap.

**Two builds, opposite directions, same effect:**

| build | Serial baseline | G1 on every case |
|---|---:|---:|
| the published demo's | 1→2 2.01× | 1.91× |
| clean-room run 48's | 1→2 1.96× | 1.82× |

**The broker vendor is not it.** Clean-room run 49's jar, built for Confluent,
measured again on Apache Kafka with only `images.kafka` and `images.kafkaLibs`
changed ([results](article-gap/vendor-run49-on-apache/suite.txt)):

| run 49's jar | 1 core | 2 cores | 4 cores | 1→2 | 2→4 |
|---|---:|---:|---:|---:|---:|
| Confluent | 131,538 | 255,648 | 493,284 | 1.944× | 1.930× |
| Apache | 128,662 | 257,855 | 481,167 | 2.004× | 1.866× |

Every case within 2.5%; the steps move in opposite directions by similar
amounts, which is what noise looks like. So the gap between runs 48 and 49 —
same interview answers, same four-vertex graph — is in the code each agent
wrote. Their parsing and client settings differ (Jackson's object mapper
against its streaming parser; default fetch sizes against 4 MB / 32 MB); **which
of those matters has not been measured.**

## Where every measurement stands, like for like

| build | 1→2 | 2→4 | against 1.80× on the low end |
|---|---:|---:|---|
| published demo, G1 every case (E1) | 1.91× | 1.99× | passes |
| clean-room run 49, Confluent | 1.94× | 1.93× | passes |
| clean-room run 49's jar, Apache | 2.00× | 1.87× | passes |
| clean-room run 48, three chains | 1.81–1.85× | 1.94–1.96× | 1→2 misses on the low end |

## What changed in the skill

- [#90](https://github.com/jimzucker/scalable-flink-skill/pull/90): the harness
  pins G1 on every case unless the configuration names a collector, and a step
  between two cases that ran different collectors is not counted. Replayed:
  runs 47, 48 and 49 unchanged; E0's and E2's 1→2 voided.
- [#86](https://github.com/jimzucker/scalable-flink-skill/pull/86): the scaling
  target is 1.80× per doubling, judged on the low end of the range (the author's
  decision, the same day).

## Still open

- **Which of run 48's code choices costs it 1→2.** Not measured.
- **The page-cache floor contradicts the published configuration.** The
  published demo left the broker 3.00 GB of cache and produced an accepted table;
  the floor, read from `record/configs.json`, is 3.25 GB and refuses it. The
  record does not include that configuration.
- **The published figures.** The post and card quote 2.06× / 1.99× from the quick
  look with a Serial baseline. Like for like the same build reads 1.91× / 1.99×.
  Changing public writing is the author's call.
