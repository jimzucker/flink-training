# Assumptions — clean-room run 44

> Clean-room run 44's own file, kept word for word. Where it names the
> prefix `st44-`, the fixture now substitutes whatever `PREFIX` is set to
> when `run-reference.sh` runs, which is `refpipe` by default.

**Nobody approved anything in this file.** The six interview answers in
`ANSWERS.md` came from a real person and are the spec. Everything below is a
decision *I* made because the answers did not settle it, there was no one to
ask, and §1 of the skill says to decide, write it down and proceed. Each one is
marked with the claim it feeds.

---

## A. The six answers (§1) — answered, not assumed

All six were answered by the user. They are in `ANSWERS.md` verbatim and are
not repeated here. Nothing in section A is mine.

---

## B. Decisions the answers did not settle

### B1. Record shape and size — feeds every throughput number

The answer says "a sensor id, a location, a date/time and a temperature in
Celsius" but not the encoding. **Assumed: one JSON object per reading, UTF-8,
Kafka key = the location string.**

```
{"sensorId":"sensor-0042","location":"loc-04","readingTime":"2026-01-01T00:16:40Z","temperatureC":21.3}
```

103 bytes of value, 6 bytes of key, constant length. A real parse (Jackson
streaming) and a real ISO-8601 parse are therefore on the per-record path,
which is what makes this a pipeline that costs something per record rather
than one bound by the transport (skill §6a).

**Marked as mine because it sets the absolute rate.** A different encoding
gives a different records-per-second and the two are not comparable.

### B2. Temperature arithmetic is exact, in tenths of a degree — feeds §4 correctness

Answer 4 says the output carries "the total and the count, not a rounded
average, so there is no rounding to argue about". Floating-point addition is
not associative, so a `double` total is only reproducible if the addition order
is, which is a promise I would rather not depend on.

**Assumed: every temperature is a decimal with exactly one fractional digit in
the range 15.0–40.0 °C, and the job accumulates the raw decimal as an integer
number of tenths.** The published total is that integer rendered back with one
decimal. Verification is then integer equality with no tolerance, and the
replay is byte-identical for a reason rather than by luck.

### B3. Hour buckets close per key, on the key's own data — feeds §4 and the second vantage

Answer 1 says the bucket comes from the timestamp inside each reading and the
machine clock is never used. Answer 4 says a late reading whose hour has
already been published does not reopen it.

**Assumed: no watermarks and no `TumblingEventTimeWindows`. A
`KeyedProcessFunction` holds one open hour per location; the open hour is
published the moment a reading for a later hour arrives for that location, and
a reading for an hour at or before the published one is dropped.**

Consequences, stated because they are visible in the output:

- The newest hour of every location is never published — nothing arrives after
  it to close it. The verifier's expected answer excludes it, computed from the
  input.
- Ordering is the per-key ordering a keyed stream guarantees, and nothing else.
  The generator writes all of one location's readings to one Kafka partition,
  and the sink is keyed by location and chained to the aggregation, so no
  rebalance sits between them.
- Nothing depends on wall clock, watermark progress, or the order in which
  different locations are interleaved.

### B4. The 100 locations are staggered in event time — feeds the second-vantage guard

This is the assumption most likely to matter to a reader.

The second vantage (§5) is the sum of the `count` field over the output topic:
how much input the pipeline's own output accounts for. That number moves **one
closed window at a time**. If all 100 locations closed their hour at the same
point in the stream it would jump by 3,600,000 records at once (1,000 sensors ×
3,600 s), against a measurement window that consumes tens of millions — a
step of several percent against a 5% tolerance, and the two vantages would then
disagree at random.

**Assumed: location *n* reads `36 × n` seconds ahead of location 0 in event
time.** Physically it is 100 sites whose clocks are offset; arithmetically it
spreads the 100 hour boundaries evenly over one simulated hour, so exactly one
location closes an hour every 36 simulated seconds and the second vantage
advances in steps of 36,000 records rather than 3,600,000.

The amount of input sitting in still-open hours is then
`1000 × (s mod 36) + 1,782,000` records, which oscillates over a range of only
35,000 records — so the *difference* the harness actually uses cancels to
better than 0.4% of any window this suite measures. That is arithmetic, not a
measurement; the run reports the vantage disagreement the harness measures.

This does not change what the pipeline computes. Every reading still falls in
the hour bucket of its own timestamp.

### B5. Names and cardinality

**Assumed:** locations are `loc-00` … `loc-99`, sensors `sensor-0000` …
`sensor-0999`, sensor *k* belongs to location *k* / 10. That is exactly the
100 locations × 10 sensors = 1,000 sensors of answer 3. The key space Flink
sees is the location string, 100 distinct keys.

### B6. Kafka partitioning: 8 partitions, location *n* → partition *n* mod 8

8 is divisible by 1, 2 and 4 (§6). 100 locations over 8 partitions is 13 on
four of them and 12 on the other four — uneven by itself. Flink 1.20 assigns
partition *p* to source subtask `(start + p) mod R`, so at 4 readers each
subtask gets `{p, p+4}` = 13 + 12 = 25 locations, and at 2 readers 50. **The
source load is exactly even at every case**, which is why the uneven partitions
are acceptable.

### B7. Generator writes with lz4 compression

**Assumed.** JSON compresses about 4:1, so the broker's page cache and its disk
both hold four times as much of the backlog, and the decompression cost lands
on the component under test, where the study wants it. It changes the absolute
rate and is stated next to it.

### B8. The guarantee

Answer 4 asks for exact totals, nothing double counted, and the same answer on
a replay. **Assumed, per §4: exactly-once checkpointing for state, and an
at-least-once Kafka sink made idempotent** — each row is the absolute total and
count for one (location, hour), so a replayed row overwrites itself with the
same bytes. Checkpoint interval 10 s.

### B9. Assertions from §4 that do not apply

| §4 assertion | status |
|---|---|
| distinct keys = the number predicted | applies: 100 locations |
| every aggregation sums to the manifest exactly | applies |
| **two paths over the same input agree exactly** | **does not apply.** This pipeline has one aggregation and one path. There is no second path to compare, so the assertion is written down here rather than silently skipped |
| each key's published values never go backwards | applies: the published hour per location is strictly increasing on the clean arm, and may step back at most once per location on the killed arm |
| each key appears in exactly one partition | applies |
| all of it again after a mid-run kill | applies |

### B10. "Byte-identical output on a replay" is per record, not per topic

Answer 1 asks for byte-identical output on a replay. **Assumed: that means
every published row is byte-identical, and the set of rows is identical.** The
interleaving of different locations across the output topic's 8 partitions is
not fixed, because different locations are independent and run on different
subtasks. Within a location — the only place Kafka or Flink promises an order —
the bytes and the order are identical.

### B11. Two panels of §7's seven are replaced

§7's panel list is written for a pipeline with fan-out. *Rate per stage* and
*the two paths overlaid* have no meaning here. **Assumed, per §7's own
instruction to keep the question and not the panel:** readings read per second
against rows published per second on a log scale, and the parallel subtasks of
the single aggregation overlaid.

### B12. Sizes chosen before anything was measured

The backlog counts in the first `pipeline.json` are a guess, as §3 says they
must be. They are re-derived from the tiny proof's measured rate before the
suite runs, and the final numbers are the ones in `pipeline.json` at the end of
the run.

### B13. Host conditions

Ambient host load was measured at 1.62 of 8 cores before the run — higher than
ideal for a scaling measurement. **Assumed: it is acceptable to proceed and to
report it.** The harness records the load average at the open and close of
every window; any case that opened on a busy host is called out beside its
number rather than reported quietly.

### B14. The dashboard's panels were checked for data, not for pixels

§7 asks for images rendered server-side with the timezone passed explicitly.
Grafana's image renderer is a separate plugin and a separate container, and this
Docker VM (9,937 MiB) had no room for one alongside a 5,632m broker and a 3,072m
worker. **Assumed: verifying that every panel returns data over the suite's own
time range — through Grafana's own datasource proxy, after the suite ended — is
an acceptable substitute, stated as one.** It is weaker than looking: it cannot
catch a legend that does not fit or a series quietly on a second axis. The
dashboard's default range is set to absolute epochs covering both suites, and
its timezone is set explicitly to `America/New_York`, the host's own.

### B15. The suite was measured twice, and both are published

The harness's scorecard flagged the first suite for host load. **Assumed: it is
legitimate to measure the same build a second time under better conditions,
provided both tables are published and the reason for the second is written down
before it runs** (FIXES.md, change 2). Suite B is the one led with because it is
later and its 2→4 interval is much tighter; suite A is kept in
`results/suite-a/`. Neither met the claim on both steps, and the prediction that
motivated the second suite was only half right — which is recorded rather than
tidied away.
