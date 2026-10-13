# Journal

## 2026-10-12 21:53 — start
Read SKILL.md and harness/README.md in full. Skill checkout level with origin/main (df12dcf).
Wrote ANSWERS.md (defaults verbatim), ASSUMPTIONS.md, PLAN.md before any code (commit f4e7a7a).

## 22:00–22:15 — build
Job, generator, verifier in one shaded jar (job/). Generator manifest is byte-identical across
two runs with one seed (checked by hand at 200,000 records before the harness checks it).
Dashboard: shipped CPU exporter, Prometheus, Grafana, 13 panels in four rows.

## 22:05–22:12 — chain 1: STOPPED at completeness (6.5 min)
Preflight 28/28 PASS. The job drained 6,000,000 orders cleanly in 91 s. My verifier then crashed:
`NoSuchMethodError ... BufferRecycler.releaseToPool()` — I had asked for jackson-databind 2.17.2,
the Kafka connector brings jackson-core 2.15.2. It was reached only because a per-record check built
its message (calling JsonNode.toString) whether or not the check failed.
Fix: databind pinned to 2.15.2; that check builds its message only on a miss.
Proved by hand against the clean arm's outputs still on the broker: every assertion held (21 s);
pointed at the wrong manifest, it stopped and named the misses.
