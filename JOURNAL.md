# Journal

A record of each step: what drove it, what was decided, how it was verified, and
what came out of its review.

Steps follow the deliverable order the assignment lists. Each is developed on its
own branch, reviewed, reworked if the review calls for it, then squash-merged to
`main`. Branches are kept so the working history stays inspectable.

| Step | Deliverable | Branch | Status |
|---|---|---|---|
| 00 | Scaffold | `step-00-scaffold` | done |
| 01 | Pipeline design | `step-01-pipeline-design` | not started |
| 02 | Generators | `step-02-generators` | not started |
| 03 | CI | `step-03-ci` | not started |
| 04 | Part 1 — positions | `step-04-part1-positions` | not started |
| 05 | Part 2 — market value | `step-05-part2-marketvalue` | not started |
| 06 | Correctness demo | `step-06-correctness-demo` | not started |
| 07 | Local Docker demo | `step-07-docker-local` | not started |
| 08 | Latency | `step-08-latency` | not started |
| 09 | Scale | `step-09-scale` | not started |
| 10 | AWS | `step-10-aws` | not started |
| 11 | Final demo | `step-11-final-deck` | not started |

CI lands at step 03: after the first module with tests exists, and before the
Flink jobs it needs to guard.

---

## Step 00 — Scaffold

- **Branch:** `step-00-scaffold`
- **Prompt:** Set up the project from the assignment, delivered step by step in the
  order the deliverables are listed. Step 0 is the scaffold: parent POM,
  `.gitignore`, `JOURNAL.md`, `docs/` layout, README skeleton.

### Approach

Build only what later steps need, and make the version choices explicit now so
they are not relitigated later.

- **Flink 1.20.4, Java 17.** Flink 1.20 is the last 1.x LTS. The deciding factor
  is step 9: AWS Managed Service for Apache Flink runs the 1.x line, so choosing
  Flink 2.x would strand the AWS deliverable.
- **Parent POM carries no modules yet.** Each module arrives with the step that
  needs it, so the build always reflects what actually exists.
- **JDK guard in the build.** This machine's default JDK is 25; Flink 1.20 does
  not run on it. The enforcer plugin fails immediately with an instruction rather
  than letting it surface as a stack trace inside Flink.
- **Jar and image versions held together.** `flink:1.20.5` is on Docker Hub but
  1.20.5 is not on Maven Central, so both are pinned to 1.20.4.

### Commands

```bash
mvn validate                        # with default JDK 25 -> fails by design
source scripts/env.sh && mvn validate   # with Java 17 -> passes
mvn dependency:get -Dartifact=<each pinned Flink artifact>
```

### Results

| Check | Outcome |
|---|---|
| `mvn validate` on JDK 25 | Fails with `Flink 1.20 requires Java 17. Run: source scripts/env.sh` |
| `mvn validate` on JDK 17 | Passes, exit 0 |
| Resolve `flink-streaming-java:1.20.4` | OK |
| Resolve `flink-clients:1.20.4` | OK |
| Resolve `flink-connector-kafka:3.4.0-1.20` | OK |
| Resolve `flink-metrics-prometheus:1.20.4` | OK |
| Resolve `flink-test-utils:1.20.4` | OK |
| Docker image `flink:1.20.4-java17` | Exists on Docker Hub |

Every pinned version was resolved against the real repository rather than assumed.

### Verification

The JDK guard is verified in both directions — it fails on the wrong JDK and
passes on the right one. A guard only tested in the passing direction would not
be evidence of anything.

### Review

Round 1 feedback confirmed Flink 1.20, the `groupId`, and the scala-free image;
asked that `.claude/` not be tracked, that changes be pushed, and that CI be
added to the plan.

- `.claude/` added to `.gitignore`
- `main` and `step-00-scaffold` pushed to origin
- CI added as step 03; former steps 03–10 shifted to 04–11

Full exchange: [`docs/reviews/step-00.md`](docs/reviews/step-00.md)

**Outcome:** approved, squash-merged to `main`, tagged `step-00`.
