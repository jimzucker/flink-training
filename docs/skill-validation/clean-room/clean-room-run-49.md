# Clean-room run 49 — the defaults, on Confluent

The same defaults as run 48, except question 6 answered **Confluent**
(`confluentinc/cp-kafka:7.7.0`), after #87 made the harness find the broker's
tools in either image. DataStream. About 1 h 40 min, 81 tool calls.
Files: [run-49/](run-49/).

## What it produced

**PASS, on the first attempt, the first skill build to pass**
([suite.txt](run-49/results/suite.txt)):

| cores | records/s | passes apart |
|---:|---:|---:|
| 1 | 131,538 | 1.1% |
| 2 | 255,648 | 6.8% |
| 4 | 493,284 | 5.4% |

1 → 2 **1.944×** (low end 1.87×), 2 → 4 **1.930×** (low end 1.913×), both
against the 1.80× target (#86). All 10 passes counted; the job jar carries no
Confluent-only classes.

## Either Kafka

Its jar, moved to Apache Kafka with only the two image lines changed, passed
preflight and completeness, and measured 2.004× / 1.866×
([article-gap/vendor-run49-on-apache](../article-gap/vendor-run49-on-apache/suite.txt)) —
within noise of the Confluent figures. Run 48's jar went the other way and
passed completeness on Confluent.

## What the agent reported

15 findings in [SKILL-FEEDBACK.md](run-49/SKILL-FEEDBACK.md). Fixed: F5
(`images.kafkaLibs` in the README) and F10 (tiny-proof broker advice for a case
at its cap), both in #88. The rest are open, several of them reported by runs 47
and 48 as well.
