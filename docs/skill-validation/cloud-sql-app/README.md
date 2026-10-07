# One SQL app on the laptop and on Confluent Cloud

The first app run through the skill's whole chain (`prove.py all`) on both
platforms, 2026-10-05 to 06. One Flink SQL statement,
`job/src/main/resources/enrich.sql`, runs unchanged on both:

```sql
INSERT INTO enriched SELECT order_id, symbol, account, qty, price, qty * price AS notional FROM `{in}`
```

Only the tables around it differ. On the laptop `job/` wraps the statement in a
small Java job, with a seeded generator and a verifier. On Confluent Cloud the
harness writes the tables and fills the input with Flink's faker connector.

| file | what it is |
|---|---|
| `pipeline.json` | the laptop run: 1, 2 and 4 cores |
| `pipeline-cloud.json` | the cloud run: 5, 10 and 20 CFU. "Local first" reads `pipeline.json`'s results before anything is paid for |
| `run-local.sh` | the laptop steps the cloud's "local first" check needs: up, preflight, completeness, tiny proof (`STEPS=` runs fewer) |
| `results-laptop/` | the laptop's evidence |
| `results-cloud/` | the fourth cloud chain; `all-console-1.log` to `-3.log` are the three before it |

Paths are shortened to `~` and `<scratch>`, and the organization ID is
replaced. The cluster, pool and environment IDs in the logs belong to resources
that were deleted.

## Laptop — PASS

| step | result |
|---|---|
| completeness | PASS: 20,000,000 records, exact, with and without a worker killed mid-drain |
| tiny proof | PASS: 1→2 cores 1.791×, 2→4 2.119×, 1→4 3.795× (PASS range 1.5–2.5× a doubling) |

The first tiny proof stopped on its own backlog check: at 1.58M records a
second the 4-core case needed 355M records, and the backlog was 275M. It
passed at 360M.

## Confluent Cloud — four attempts

| attempt | how it ended | what was fixed (scalable-flink-skill) |
|---|---|---|
| 1 | STOPPED at preflight: the back-pressure row asked the laptop's Flink | #137 laptop-only rows; teardown however the chain ends |
| 2 | STOPPED at completeness: the count gave no full answer in 30 minutes | #139 counts run as snapshot queries (findings §8, run 23) |
| 3 | completeness did not match: each output row read three times, and 16% more out than in | #140 every results page read once. The 16% is not explained (findings §8, run 24) |
| 4 | every phase ran; the report crashed after the suite | #141 the report renders cloud cases, and a crash still tears down |

Attempt 4:

| step | result |
|---|---|
| completeness | PASS: 45,001,150 records in, the same out, exact per account |
| tiny proof | PASS: 5 CFU 205,407 records a second, 10 CFU 478,021; 5→10 2.33× |
| suite | 7 cases. The first 5 CFU case was thrown out twice: its two counts were 12.8% and 8.3% apart, against a 5% limit. Why is not known |
| 10→20 CFU | **1.73×, missed** (target 1.80×): 1.73× in both passes |
| 5→10 CFU | 1.789×. `suite.json` says "met", which was a harness bug: the step was judged on one same-time pair of 1.948×. Since #142 it reads **undecided**: its readings fall on both sides of 1.80× |

The crash in the report skipped the teardown, and the stack stayed up for nine
hours. That, the four attempts and the probes used about $213 of Confluent's
promo credit on 2026-10-06.

`pipeline-cloud.json` has no `claimSteps` yet, so "local first" stops the next
cloud run before anything is created. Name the steps the claim is about first.
