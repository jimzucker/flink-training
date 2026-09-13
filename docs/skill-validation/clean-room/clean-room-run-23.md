# Clean-room validation, run 23 — chain clean, and two harness defects found

Run 22 produced no ratios at all: the VM budget rule from #68 refused
configurations that runs 20 and 21 had already passed with, and it contradicted
the broker guard's own hint — 5,632m asked for against 3,613m allowed. #69
removed the enforcement, and this run followed on the same day.

## Result

**Chain clean on the first attempt, 49.0 min, zero suite refusals.**

| cores | records/s | passes | spread | % of cap | GC |
|---:|---:|---:|---:|---:|---:|
| 1 | 178,387 | 3 | 4.7% | 99.6% | 11.2% |
| 2 | 372,648 | 2 | 0.5% | 100.1% | 4.1% |
| 4 | 668,253 | 2 | 0.8% | 100.3% | 1.6% |

1→2 = **2.089×** (2.025–2.132), 2→4 = **1.793×** (1.782–1.804). Quick table,
stamped `publishable: false`; the agent quoted that verbatim and added the
README's own line about never quoting a quick table.

Wall clock 1 h 47 m: 49.0 min accepted chain, 56.9 min before it (~37 first-time
build and measurement, ~19 redone), teardown ~1 min. **None of the accepted
chain was re-run.**

## Two defects in the harness, found by the run

1. **The shipped example kills the 4-core worker.** `pipeline.example.json` had
   768m base + 1280m per core, which is 5,888m at four cores; with a 6g broker
   that is past what a 7.8 GB VM will give, and the task manager was
   memory-killed. The example now ships 1024m + 768m per core — what this run
   actually used.
2. **The broker guard fires on the smallest case, not the largest.** At
   `kafkaMemory: 3g` the 1-core case hit the cgroup limit **9,437 times** while
   the 4-core case hit it zero times: on an idle VM the page cache reaches the
   limit, and under load global reclaim trims first. Run 21 hit the same thing.
   The guard now exempts a case whose worker is at ≥99% of cap — a worker at its
   cap is not waiting on the broker — while the case the guard was built for sat
   at 96.4% and ran 13% slow.

## Measured, not explained

- 1→2 above linear at 2.089× with GC 11.2% at one core against 1.6% at four —
  but giving the 1-core case 17% more memory moved neither its GC nor its rate,
  so heap size alone does not explain it and the controlled experiment was not
  run.
- Where the missing 10% of 2→4 goes: worker at cap, broker at 20% of its cap,
  source idle 3.2%, no `ceiling` run, so the ceiling is unnamed.
- Broker refaults swung 89k–572k with no effect on rate.
