# Journal — clean-room run st53

Times are local (EDT), 2026-10-11.

## Step 1 — interview, assumptions, plan (19:07–19:15)

- Read `SKILL.md` (1,409 lines) and `harness/README.md` (440 lines) in full, then the parts of `prove.py` and `lib.py` that the contract leaves implicit: the compose file it generates, preflight's checks, completeness, the README check, the dashboard checks, backlog sizing.
- Skill checkout `~/.claude/skills/scalable-flink-skill` level with `origin/main` at `ada7890` after a fetch.
- Took the default for all six questions, copied verbatim into `ANSWERS.md`. Wrote `ASSUMPTIONS.md` and `PLAN.md` before any code.

## Step 2 — build (19:15–19:25)

- One Maven module, `job/`, shaded jar `job/target/st53-1.0.jar`: the job (`PositionsJob`), the generator (`GenerateBacklog`), the verifier (`VerifyCompleteness`). Flink is `provided`; the generator and verifier use only kafka-clients and Jackson, so they run on the host from the same jar.
- First compile stopped on `DeliveryGuarantee`: `flink-connector-base` is not pulled in by `flink-connector-kafka 3.4.0-1.20` at compile scope and is not in the image's `lib/`. Added it at compile scope.
- Generator determinism checked by hand: two manifests at 200,000 records, one seed, identical sha256 (`f5b4c30a…`).
- Wrote `pipeline.json`, the dashboard (`dashboard/`), `README.md`.

## Step 3 — harness checks before the chain (19:25–19:50)

- `prove.py replay`: OK. `prove.py selftest-pure`: 320 of 321 guards OK. The README guard's self-test did not fire because operator `orders-source` contains the topic name `orders` and the README check is a substring search (SKILL-FEEDBACK F1). Renamed the operator to `order-source`, rebuilt; then 321/321.
- `prove.py up`, then `prove.py completeness` on its own: waited 3 minutes for `mediaanalysisd` (about 3.6 cores) to go quiet, then passed both arms: clean drain 160 s, killed at 39% and restarted, 19 of 19 verifier checks on each arm. Design diff clean; job graph 4 tasks, 14 operators.
- Checked the dashboard's operator regexes against what Prometheus actually holds: the reporter rewrites `-` to `_` (SKILL-FEEDBACK F6). My regexes used `.` and matched.
- `prove.py preflight` on its own: 28/28. Key spread at 4 cores: by-symbol 1065/1008/979/1044, by-account 4017/4069/4159/4139 (maxParallelism 128). Host probe: register-only 1→2 97%, 2→4 99%; memory-bound 1→2 80%, 2→4 80%.

## Step 4 — the chain (`prove.py all`, launched 19:50:46)

- up 2 s; preflight 160 s; completeness 745 s, passed.
- Tiny proof (714 s): 4 cores 431,049/s at 99.2% of cap; 2 cores 250,199/s at 101.1%; 1 core 126,002/s at 99.8%. Second-vantage disagreement 0.07–0.23%. Tiny ratios: 1→2 1.986×, 2→4 1.723×. Backlog needed at the measured rate: 96,986,092; configured 250,000,000 (an over-sized guess; costs fill time only).
- Fill 154 s (250,000,000 records, 148 s of generator time).
- Suite 3,849 s: the 10 planned cases, then 6 settling cases (one for 1→2, five for 2→4). One 1-core pass (p3-asc) was thrown out as a ceiling at 94.7% of cap. 1→2 1.878×, met within the 1% margin (low end 1.786×). 2→4 1.799×, not met (low end 1.725×, 4.2% under). Spreads: 1 core 5.7%, 2 cores 14.4%, 4 cores 18.6%.
- Report 2 s; `all` ended `STOPPED at report` after 93.8 minutes. Dashboard: every panel had data over the suite; all 19 §7 metric names returned data.

## Step 5 — after the stop (21:25–21:45)

- `prove.py probe --repeats 9`: arithmetic 2→4 1.98×; memory-heavy 2→4 1.65× (middle half 7%).
- `prove.py ceiling`: broker 2.5 / 1.0 / 0.5 cores → 504,732 / 503,901 / 487,985 per second at 4 cores, worker at 99% of cap. Broker is not the ceiling.
- No §6a change applied; reasons in `FIXES.md`. Stack left up.
