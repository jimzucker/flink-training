# Journal

One branch, a commit per step with the build hash (SKILL.md §2, nobody to review).

| when | step | what drove it | what was decided | how it was verified |
|---|---|---|---|---|
| 13:06–13:10 | interview, plan | §1, §1a | all six defaults taken; ANSWERS, ASSUMPTIONS, PLAN written and committed before code | files committed (60fd66b) |
| 13:10–13:16 | build | §2 | one shaded jar: job, generator, verifier; generator determinism checked by hand (two manifests at 200,000 records, byte-identical) | `cmp` of the two manifests |
| 13:16–13:41 | chain 1 (build f0569942fc1b) | `prove.py all` | — | preflight passed; completeness passed both arms (clean 112.7 s; killed at 3,991,564 of 8,000,000); tiny proof STOPPED: the prices source ran at parallelism 1 and the harness requires every vertex at the case's parallelism. 1-core tiny case read 123,354 rec/s |
| 13:42 | fix | chain 1's stop | prices source now runs at the case's parallelism; backlogs re-sized from the 1-core rate (suite 180,000,000, tiny 120,000,000); price writes made asynchronous (chain 1's first fill spent 5.5 min writing 16,384 prices one acknowledged send at a time) | rebuild; chain 2 |
| 13:42–14:07 | chain 2 (jar sha 2d0f47f57e78, prices source fixed) | rerun | — | completeness passed both arms; tiny proof 1c 125,028, 2c 231,876, 4c 429,240 rec/s; STOPPED by one harness self-test that depends on backlog.count (SKILL-FEEDBACK F4) |
| 14:07 | fix | chain 2's stop | backlog.count 250,000,000 so the self-test's credit is not capped | chain 3 |
| 14:07–15:29 | chain 3 (build e35f29848e13) | `prove.py all` | — | preflight, completeness (both arms), tiny proof (1c 127,892, 2c 220,390, 4c 420,010; ratios 1.72x and 1.91x inside bounds), fill 292 s, suite 51 min. STOPPED at report: no scaling result, 2c and 4c spread 24.4% / 28.8%, slow passes at 2.7–3.9 GB host swap |
| 15:30–16:13 | suite re-run, same build | FIXES 1 | prediction written first | no result again: 1c 21.0%, 4c 28.9% spread; slow passes at 4.4–5.1 GB host swap |
| 16:15 | probe, 9 repeats | §6 | — | host memory-heavy work 1->2 1.96x, 2->4 1.50x (middle half 7%); simple arithmetic 1.96x / 1.96x |
| 16:20 | stopped | no human to close the programs holding the memory | stack left up | — |
