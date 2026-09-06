# Clean-room validation, run 18 — one run, as a user would ask for it

No experiment, no prediction, no special workload: the plain question a user
brings to the skill, on the harness as it stands at #59. This is the first run
to exercise the partitions guard (#57), which was merged after run 17's mirror
was taken.

Same conditions as [runs 1–17](README.md): fresh agent, empty directory, barred
from this repository and every other test directory, one prompt, no human
input. Run 16's prompt verbatim — positions by symbol and by account, the two
step ratios, `prove.py all --quick`.

## The criteria, written before launch

| criterion | the record |
|---|---|
| the chain passes on the first attempt | run 16 yes; run 15 no (3 attempts); run 17 no (2) |
| harness verbatim, one suite, 0 forbidden-path reads | every run |
| chain `preflight` → `report` ≤ 45 min | 35.6 (16), 33.1 (17), 37.9 (15) |
| whole-run wall clock (reported, not judged) | 1 h 37 m (16), 1 h 53 m (17), 55.9 min (14) |
| no case refused for broker memory | 0 hits (15, 16); run 17 needed 6 GiB and the guard said so |
| every case at 95–101% of cap | 96.3–100.2% (16), 99.5–100.2% (17) |
| sentinel measured, drift reported | +4.1% (16), −1.5% (17) |
| table stamped unpublishable | every `--quick` run |
| preflight reports the partition split | new in #57, first exercised here |
| ratios (recorded, not judged) | 1→2 has read 2.06–2.74×; 2→4 has read 1.72–1.95× |

## Response

Opus 5, harness at #59, `prove.py all --quick`, 47 tool calls, 0
forbidden-path reads, 17:07 → 18:32. Raw results in [run-18/](run-18/).

| criterion | result |
|---|---|
| chain passes on the first attempt | **FAIL** — attempt 1 refused at the tiny proof (80M tiny backlog drained under the 4-core job); resized and passed on attempt 2 |
| harness verbatim, one suite, 0 forbidden reads | PASS |
| chain ≤ 45 min | **PASS — 36.1 min** (up 2 s, preflight 64, completeness 346, tinyproof 555, fill 236, suite 964) |
| whole run (reported) | 1 h 25 m — 24 min build, 14 min the refused attempt |
| no broker-memory refusal | PASS — 0 hits every case |
| every case 95–101% of cap | PASS — 99.7 / 99.8 / 96.0%, sentinel 95.1% |
| sentinel measured | PASS — **+7.5%** |
| table stamped unpublishable | PASS |
| preflight reports the partition split | **PASS — first live use of #57**, 14/14 checks |
| ratios (recorded, not judged) | 1→2 = **2.146×**, 2→4 = **1.539×** |

| cores | records/s | % of cap | src idle |
|---:|---:|---:|---:|
| 1 | 259,267 | 99.7% | 1.0% |
| 2 | 578,061 | 99.8% | 3.6% |
| 4 | 889,406 | 96.0% | 5.5% |
| 1 (sentinel) | 279,391 | 95.1% | 0.8% |

## Measured, not explained

- **2→4 = 1.539×**, the lowest since run 14's starved-broker case — but nothing
  is starved here: broker at 0.52 of 2.5 cores, zero memory-limit hits, source
  idle 5.5%, graph identical across cases, worker at 96.0% of cap. No cause
  measured, none offered.
- **Sentinel drift +7.5%** — the 1-core case read 259,267 first and 279,391
  last. Taking the other end moves 1→2 between 2.07× and 2.23×. One pass per
  case cannot separate drift from noise.
- The refused attempt was the agent sizing backlogs from a guess of ~500k rec/s
  for a pipeline that runs ~930k. Three of the last four runs lost their first
  chain attempt to backlog sizing, never to the harness.

