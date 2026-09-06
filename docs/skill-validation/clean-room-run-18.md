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

*(written after the run)*
