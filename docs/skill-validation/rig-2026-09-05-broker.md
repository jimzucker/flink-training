# Diagnosis, 2026-09-05 — run 14's 1.35× was a starved broker

[Run 14](clean-room-run-14.md) reported **2→4 = 1.35×** on one pass per case,
against 1.85–2.15× in every earlier run. This is the controlled comparison
that explains it. No agent, no skill: the agent's own build and backlog from
run 14, re-measured on the same rig with one variable changed at a time.

## What was held still

Build `01cf956ac64d455e`, 264M-record backlog, 8 partitions, 10 s
checkpoints, four outputs per input, worker memory 2560m/3g, three passes per
case in alternating order plus the sentinel. Only the broker's container
memory limit and CPU cap were changed, and never both at once.

## The arms

| arm | 4-core case | 2-core case | 2→4 |
|---|---|---|---|
| broker 2 GiB, cap 3.0 | **all three passes refused** at 93.1 / 93.8 / 93.1% of cap | 336,451 and 401,088 (two more refused at 89.9 / 90.5%) | not reportable |
| broker 4 GiB, cap 3.0 | 671,852 / 657,096 / 626,011 at 99.6–100.1% of cap, mean **651,653**, spread 7.0% | mean 370,883, spread 20.5% | voided by the 2c spread |
| broker 4 GiB, cap 2.0 | mean **604,603**, spread 12.7%, cap 99.2% | mean 350,492, spread 10.9% | **1.725×** |

Broker CPU cap is not the variable that matters: the broker used 0.10–0.25
cores of the 2–3 it was given in every arm.

## The measurement that names the cause

The broker's own cgroup counters, read at the window's open and close:

| broker limit | limit hits in window | file-page refaults | file cache | rate | worker at |
|---|---:|---:|---:|---:|---:|
| 2 GiB | **30,927** | 520,575 | 649 MB | 562,907 | **96.4%** |
| 4 GiB | **0** | 584,091 | 2.12 GB | 646,423 | 98.1% |

Those two are single 4-core cases run back to back on the same stack, minutes
apart, with nothing changed but the limit. The broker at 2 GiB could not hold
the working set, evicted file pages and read the backlog back off disk; the
worker waited on it and the measured rate fell 13%.

**96.4% of cap is above the 95% floor.** No guard on record could refuse that
case — which is exactly how run 14 published 1.35× from a 4-core pass sitting
at 95.9%. The guard added in #54 refuses on the limit hits themselves, and
its threshold comes from the measured noise: across the seven cases run at
4 GiB the in-window count was zero every time.

This is the third time the same pathology has been measured on this host.
Run 13's agent found it by hand (a 4-core case at 87.0% of cap, 71,717 limit
hits, 1.21M refaults, cache squeezed to 412 MB) and fixed it by going from
2 GiB to 4 GiB. Run 14 shipped 2 GiB and nothing caught it. Now it is a guard.

## What is still not explained

With the broker fed and the worker pinned at 99–100% of its cap in every
case, this pipeline still reads **1.725×** from 2 to 4 cores, and per-core
throughput falls: 186k at one core, 200k at two, 163k at four. That is not a
measurement artefact and no mechanism is offered for it here. The 2-core case
is also the noisy one — its spread was 20.5% in one arm and 10.9% in another,
against a 20% ceiling.
