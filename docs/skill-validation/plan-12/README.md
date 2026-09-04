# Plan 12 — stable runs, a true 1→2 and 2→4, a harness with no open defects, and a shorter clock

Written 2026-09-04 after [run 11](../clean-room-run-11.md), before any of it
ran. Each phase below carries its **prompt** — objective and pass criterion,
written before launch — and its **response** — the verbatim captured output
and the verdict, written after. A phase does not start until the one before
it has passed and merged. One change per phase; every threshold change is
replayed against the recorded suites before it touches a stack.

The frame: a plan can make the ratio *stable and true*; it cannot make it
2.0. What it can do is remove every known source that moves the number and
fix the structural reason 1→2 has never read 2× — at parallelism 1 a
`keyBy` repartitions nothing, so the one-core case has always done
different work.

## Phase 1 — measure the rig before touching a threshold

Same harness, run 11's pipeline and build (`6ee9600b17e9ba41`), same caps,
10 s checkpoints, 200M backlog, stack up once for all three arms.

### 1A — noise floor

**Prompt.** The 4-core case, six passes, nothing changed between them. Pass
when six measurements exist, each either valid or refused with its guard
named. Output: spread (max−min)/mean, standard deviation, and the source-idle
band. Decision rule: this spread is the rig's own noise at a constant config
and becomes the unit every threshold and expected band is expressed in.

**Response.** Six valid passes, none refused ([raw](phase1/phase1a.json), [driver](phase1/phase1.py)). Stack up 05:29, fill 200M in 5.9 min, six passes 05:36–06:00.

| pass | rec/s | TM % of cap | src idle | src back-pressured | broker cores | window |
|---|---:|---:|---:|---:|---:|---:|
| n1 | 772,290 | 96.0% | 14.0% | 8.1% | 0.58 | 70 s / 8 |
| n2 | 791,042 | 95.9% | 11.7% | 7.0% | 0.59 | 70 s / 8 |
| n3 | 759,646 | 98.4% | 9.1% | 8.1% | 0.53 | 60 s / 6 |
| n4 | 798,902 | 99.8% | 11.4% | 6.6% | 0.56 | 60 s / 6 |
| n5 | 747,776 | 97.5% | 10.2% | 7.1% | 0.51 | 60 s / 6 |
| n6 | 720,658 | 99.7% | 8.5% | 12.2% | 0.49 | 70 s / 8 |

**Mean 765,052 rec/s; spread 10.2%; sd 3.8%; range 720,658–798,902.** Rate does not track cap use: the pass at 99.8% of cap is the fastest and the one at 99.7% the slowest. Source idle 8.5–14.0%, all under the 15% ceiling. **The rig's own noise at a constant config is a 10% spread over six passes, sd 3.8%.** Run 11's 12.2% four-core spread and its 1.87× against the live test's 2.02× are both inside this. Every expected band from here is expressed in it: a step ratio is "2.0" when it lies within ±2 sd ≈ ±8% → **1.85–2.15×**; a suite is stable when its per-case spread over three passes is under the 20% ceiling, which this floor confirms rather than moves (six passes at 10% → three passes will read less, not more).

Left unexplained, on purpose: n6 read 6% below the six-pass mean at 99.7% of cap with the highest back-pressure of the arm (12.2%). One pass is not a trend.

### 1B — host-load arm

**Prompt.** The 4-core case, three passes, with `df` / `docker exec` polling
every 5 s on the host during the window — run 11's refusal hypothesis, run as
the one variable. Pass when three measurements exist. Decision rule: if cap
use or rate moves by more than the 1A spread, the hypothesis is a measurement
and the host-quiet guard (phase 2.3) gets its threshold from this arm; if it
does not, the hypothesis is recorded as refuted and 2.3 is dropped.

**Response.** Three valid passes under continuous host polling (`df -g /`, two `docker exec`, `docker stats --no-stream`, every 5 s for the whole arm) ([raw](phase1/phase1b.json)).

| pass | rec/s | TM % of cap | src idle | src back-pressured | broker cores | window |
|---|---:|---:|---:|---:|---:|---:|
| h1 | 763,812 | 95.6% | 13.8% | 6.4% | 0.55 | 60 s / 6 |
| h2 | 799,790 | 98.9% | 12.4% | 5.9% | 0.58 | 60 s / 6 |
| h3 | 782,835 | 99.0% | 9.5% | 8.4% | 0.56 | 60 s / 7 |

**Mean 782,146 rec/s, range 763,812–799,790** — every pass inside arm A's 720,658–798,902, and h2 is the fastest pass of the day. Cap use 95.6–99.0%, inside arm A's 95.9–99.8%. **The hypothesis is refuted as stated:** host polling at this rate does not move throughput or cap use by more than the rig's own noise. Run 11's 2-core refusal at 96.4% has no measured cause; the polling stays a hypothesis-marked-false in its report. **Phase 2.3 (host-quiet guard) is dropped.** What arm B does not rule out: a heavier host load than four commands per 5 s.

### 1C — baseline structure

**Prompt.** One core at parallelism 1, one core at parallelism 2 (two slots),
and two cores at parallelism 2, three passes each, alternating. Pass when
nine measurements exist. Decision rule: if 1c/p=2 → 2c/p=2 reads 2.0 ± the 1A
spread and 1c/p=1 → 2c/p=2 does not, the one-core case is redefined at the
suite's parallelism (phase 2.5); if neither reads 2.0 ± spread, 1→2 stays
unreported and the reason is written down as unknown.

**Response.** Nine cases, seven valid, two refused by the 98% baseline cap floor ([raw](phase1/phase1c.json), [driver](phase1c.py)). `run_case` gained a `parallelism` argument for this arm (slots and job parallelism decoupled from the core count; the slots == parallelism guard reads back the override).

| case | pass | rec/s | TM % of cap | src idle | src back-pressured | GC % of cap | status |
|---|---|---:|---:|---:|---:|---:|---|
| 1c / p=1 | 1 | 175,286 | 99.9% | 0.2% | 23.2% | 5.7% | valid |
| 1c / p=2 | 1 | 117,909 | 99.7% | 1.9% | 23.7% | 6.2% | valid |
| 2c / p=2 | 1 | 406,455 | 99.6% | 14.6% | 13.7% | 3.8% | valid |
| 2c / p=2 | 2 | 385,684 | 99.8% | 14.1% | 14.4% | 2.9% | valid |
| 1c / p=2 | 2 | 114,298 | 99.8% | 1.9% | 25.6% | 6.4% | valid |
| 1c / p=1 | 2 | 195,414 | 95.3% | 0.2% | 23.9% | 7.5% | refused: task manager used 95.3% of its 1-core cap (floor 98%) |
| 1c / p=1 | 3 | 189,498 | 100.1% | 0.1% | 28.2% | 6.7% | valid |
| 1c / p=2 | 3 | 113,482 | 94.2% | 2.3% | 20.7% | 7.0% | refused: task manager used 94.2% of its 1-core cap (floor 98%) |
| 2c / p=2 | 3 | 390,475 | 100.1% | 14.4% | 13.3% | 3.3% | valid |

| step | all passes | valid passes only |
|---|---:|---:|
| 1c/p=1 → 2c/p=2 | **2.11×** | 2.16× |
| 1c/p=2 → 2c/p=2 | 3.42× | 3.40× |

**The premise of the plan was wrong, and the measurement says so.** Two slots on one core is not the comparable baseline — it is a *weaker* one, a third slower than one slot at the same 100% of cap, and the step from it reads 3.4×, exactly the superlinear artefact the tiny proof exists to catch. The plain one-core case at parallelism 1 steps to two cores at **2.11×, inside 2.0 ± 8%** (the 1A band). Whatever `keyBy` does at parallelism 1 in this pipeline, it does not make the one-core case a soft baseline here.

Decision, by the rule written above: **the one-core case keeps its definition; phase 2.5 is dropped; 1→2 is reportable** with the standard case. Noted, not explained: the one-core case is the noisiest (10.8% over three passes, and its fastest pass was the refused one at 95.3% of cap), its GC fraction is twice the two-core case's, and its source idles at 0% while the two-core source idles at 14%.

Arm A and arm C together, as a replay fixture ([`record/plan12-phase1.json`](../../../.claude/skills/prove-it-scales/harness/record/plan12-phase1.json)): 1→2 = 2.11×, 2→4 = 1.94× on the same build, rig and day, arms back to back rather than interleaved. Replay: 14 suites, OK.

### Phase 1 verdict

| arm | passes | criterion | result |
|---|---:|---|---|
| A | 6/6 valid | six measurements exist | **PASS** — noise floor 10.2% spread, sd 3.8% |
| B | 3/3 valid | three measurements exist | **PASS** — hypothesis refuted; 2.3 dropped |
| C | 7/9 valid, 2 refused with the guard named | nine measurements exist | **PASS** — baseline kept; 2.5 dropped |

Stack down at 06:50, nothing with the prefix survived, fstrim returned 52.5 GiB. Stack time 05:29–06:50, 1 h 21 m for 18 measured cases.

## Phase 2 — harness fixes, one PR each, replayed first

| # | change | closes | pass criterion |
|---|---|---|---|
| 2.1 | `kafkaCapFrac` against the step cap in `ceiling`; refusal records complete | run 11 cosmetic defect | pure self-test passes; replay OK; run 11's ceiling.json re-rendered shows 0.88 at k0.6 |
| 2.2 | sentinel: the suite runs the baseline case first and again last; drift beyond the 1A floor voids the suite | 801k after 776k; run 10's falling third passes | self-test fires on a synthetic drift; replay refuses none of the 13 recorded suites |
| ~~2.3~~ | ~~host-quiet guard~~ | **dropped by 1B**: host polling did not move the number | — |
| 2.4 | `down` kills any host process holding the results directory and asserts none survive | the one rule broken on every run it appears in | live: a planted watcher shell is found and killed; `down` refuses if one survives |
| ~~2.5~~ | ~~one-core case at the suite's parallelism~~ | **dropped by 1C**: the plain one-core case steps at 2.11×; two slots on one core is the weaker baseline | — |
| 2.6 | sink-size projection in the tiny proof: bytes/output × suite backlog × outputsPerInput vs free disk, refused before the suite fill | run 11's rebuild, 37 min of gates run twice | self-test fires on a synthetic 607 B record; run 11's build-A shape would have been refused at minute 20 |

## Phase 3 — run time

| phase | run 11 | target | how |
|---|---:|---:|---|
| build + preflight | 16 | 16 | no waste |
| gates | 37 | 18 | once (2.6); completeness and tiny proof share one stack session and one fill |
| fill | 5 | 5 | already sized from the formula |
| suite | 23 | ~26 | +1 sentinel case |
| ceiling | 12 | 0 | optional stays optional |
| agent latency | 20 | ~2 | `prove.py all`: one command, preflight → gates → fill → suite → report, a done-file at the end |

Pass criterion: the whole chain driven by hand on run 11's pipeline in ≤ 1.25 h
from `preflight` to `report`, with the suite's own thresholds untouched.

## Phase 4 — run 12

Criteria, written before launch:

- uses the harness verbatim; one suite; ≤ 1.25 h (required)
- 1→2 and 2→4 each inside 2.0 ± the 1A floor (the band comes from 1A, not
  from a guess)
- sentinel drift under the floor; the host-quiet guard never fires
