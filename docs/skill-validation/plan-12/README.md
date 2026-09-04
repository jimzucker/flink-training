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

**Response.** _pending_

### 1B — host-load arm

**Prompt.** The 4-core case, three passes, with `df` / `docker exec` polling
every 5 s on the host during the window — run 11's refusal hypothesis, run as
the one variable. Pass when three measurements exist. Decision rule: if cap
use or rate moves by more than the 1A spread, the hypothesis is a measurement
and the host-quiet guard (phase 2.3) gets its threshold from this arm; if it
does not, the hypothesis is recorded as refuted and 2.3 is dropped.

**Response.** _pending_

### 1C — baseline structure

**Prompt.** One core at parallelism 1, one core at parallelism 2 (two slots),
and two cores at parallelism 2, three passes each, alternating. Pass when
nine measurements exist. Decision rule: if 1c/p=2 → 2c/p=2 reads 2.0 ± the 1A
spread and 1c/p=1 → 2c/p=2 does not, the one-core case is redefined at the
suite's parallelism (phase 2.5); if neither reads 2.0 ± spread, 1→2 stays
unreported and the reason is written down as unknown.

**Response.** _pending_

## Phase 2 — harness fixes, one PR each, replayed first

| # | change | closes | pass criterion |
|---|---|---|---|
| 2.1 | `kafkaCapFrac` against the step cap in `ceiling`; refusal records complete | run 11 cosmetic defect | pure self-test passes; replay OK; run 11's ceiling.json re-rendered shows 0.88 at k0.6 |
| 2.2 | sentinel: the suite runs the baseline case first and again last; drift beyond the 1A floor voids the suite | 801k after 776k; run 10's falling third passes | self-test fires on a synthetic drift; replay refuses none of the 14 recorded suites |
| 2.3 | host-quiet guard: CPU outside the Docker VM sampled through each window; above the 1B threshold the case is marked "host noise" | the polling hypothesis; §6's watcher rule | self-test fires on a synthetic load; replay OK |
| 2.4 | `down` kills any host process holding the results directory and asserts none survive | the one rule broken on every run it appears in | live: a planted watcher shell is found and killed; `down` refuses if one survives |
| 2.5 | one-core case at the suite's parallelism, if 1C says so; SKILL.md §3 updated | 1→2 | self-test asserts slots == parallelism at every case; replay OK |
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
