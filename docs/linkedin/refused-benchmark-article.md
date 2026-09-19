# A Claude skill that refuses to publish a bad benchmark

Draft. The short pitch for the prove-it-scales skill, told through the 2 KB
payload experiment. Every figure comes from
[`docs/skill-validation/payload-2k.md`](../skill-validation/payload-2k.md) and
the result files under `docs/skill-validation/payload-2k/`; the refusal text is
from `.claude/skills/prove-it-scales/harness/prove.py`.

A longer case-study version was cut on 2026-09-18 — the author asked for a
short pitch instead. A manager-facing post to sit above this is drafted but not
yet filed.

---

Every scaling claim rests on a measurement, and most measurements have no way to say "this run was invalid." So a bad run and a bad system look identical.

**prove-it-scales** is a Claude Code skill that builds a data pipeline and then measures whether it scales, under rules strict enough that a compromised measurement can't reach the table. When it can't trust the number, it refuses and says why.

Here's it working.

I made the messages in a pipeline six times bigger to see what that cost. The result came back: four times the CPU, **1.56×** the throughput. Both measurements were individually clean — the worker pinned at 100% of its cap, garbage collection negligible, two independent record counts agreeing within 1%. Every check that asks *is this case trustworthy* said yes.

The harness refused it anyway, because the ratio fell outside the range it will accept:

```
STOPPING: ratio outside bounds.
```

That refusal was right. The tick data showed the 4-core rate collapsing mid-run — 69,517 orders/s down to 7,183 — and the measurement window had landed inside the collapse. A real number, honestly recorded, that wasn't a ceiling.

Same rig, same build, next day, with the original message size beside it as a control:

| | 1 core | 4 cores | 1→4 |
|---|---:|---:|---:|
| 2 KB message, day 1 | 24,536 | 38,175 | **1.556 — refused** |
| 2 KB message, day 2 | 22,733 | 103,908 | **4.571** |
| 326 B control, day 2 | 68,203 | 294,691 | **4.321** |

It scales. The answer that said otherwise lived for one day inside a system that wouldn't print it.

**I still can't prove what went wrong on day one.** The likely cause is host memory pressure, but nothing recorded it at the time — so it's written into the record as a hypothesis, sitting next to the numbers it would explain.

What the good measurement bought was the real finding: the bigger messages cost 3× per message but move 2× more data per second, and **68% of that cost is JSON parsing — the job was parsing every message twice.** Worth fixing, and invisible until the number could be trusted.

The skill ships with the harness that does the refusing: 38 guards, each one added because a run paid for it. It targets Flink on Kafka today.

Free and open, with all the data above: https://github.com/jimzucker/flink-training

---

## The figures, and where they come from

| figure | source |
|---|---|
| 1.556, refused; 4.571 and 4.321, passed | the three `tinyproof.json` files under `payload-2k/` |
| worker at 100.2% / 99.9% of cap, GC 1.4% / 0.3%, vantage 0.96% / 0.33% | `results-2026-09-14-f32/tinyproof.json`, and the run's `harness.log` |
| both day-1 cases recorded `status: OK` | same file — the refusal was the ratio bound, not a case check |
| bounds 3.00–5.00 for a 1→4 tiny proof | `tinyRatioLo` 1.5 / `tinyRatioHi` 2.5 in `harness/lib.py`, times the ideal ratio ÷ 2 |
| 69,517 → 7,183 orders/s | consecutive 5 s committed-offset ticks, day-1 4-core case |
| 38 guards | the guard self-test, which broke every one on purpose and checked each fired, on both passing runs |
| 3× per message, 2× more bytes/s | 68,203 → 22,733 orders/s at 1 core; 22,268 → 46,250 KB/s |
| 68% of the added cost is parsing | `payload-2k/bench/stages.log`, 13.49 of 19.73 µs added per order |

Two claims in the post are deliberately not tightened, because the record does
not support more: host memory pressure is a hypothesis, not a measured cause;
and the stage timings are not a profile of the running job — about 10 µs per
order of the pipeline's added cost is unexplained.
