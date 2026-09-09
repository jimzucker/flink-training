# Clean-room validation, run 25 — the harness was broken when it started

A plain user prompt on the full current harness. It is recorded mainly for what
it says about the harness, not the pipeline: **`prove.py all` was unusable when
the agent began**, because of a defect merged an hour earlier.

## What the agent hit

`host_ceiling()`, added in #78, assigned into `out` — a name belonging to a
different function. Preflight needs Docker, so neither the pure self-test nor
the replay ever runs it, and the `NameError` fired for any pipeline after 17 of
18 preflight rows had passed. The chain failed at preflight in 1.7 min.

The agent then ran the harness's own commands step by step — its documented
mode — without editing or reimplementing anything, and reported both the defect
and the fact that **`prove.py` changed on disk at 20:56:21 mid-chain, not by
it**. That was the fix being synced. It also checked `lib.py`'s mtime to
establish that no threshold or guard had moved. That is exactly the right
conduct and it is worth more than the numbers below.

Fixed in [#79](https://github.com/jimzucker/flink-training/pull/79), which also
added `namecheck.py` to `replay`: Python's own scope analysis over the harness,
in milliseconds. Against the broken revision it reports
`host_ceiling() reads undefined name 'out'`.

## The measurement

| step | ratio | interval | of linear (low) | claim |
|---|---:|---|---:|---|
| 1→2 | 2.346 | [2.246, 2.263] | 112% | met — **and not believed** |
| 2→4 | 1.764 | [1.683, 1.846] | 84% | **missed** |

| cores | records/s | passes | spread | % of cap | GC |
|---:|---:|---:|---:|---:|---:|
| 1 | 220,719 | 3 | **14.3%** | 98.9% | **15.5%** |
| 2 | 517,823 | 2 | 4.5% | 98.4% | 6.1% |
| 4 | 913,312 | 2 | 0.2% | 96.0% | 2.8% |

913,312 rec/s at four cores — 4.57M sink records/s — is the highest on record.
The 1-core case is the problem: 14.3% spread, 15.5% GC, and a **sentinel drift
of −14.5%**, which is what makes 1→2 read 2.346. The agent said plainly it did
not believe that figure, which is the correct response to its own baseline
moving 14% across the suite.

## The host probe, now in preflight

```
        1→2     2→4
alu     97.4%   97.8%
mem     91.7%   77.0%
```

2→4 = 88% of linear sits between those bounds. Context, not a mechanism, and
the agent said so.

## Measured, not explained

- Why 2→4 returns 88%: broker at 0.61 of 2.5 cores, source idle 1.9%, GC
  *falling* across cases (15.5 → 6.1 → 2.8%), no memory pressure.
- Two tiny proofs refused the 4-core case at 92.7% and 92.9% of cap while a
  standalone probe of the same build read 96.3% — 3.6 points of wander with no
  mechanism offered.
- The agent changed one thing (sink payload ~110 B JSON → ~30 B pipe-delimited,
  cutting roughly two thirds of ~390 MB/s of broker writes) and measured both
  arms: 788,614 rec/s at 92.7/92.9% of cap against 920,060 at 98.8%. It states
  in its own report that this is two builds rather than one variable on one
  build, and claims no causation.
