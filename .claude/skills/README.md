# Skills

This repository no longer carries a copy of the skill it produced.

**`scalable-flink-skill` lives in its own repository:**
[github.com/jimzucker/scalable-flink-skill](https://github.com/jimzucker/scalable-flink-skill).
Install it once and it is available everywhere, including here:

```bash
git clone https://github.com/jimzucker/scalable-flink-skill ~/.claude/skills/scalable-flink-skill
```

It interviews you one question at a time about a pipeline you want to build and
prove scales, then enforces the measurement discipline that makes the resulting
numbers defensible. It was written from what this project got wrong the first
time, and [validated against it](../../docs/skill-validation/) 30 times.

## Why it is not vendored here

It was, until 2026-09-19. Two copies meant every threshold change had to be
synced or the two would drift — and the harness's replay fixtures do change
when a guard is revised. One source of truth is worth more than the
convenience of a repository-local copy, now that a global install serves the
same purpose.

What stayed behind is the evidence, not the code:
[`docs/runs/harness-record/`](../../docs/runs/harness-record/) holds the twelve
recorded suites from runs 9 and 10 and plan 12 that
[`scripts/build-runs-ledger.py`](../../scripts/build-runs-ledger.py) reads for
82 of the ledger's rows. They are measurements this project made, so they
belong with its other measurements.
