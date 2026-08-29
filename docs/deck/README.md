# The deck

The live demo is the deliverable. These slides are the handout and the backup.

| file | for |
|---|---|
| [`final-demo.pptx`](final-demo.pptx) | **presenting.** Titles, tables and the diagram — nothing to read aloud, so attention stays on the live screen. |
| [`final-demo-handout.pptx`](final-demo-handout.pptx) | **leaving behind.** The same thirteen slides with a paragraph of prose on each, readable by someone who missed the demo — and the fallback if the live stack fails. |

Both carry speaker notes on every slide.

## Rebuilding

```bash
python3 -m venv .venv
.venv/bin/pip install -r scripts/deck-requirements.txt
.venv/bin/python scripts/build-deck.py
```

`scripts/build-deck.py` holds every figure **once** and renders it twice, so a
number cannot drift between the two files. It re-renders the pipeline diagram
first — [`pipeline.png`](pipeline.png), produced from `docs/design/pipeline.svg`
by `scripts/render-diagram.py` — so the deck cannot carry a stale one.

## Reviewing without PowerPoint

```bash
.venv/bin/python scripts/preview-deck.py > /tmp/deck-preview.html
```

Renders both `.pptx` files to one HTML page from their **real shape geometry** —
actual positions, sizes, fills, fonts, tables and pictures. It shows what the
files contain rather than what they were meant to. Browser text metrics are not
PowerPoint's, so it narrows the question of whether anything overflows its box
without settling it.

## What the slides carry

| # | Slide | |
|---|---|---|
| 1 | Title | What it is, what it runs on |
| 2 | One order becomes four allocations | The problem, and why the two aggregations differ |
| 3 | Six numbered elements, end to end | The pipeline diagram |
| 4 | What the numbers should be | Expected inputs, the demo's settings, sinks 3–6 — **before** running |
| 5 | **LIVE** | Start the generators, switch to Grafana |
| 6 | Every number has an answer | The three questions the dashboard invites |
| 7 | A replayed record is a wrong number | Exactly-once, proved by killing a task manager |
| 8 | Two numbers, and they are not the same | 59 ms of work vs 518 ms to a consumer |
| 9 | Both required cases pass | 100× orders, 20× prices, latency unmoved |
| 10 | Double the units, double the throughput | The demo pair: 2 → 4 units, 1.96× |
| 11 | *Backup* — the whole curve | Where it stops, and why the broker looks idle |
| 12 | *Backup* — laptop's ceiling or Flink's? | The AWS comparison, with its caveats stated |
| 13 | *Backup* — how it was built | Thirteen steps, CI, everything scripted |

Slides 11–13 are answers, not part of the flow.

Why the deck is shaped this way, and what its review changed:
[`docs/steps/step-13/deck.md`](../steps/step-13/deck.md).
