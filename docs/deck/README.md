# The deck

The live demo is the deliverable. These slides are the handout and the backup.

| file | for |
|---|---|
| [`final-demo.pptx`](final-demo.pptx) | **presenting.** Titles, tables and the diagram — nothing to read aloud, so attention stays on the live screen. |
| [`final-demo-handout.pptx`](final-demo-handout.pptx) | **leaving behind.** The same nine slides with a paragraph of prose on each, readable by someone who missed the demo — and the fallback if the live stack fails. |

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
| 2 | Six numbered elements, end to end | The pipeline diagram |
| 3 | One order becomes four allocations | The problem, and why the two aggregations differ |
| 4 | What the numbers should be | Inputs, sinks 3–6, and the demo's settings — **before** running |
| 5 | **LIVE** | Start the generators, switch to Grafana |
| 6 | Double the units, double the throughput | The numbers: 2 → 4 units, **1.96×**, with the cores and broker columns |
| 7 | Two units, then four — against a perfect 2× | The same result as a chart, with the ideal drawn |
| 8 | Every number has an answer | The three questions the dashboard invites |
| 9 | Both required cases pass | 100× orders, 20× prices, latency unmoved |

**Eight slides, ending on the scaling result.** Exactly-once and latency were cut
along with the three backup slides: the guarantees are demonstrated live rather
than described, and the deck closes on 1.96× rather than trailing off into the
curve's ceiling and its caveats. The evidence for everything cut is still in the
repo — `docs/steps/step-04`, `step-09` and `step-12` — and the runbook has the
answers ready if anyone asks.

Why the deck is shaped this way, and what its review changed:
[`docs/steps/step-13/deck.md`](../steps/step-13/deck.md).
