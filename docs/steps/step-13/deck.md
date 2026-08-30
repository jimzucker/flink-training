# Step 13 — the final deck

**Bottom line:** the live demo is the deliverable; the slides are the handout and
the backup. Two decks are generated from one content model so a number cannot
drift between them.

The files live in [`docs/deck/`](../../deck/) — they are a deliverable, not
per-step evidence, so they sit where someone looking for the deck would look.

| file | for |
|---|---|
| `docs/deck/final-demo.pptx` | **presenting.** Titles, tables and the diagram. Nothing to read aloud, so attention stays on the live screen. |
| `docs/deck/final-demo-handout.pptx` | **leaving behind.** The same nine slides with a paragraph of prose on each, readable by someone who missed the demo — and the fallback if the live stack fails. |

Both carry speaker notes on every slide.

## Building them

```bash
python3 -m venv .venv && .venv/bin/pip install python-pptx
.venv/bin/python scripts/build-deck.py
```

`scripts/build-deck.py` holds the content once and renders it twice. Every figure
in it is quoted from a doc in this repo, with a pointer to which — so updating a
measurement means changing it in one place and rebuilding, rather than hunting
for it across two binary files.

The pipeline diagram is **the project's own** `docs/design/pipeline.svg`, rendered
by `scripts/render-diagram.py` and embedded on slide 3. An earlier version drew a
simplified pipeline in native PowerPoint shapes; it was not good enough, and there
is no reason for the deck to have a second, worse diagram when the real one exists.

Rendering it needs one repair. The arrowheads are applied through a CSS class —
`.edge { marker-end:url(#arrow) }` — and `svglib` does not resolve CSS-applied
markers, so every edge came out as a plain line. The renderer bakes an explicit
polygon onto the end of each edge first, sized and positioned from the `<marker>`
definition rather than by eye, and the edges are all orthogonal so the direction
of the final segment is unambiguous. Then `svglib` → PDF → `sips` → PNG → trim,
which needs no native cairo.

`scripts/build-deck.py` re-runs the diagram render on every build, so the deck
cannot carry a stale copy of it.

## Seeing them without PowerPoint

`scripts/preview-deck.py` renders the generated `.pptx` files to a single HTML
page by reading their **real shape geometry** — every position, size, fill, font,
table cell and embedded picture, laid out at its actual coordinates. It shows what
the files contain rather than what they were meant to contain, which is the point:
it caught a build where an edit silently failed to apply and a slide still carried
the old text.

```bash
.venv/bin/python scripts/preview-deck.py > /tmp/deck-preview.html
```

Browser text metrics are not PowerPoint's, so this narrows the question of whether
anything overflows its box but does not settle it.

## The nine slides

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

Slide 4 is the one that makes the demo a verification rather than a tour: the
numbers are committed to out loud before anything starts.

## What the deck deliberately does not do

**The design comes before the problem.** The diagram makes the problem legible —
once someone has seen one order fan out to four allocations on the picture, the
sentence explaining it lands. The other order asks them to hold an abstraction
until the diagram arrives.

**Scaling sits right after the live demo**, while the pipeline is still on screen
and the audience still has the shape of it, rather than trailing the deck. It gets
two slides: the numbers with their cores and broker columns, then the same result
as a chart with a perfect 2× drawn as a dashed line. The bars are exactly
proportional to throughput — their height ratio is 1.964, the same as the data —
so the chart cannot flatter the result with a truncated axis.

**It ends on the scaling result.** The deck closes on 2 → 4 units at 1.96× rather
than continuing into the curve's ceiling, the AWS comparison, and how the project
was built. Those three were backup slides, and a backup slide that follows the
conclusion undercuts it — the last thing shown is the thing remembered.

**It does not describe the guarantees it can demonstrate.** Exactly-once and
latency each had a slide; both are better shown on the running dashboard than
read off a table, and the runbook carries the numbers for anyone who asks. The
evidence stays in `docs/steps/step-04` and `step-09`.

**It shows one configuration: the one the demo runs.** Window 10 s, checkpoint
interval 1 s, 8 partitions. Earlier drafts carried the specified one-minute window
alongside as a parenthetical, which made every settings line an argument with
itself — the audience does not need to be told which of two numbers is not in
force. The pipeline diagram is relabelled for the deck by
`scripts/render-diagram.py` for the same reason, so slide 2 and slide 4 agree.
`docs/design/pipeline.svg` still states the specification, because that is what a
design document is for.

**It does not include a screenshot of Grafana.** The dashboard is the live
artefact; a picture of it invites the audience to read a still image instead of
watching the real one.
