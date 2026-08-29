# Step 13 — the final deck

**Bottom line:** the live demo is the deliverable; the slides are the handout and
the backup. Two decks are generated from one content model so a number cannot
drift between them.

| file | for |
|---|---|
| `final-demo.pptx` | **presenting.** Titles, tables and the diagram. Nothing to read aloud, so attention stays on the live screen. |
| `final-demo-handout.pptx` | **leaving behind.** The same thirteen slides with a paragraph of prose on each, readable by someone who missed the demo — and the fallback if the live stack fails. |

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

## The thirteen slides

| # | Slide | Carries |
|---|---|---|
| 1 | Title | What it is, what it runs on |
| 2 | One order becomes four allocations | The problem, and why the two aggregations differ |
| 3 | Six numbered elements, end to end | The project's pipeline diagram, with a note that its 1-min window is the specification |
| 4 | What the numbers should be | Expected inputs, the demo's window and checkpoint settings, and sinks 3–6 — **before** running |
| 5 | **LIVE** | Start the generators, switch to Grafana |
| 6 | Every number has an answer | The three questions the dashboard invites |
| 7 | A replayed record is a wrong number | Exactly-once, proved by killing a task manager |
| 8 | Two numbers, and they are not the same | 59 ms of work vs 518 ms to a consumer |
| 9 | Both required cases pass | 100× orders, 20× prices, latency unmoved |
| 10 | Double the units, double the throughput | The demo pair: 2 → 4 units, 1.96× |
| 11 | *Backup* — the whole curve | Where it stops, and why the broker looks idle |
| 12 | *Backup* — laptop's ceiling or Flink's? | The AWS comparison, with its caveats stated |
| 13 | *Backup* — how it was built | Thirteen steps, CI, everything scripted |

Slides 11–13 are answers, not part of the flow. Slide 4 is the one that makes the
demo a verification rather than a tour: the numbers are committed to out loud
before anything starts.

## What the deck deliberately does not do

**It does not lead with the scaling curve.** The demo pair is two cases — 2 and 4
units — because each case costs minutes of warm-up that is dead air, and 2 → 4 is
the sharpest form of the claim at 1.96×. The full four-point curve and its ceiling
are a backup slide.

**It does not oversell the AWS run.** Slide 12 states in its own text that the run
was a confirmation rather than a second demo, that its step ratios behave oddly in
a way that is not yet explained, and that its two highest points were measured
once each.

**It quotes the demo's settings, not the specification's.** The window is 10 s in
the demo and the checkpoint interval is 1 s, so the market value sinks emit 4 and
16 records *per window* rather than the 4/min and 16/min a 60-second window would
give. Slide 4 states both the setting and the reason, because a slide that
promises numbers the audience will not see defeats the purpose of committing to
them in advance.

**It does not include a screenshot of Grafana.** The dashboard is the live
artefact; a picture of it invites the audience to read a still image instead of
watching the real one.
