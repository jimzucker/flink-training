# Step 13 — final deck

## Round 1

### Feedback

> Let's merge the aws step and make the demo deck its own step

Then, on the two questions the step could not settle from the repo:

> **Deck format:** PowerPoint (.pptx)
> **Depth:** Do both versions

### What that decided

PowerPoint rather than a shareable link or Markdown, because the deck has to
survive being handed in and opened by someone who will not click a link — and
because the assignment itself arrived as a `.pptx`.

Both depths rather than one, which is why there are two files. The presenter's
spine and the standalone handout are the same thirteen slides; the handout adds a
paragraph to each. Generating both from `scripts/build-deck.py` means the numbers
cannot disagree between them.

#### Round 2 — the deck quoted the specification, not the demo

> I thought we where emitting mv every 10s and reduced checkpoint ?

Correct, and the deck was wrong in three places. `WINDOW_MS` defaults to `10000`
in `compose.yml` and in `verify-topics.sh`, and `DEFAULT_CHECKPOINT_INTERVAL_MS`
is `1_000` — so the demo runs a **10-second window** and a **1-second
checkpoint**, while the deck had carried the specification's 60-second window
through to the observed rates.

| slide | was | now |
|---|---|---|
| 2 | "emitted once per minute" | "once per key per window" |
| 3 | diagram: "1-min window", sinks "4/min" and "16/min" | "10s window", "4 per window", "16 per window" |
| 4 | "4 / min", "16 / min" | "4 per 10 s", "16 per 10 s", plus the window and checkpoint settings as their own rows |
| 8 | "1 s" | "1 s (the default)" |

This mattered more than a wording slip. Slide 4's whole job is to commit to the
numbers before anything runs, so that the dashboard either matches or it does
not — and it was promising rates the audience would never see.

The README had it right the whole time: its settings table already recorded
`WINDOW_MS` as *10000 locally, 60000 in the job* and `CHECKPOINT_INTERVAL_MS` as
`1000`. The deck was the only place the two had been conflated.

### Round 3 — use the project's diagram

> The native drawn flow diagram is not good, can you use the one from project

Right, and there was no good reason for the deck to carry a second, worse diagram
when `docs/design/pipeline.svg` already existed. Slide 3 now embeds it.

Getting it in took one repair. The arrowheads are applied through a CSS class —
`.edge { marker-end:url(#arrow) }` — and `svglib` does not resolve CSS-applied
markers, so the first render came out with every edge a plain line. Two other
routes were tried and rejected: PowerPoint's own AppleScript export times out on
this machine, and `qlmanage` keeps the arrowheads but forces a square canvas and
clips the right half of the diagram.

`scripts/render-diagram.py` bakes an explicit polygon onto the end of each of the
fifteen edges, sized and positioned from the `<marker>` definition rather than by
eye — the edges are all orthogonal `M`/`H`/`V` paths, so the direction of the
final segment is unambiguous. Then `svglib` → PDF → `sips` → PNG → trim, with no
native cairo anywhere in the path.

`build-deck.py` re-runs that render on every build, so the deck cannot carry a
stale diagram.

One consequence worth naming: the diagram says "Window 1 min", which is the
specification, while slide 4 says the demo runs a 10-second window. Slide 3 now
carries a caption saying so, and the speaker note repeats it, rather than leaving
two slides that appear to disagree.

## Still open

**Text fitting is unverified.** There is no renderer on this machine — no
LibreOffice, no SVG converter — so the decks were checked structurally (slide
count, shape bounds, notes present) but nobody has looked at them. A paragraph
that overflows its box would not have been caught. One pass in PowerPoint is
needed before this is presented.

**No Grafana screenshot.** Deliberate, on the grounds that a still image invites
the audience to read it instead of watching the live dashboard. If the live stack
is judged too risky to depend on, the handout deck is the fallback and it would
need one.
