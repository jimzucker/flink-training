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
