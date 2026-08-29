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

### Still open

**Text fitting is unverified.** There is no renderer on this machine — no
LibreOffice, no SVG converter — so the decks were checked structurally (slide
count, shape bounds, notes present) but nobody has looked at them. A paragraph
that overflows its box would not have been caught. One pass in PowerPoint is
needed before this is presented.

**No Grafana screenshot.** Deliberate, on the grounds that a still image invites
the audience to read it instead of watching the live dashboard. If the live stack
is judged too risky to depend on, the handout deck is the fallback and it would
need one.
