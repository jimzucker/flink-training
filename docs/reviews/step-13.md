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

### Round 4 — a documentation audit

> Check that all docs are now correct and aligned

Five misalignments, four of them predating this step:

**`README.md` quoted the specification's window in the expected-output table.**
Sinks 5 and 6 were listed at "4 / min" and "16 / min", but `verify-topics.sh`
runs at `WINDOW_MS=10000` — so the table promised rates that neither the demo nor
the verification produces. Now stated per window, with the 10s/60s split spelled
out. This is the same error the deck had, in the doc the deck was built from.

**`README.md` described a stale `scale-units.sh`.** It said the script "sets 1,
2, 4 and 8"; the default is now 2 and 4.

**`docs/steps/step-04/exactly-once.md` stated the 5-second checkpoint interval as
current.** It was current at step 04 and step 09 lowered the default to one
second. Marked as superseded, with the current figure alongside.

**`docs/design/pipeline-design.md` gave the one-minute rates without noting the
override.** Correct as a specification document, but it is where the deck's
diagram comes from, so it now says which number you will actually watch.

**The deck disagreed with itself on one slide.** Slide 8 carried "518 ms" in the
consumer-wait table and "497 ms" in the interval-comparison table beside it. Both
are real measurements from step 09 — the first taken after one second became the
default, the second from the interval experiment — but presented side by side
they read as two answers to one question. The slide now uses 518 ms / 1 025 ms in
both, matching the README.

`scripts/preview-deck.py` was also undocumented despite being committed; it is
now described in the step doc.

### Round 5 — eight slides, ending on the result

> Remove slides 11/12/13 we stop showing the objective of 2x scaling, slide 4
> looks wrong, remove slides 7 and 8

**Cut to eight.** Slides 11, 12 and 13 were backup — the full curve and its
ceiling, the AWS comparison, and how the project was built. A backup slide that
follows the conclusion undercuts it, because the last thing shown is the thing
remembered: the deck was ending on "here is where it stops working" rather than
on 1.96×. Slides 7 and 8, exactly-once and latency, went too — both are better
demonstrated on the running dashboard than read off a table.

Nothing was deleted from the repo. `docs/steps/step-04`, `step-09` and `step-12`
still hold the evidence, and the runbook still has the answers ready.

**Slide 4 was wrong in three ways**, and the diagnosis is worth recording because
the cause was a previous fix:

| | |
|---|---|
| ragged bottoms | the inputs table ran to 5.15" while the sinks table stopped at 3.95" — a 1.2" hole |
| a muddled table | "Input / setting" covered inputs, reference data *and* settings in one column |
| a stranded caption | floating at 5.90", below both tables and attached to neither |

The second caused the first. Round 2 added the window and checkpoint rows to fix
a real error, but bolted them onto a table that was already an inputs table — so
the header became a slash-compound to cover both, and the table grew two rows
past its neighbour. The settings now have their own full-width strip beneath both
tables, the inputs table is inputs again, and the two tables end within half an
inch of each other.

### Round 6 — order, and a chart for the 2x

> Witch slides 2 and 3, move 8 before 6, as a slide with relevant graphs showing
> 2x scaling clearly
>
> Keep the current scaling slide add one with graphs

**Design before problem.** The diagram makes the problem legible; the other order
asks the audience to hold an abstraction until the picture arrives.

**Scaling moved up behind the live demo**, while the pipeline is still on screen,
instead of trailing the deck.

**Two scaling slides, not one.** The table keeps the cores and broker columns —
which is what makes the result attributable rather than asserted — and a new
chart puts the same numbers against a dashed line at exactly twice the two-unit
result. The claim is whether the second bar reaches that line, so the line is
drawn rather than described.

The bars are drawn from the values with no baseline offset: their height ratio is
**1.964**, identical to the throughput ratio. A chart that argues for 2× must not
flatter itself with a truncated axis, and this one is checked rather than assumed
— the build verifies the two heights against the two numbers.

Nine slides: title, design, problem, expected output, live, scaling, scaling
chart, explain-any-number, load.

### Round 7 — one configuration, not two

> Why does slide 4 say 1 min specified? Remove notes like that just focus on how
> we agreed to run the demo

Round 2 fixed a real error — the deck had been promising rates the demo does not
produce — but fixed it by carrying *both* numbers, so every settings line became
an argument with itself. The audience does not need to be told which of two
values is not in force.

The deck now states one configuration: window 10 s, checkpoint interval 1 s, 8
partitions. Every "(specified: 1 min)", "the job defaults to the specified
minute" and the matching speaker notes are gone, from both files.

That exposed a dependency worth recording. The pipeline diagram has "Window 1
min" drawn into it, so removing the caption would have left slide 2 contradicting
slide 4. `scripts/render-diagram.py` now relabels the deck's copy — "Window 1
min" to "Window 10s", "once per minute" to "once per window" — and fails loudly
if either string stops appearing in the source, so a diagram edit cannot silently
break the substitution. `docs/design/pipeline.svg` is untouched and still states
the specification, which is what a design document is for.

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
