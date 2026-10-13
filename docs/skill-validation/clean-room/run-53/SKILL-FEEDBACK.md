# Skill feedback — clean-room run st53

Skill commit `ada7890` (`~/.claude/skills/scalable-flink-skill`, level with
`origin/main` at the start). Each item quotes the wording that cost time or
was wrong, and says what it should say instead. Written as the run went.

## F1. The README check's self-test fails for a pipeline whose operator name contains its input topic's name

`prove.py selftest-pure` reported 1 of 321 guards not OK:
`README: a missing topic is named in the message — DID NOT FIRE`.
The self-test drops the first name (`topics.in`, here `orders`) from a
generated README and expects the check to complain. It did not, because my
operator `orders-source` was still in the text and the check is a substring
search (`lib.readme_missing`: `n.lower() not in low`). So the guard's own
self-test is a function of the pipeline's naming, and a real README that
never mentioned the `orders` topic would pass as long as an operator called
`orders-source` was named. The same hole covers `prices` / `prices-source` /
`parse-prices`.

- Cost: one rebuild and one rename (`orders-source` → `order-source`), about 5 minutes, and finding it needed reading `prove.py` because the self-test line does not say why it did not fire.
- Should be: match whole names (word boundaries, or match backticked names), and have the self-test build its README from names that cannot contain each other. Until then, `harness/README.md`'s `design` row should say: *"Do not give an operator a name that contains a topic's name: the README check is a substring search."*

## F2. `harness/README.md` and `SKILL.md` §9 disagree on the axis wording

`SKILL.md` §9: *"axis — always one machine, more cores"*. The shipped
`pipeline.example.json` has `"axis": "one worker growing: one task manager
container capped at N cores, parallelism N, N slots"`. Minor, but the example
is the thing people read. Should be: the example uses §9's words.

## F3. The shipped example verifier ignores `{arm}`

`harness/README.md` (verifier row) says the clean and killed arms want
**different** assertions and passes `{arm}`; `pipeline.example.json`'s
`verifier.cmd` does not pass `{arm}`. The one example of a verifier command
therefore teaches the thing the row warns against. Should be: add
`--arm={arm}` to the example.

## F4. "Never goes backwards" does not say *what* value, and a position can go down

§4: *"each key's published values never go backwards"*. A position goes up
on a buy and down on a sell, so a published position going down is correct.
I had to decide that the monotonic thing is a per-key count of orders applied,
carried in every output record. §5's SQL table hints at the same (*"a
`COUNT(*)` column in the aggregate"*) but only for SQL. Should be, in §4:
*"…never go backwards — judged on something monotonic per key, such as a count
of inputs applied, carried in each output record. The value itself (a
position) may legitimately go down."*

## F5. The duplicate-input requirement gives no design for dropping repeats at scale

§4: *"the input repeats a small share of its records on purpose … every total
still matches the manifest, which counts each record once"*. The pipeline has
to drop repeats, and the obvious design (remember every order id) is unbounded
state for a 250,000,000-record backlog. Nothing says how earlier runs did it.
I used ids that increase within a partition and kept, per key, the highest id
applied from each input partition (8 longs). Should be: one sentence giving a
bounded way to do it, and saying that the generator has to make it possible
(where the repeat is written, and what the id means).

## F6. Flink's Prometheus reporter rewrites operator names, and §7 does not say so

§7: *"Match operators by the names your job gives them (`operator_name=~".*parse.*"`)"*.
The names arrive rewritten: `order-source` is `Source:_order_source`,
`position-mv-by-symbol` is `position_mv_by_symbol` (read back from Prometheus
on this run). A panel written as `operator_name=~"position-mv-by-.*"` returns
nothing, and the chain then stops after completeness for an empty panel. I
caught it before the chain by querying Prometheus. Should be: *"The reporter
replaces every character outside letters, digits, `:` and `_` with `_`, so
`position-mv-by-symbol` is matched by `position_mv_by_symbol` (or by `.` in
the regex)."*

## F7. Completeness waits for a quiet machine, though it measures nothing

§6's busy-machine row lists *"each completeness arm"* among the cases that
wait. Completeness is a correctness check; a busy host changes how long it
takes, not whether it passes. On this run the standalone completeness waited
3 minutes for `mediaanalysisd` (3.6 cores, the post-upgrade Photos analysis);
with `busyWaitS` at 10 minutes a longer burst would have stopped the chain on
a step that cannot be affected by it. Should be: completeness reports the
busy machine and carries on, or waits with its own, longer limit.

## F8. `extraServices` values are written into YAML unquoted (read from the code, not hit)

`lib.yaml_block` writes `f"{pad}{k}: {v}"`, so an environment value of
`"true"` in `pipeline.json` reaches compose as the YAML boolean `true`, which
compose rejects for `environment`. Grafana's anonymous access needs exactly
that (`GF_AUTH_ANONYMOUS_ENABLED`), and `harness/README.md` says Grafana *"must
… allow anonymous or `admin:admin` access"*. I wrote `"\"true\""` and
`"\"13000:3000\""` (the self-test's own fixture quotes ports the same way) and
did not see it fail, so this is from reading the code. Should be: quote
string scalars in `yaml_block`, or say in the `extraServices` row that
booleans and `host:container` ports must carry their own quotes.

## F9. The self-test's fake-rig messages are written into the real `harness.log`

During the tiny proof, `results/harness.log` received
`[20:17:05] STOPPED: 1 of 2 dashboard panels show no data over the completeness run: "Input rate" (query A (rate_a) returns nothing)`.
My dashboard has no panel called "Input rate"; this is the live self-test
breaking a guard on purpose against a fake Grafana. It is indistinguishable
from a real stop, and my first wait loop matched on `STOPPED` in that file
and returned early. Should be: self-test messages go to their own file, or
carry a prefix such as `self-test (on purpose):`.

## F10. Three instructions about a short step pull in different directions

When a step is not met, §6 says, in order: *"Neither is optional when a step
falls short and the scorecard says CPU"* (probe, ceiling); *"When there is no
human to say yes, tune until you run out of levers"*; and, in the guard
table, *"With nobody to ask, report the step as not met, with the percent,
and stop"*. The harness's own last line resolved it — *"More passes could
change that; change nothing in the pipeline on it"* — but SKILL.md should
say it in one place: *"Not met by less than its range: run probe and ceiling,
change nothing, stop. Not met by more than its range: probe, ceiling, then
§6a."*

## F11. `headline.md` and the scorecard use different words for the same step

The scorecard says 2→4 is **"not met"**; `results/headline.md`, the page the
README is told to link to, says **"spans"** in the "target 1.80x" column. A
reader of the README sees a word the scorecard never uses. Should be: the
same verdict words in both — "not met — more passes could change it".

## F12. Waiting on `results/DONE` with `pgrep -f` kills nothing but never ends

Not the skill's fault, but worth one line in §5's watching advice: a wait
loop of the form `until ! pgrep -f "prove.py X"; do sleep; done` matches its
own shell's command line, so it never ends. I lost two waits (about 15
minutes of wall clock, no measurement affected) to it before switching to
waiting on the process id. §5 already warns that a waiting shell must not
name the project path; it could add: *"wait on a pid or on `results/DONE`,
never on `pgrep -f` of a string your own loop contains."*

## F13. How well the skill supported the README, the evidence in git, and the job diagram

- **README.** §2's section table was enough to write it in one pass before
  any code ran, and preflight's check (every topic and operator named) held
  it to the design. The check itself is weak in one way (F1: substring
  search) and its self-test depends on the pipeline's names. Linking
  `results/records.md` instead of typing examples was the right call: the
  harness wrote one real record per topic, all six topics, correctly
  labelled.
- **Evidence in git.** Worked as described: `results/.gitignore` appeared on
  the first command, and every step ended with a plain list of what was not
  committed yet. The kept set (verdicts, graph, records, headline, manifest
  summary) was right; logs stayed out. The only gap: the scratch logs of the
  standalone `completeness`, `preflight`, `probe` and `ceiling` runs I made by
  hand are not in `results/`, and `ceiling.json` is the only machine-readable
  record of the probe-and-ceiling step's ceiling half — the probe writes no
  results file at all, so its numbers live only in my `FIXES.md`. Should be:
  `prove.py probe` writes `results/probe.json`, kept by the `.gitignore`.
- **Job diagram.** Correct and readable: 4 tasks, 14 operators, every
  Writer tied to its topic, keyBy edges labelled with key set and count
  (4,096 / 16,384), both broadcasts drawn, `every 10 s` on the throttled
  outputs, and the second input tied to its source through `design.sources`.
  The PNG rendered (3168 × 834). One thing cost thought: the keyBy label and
  the output key counts come from matching `keySets` stage names as
  substrings of operator and topic names, which is documented only in code
  (`graph_context`, `graph_tasks`). I named my stages `by-symbol` and
  `by-account` so they would match both the keyed operators and all four
  outputs; the README's `keySets` row should say the stage name is matched
  against operator and topic names.
