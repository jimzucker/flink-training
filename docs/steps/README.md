# Steps

One directory per step, holding the working detail behind the
[journal](../../JOURNAL.md) entry: design notes, measurements, and the raw output
of whatever was run.

Each also carries a **`transcript.md`** — every prompt and every response for that
step, verbatim and unedited, in the order they happened.

## Why the transcripts are here

Three records describe this project, and they are deliberately not the same
thing:

| record | what it is | who it is for |
|---|---|---|
| [`JOURNAL.md`](../../JOURNAL.md) | what was decided and why | someone reading the project |
| [`docs/reviews/`](../reviews) | the review exchange, round by round | someone checking the work was challenged |
| `transcript.md` | everything said, both sides | someone who trusts neither of the above |

The first two are written. The third is generated, which is the point: a summary
can flatter its author, and a transcript cannot. If the journal claims a decision
was reasoned rather than stumbled into, the transcript is where that gets
checked — including the wrong turns, the corrections, and the questions that were
answered with one word.

For a project whose headline is that an AI built it, **the human input is the
number that matters.** It is 7,229 words across 262 prompts, and the transcripts
are how anyone can verify that rather than take it on trust.

## Regenerating

    python3 scripts/build-transcript.py

Rebuilt from the Claude Code session logs, bucketed into steps by tag timestamp,
so it is never hand-maintained and cannot drift from what happened. Run it after
each step. Account identifiers, addresses and anything credential-shaped are
redacted on the way out — this repo is public and the session logs are not.
