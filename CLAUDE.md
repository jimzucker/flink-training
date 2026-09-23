# Working in this repository

Committed guidance for Claude Code, loaded automatically in this repo. It
replaces the per-machine memory the project used to rely on: **if memory and
this file disagree, this file wins**, and anything worth keeping goes here or in
`docs/`, never only in memory.

Where the evidence lives:

| what | where |
|---|---|
| every scaling pass, all steps and runs | `docs/runs/ledger.csv` — see `docs/runs/README.md` |
| the runs status table | top of `docs/skill-validation/README.md` |
| findings behind the rules below | `docs/skill-validation/findings.md` |
| the skill itself | its own repository: [jimzucker/scalable-flink-skill](https://github.com/jimzucker/scalable-flink-skill) |
| a pipeline we already trust, for testing harness changes without a clean room | `docs/skill-validation/reference-pipeline/` |
| public writing, tone rules, canonical wording | `docs/linkedin/README.md` |
| what happened in each step, verbatim | `docs/steps/step-NN/transcript.md` |

## Evidence, not recall

Most wrong answers in this project were guesses stated as facts: a speed
multiple rounded up to "about three times", a rule list presented "as approved"
after it had been reworded, a run count that undercounted, a search hit read as
proof. So:

- **Every number, file path, commit, run result or quoted wording comes from
  reading the committed source in this session** — not from memory, a summary,
  or an earlier reply. Name the file it came from.
- **Memory is an index, not evidence.** It goes stale. When memory and the
  repository disagree, the repository is right — and the memory entry gets
  corrected.
- **Approved wording is copied, never reconstructed.** Retrieve it from the
  committed file or the transcript and paste it verbatim.
- **Say what a check can and cannot show.** A grep hit does not prove a fact is
  recorded; a match in a transcript is not a written rule. Label a loose check
  as loose.
- **If it is not verified, say "not verified"** — or verify it first. Do not
  round, extrapolate or fill a gap to make an answer look complete.
- **Before adding a derived figure or column, recompute what is already
  published from the same data** and confirm the convention matches.

## Answering

- **Lead with the answer. Keep it short.** Status is one or two lines. A table
  beats a paragraph. The derivation goes in the record, not the reply.
- **"The status table" / "our normal review"** means the runs table at the top
  of `docs/skill-validation/README.md`. Read it from the file every time, show
  it as a table, and say that run 29 has no row.
- **The user's own text wins.** When they paste wording, use it verbatim. Never
  reword a list they have approved — copy it from where it is committed.

## Measuring

- **Measure before explaining.** A cause needs one rig, one build, one variable
  changed, both arms measured. Anything less is labelled a hypothesis. *"I don't
  know yet"* is a complete answer. (Four wrong mechanisms in one day — findings §2.)
- **Never explain one system's number with another system's measurement.**
  Rates compare only within one run: runs built different pipelines.
- **Diff every component's configuration before experimenting.** Constants
  cannot be causes. (findings §3)
- **Apples to apples:** same inputs, same job, same instrument — including key
  cardinality and test conditions.
- **When the shape of the input changes** — cardinality, record size, key
  format — re-check memory and GC, backlog sizing, the verifier's expectations
  and the guard thresholds, front to back.
- **One measurement is a guess.** A two-pass ratio carries about ±4% on this
  rig. Know the host's ceiling before blaming the code: memory-bound work
  returns ~77% on the second core doubling here.
- **Check arithmetic in a table before asserting it in prose**, and read public
  figures back from the source files before publishing them.

## Testing and runs

- **Stop at the first failure.** Don't run the rest of a battery on code that is
  already broken. Probe the hardest case first.
- **Prove a fix at the cheapest level that reproduces it**, in order:
  `prove.py selftest-pure` → `prove.py replay` → the rig (`prove.py all --quick`)
  → the reference pipeline → a clean-room run. A run is never the first proof
  of a fix.
- **A harness change is tested against the reference pipeline, not a clean
  room.** `docs/skill-validation/reference-pipeline/run-reference.sh` re-runs
  the whole chain against a build we already trust, in about an hour with no
  pipeline to write or debug. Spend a clean-room run only when what the skill
  *teaches* has changed — runs 44, 45 and 46 cost 363k, 559k and 294k tokens,
  and most of that was an agent debugging its own Flink job.
- **Replay every new guard or threshold against every recorded run** before it
  can block anything. Thresholds come from measured spread, not round numbers.
  One change per validation run.
- **If a rule matters, make the tool enforce it** — a guard with a self-test in
  the harness, or delete the rule. Prose rules get broken.
- **Verify the launch.** After starting a long job, confirm a few seconds later
  that it is alive and its log advanced before reporting an ETA; say "issued"
  vs "confirmed running". Chain the next step onto the end of the current one.
- **One clean-room run at a time** on this machine, and check host free disk
  (`df -h /System/Volumes/Data`) before starting one.
- **Don't send output to `/dev/null`** while finding out whether a command works.
  Assert the effect, never the exit code.
- **After any new run:** `python3 scripts/build-runs-ledger.py`, and add the
  run's row to the status table.

## Git and pull requests

- A branch per change, squash-merged. PR bodies end with the Claude Code
  attribution.
- **Pushing needs Java 17** — the pre-push hook runs Maven, whose enforcer fails
  otherwise. Push in one shell, then confirm the branch reached origin:

  ```
  source scripts/env.sh >/dev/null 2>&1 && git push -u origin <branch>
  git ls-remote --heads origin <branch>
  ```

- **Auto-merge is disabled.** Poll `gh pr checks <n>` until nothing is pending
  and nothing failed, then `gh pr merge <n> --squash --delete-branch`. A push to
  the branch restarts the checks.
- **Don't leave uncommitted edits on `main`** while a merge is being waited on;
  the pull afterwards aborts over them.
- **`git reset --hard` is blocked in auto mode.** Rebuild a file from the remote
  with `git show origin/main:<path> > <path>`. Move a commit made on `main` onto
  a branch with `git checkout -b <branch>` then `git branch -f main origin/main`.
- **Use absolute paths.** A `cd` in one of several parallel shells moves the
  working directory the others resolve against.

## Docker on this laptop

- Docker Desktop does not return pruned space to macOS. Trim inside the VM:

  ```
  docker run --rm --privileged --pid=host alpine nsenter -t 1 -m -u -n -i -- fstrim -v /var/lib/docker
  ```

- **Never `docker system prune -af --volumes`** — it takes the project's images
  and the `n8n_data` volume. Restart sequence and measurements: findings §5.

## Records

- **Transcripts are generated, never hand-edited:** `scripts/build-transcript.py`.
  Redaction lives in its `REDACTIONS`; results and transcripts are secret-scanned
  and user paths redacted before commit.
- The user's email is for identification only; never send it to another service.
