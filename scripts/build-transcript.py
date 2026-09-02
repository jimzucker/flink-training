#!/usr/bin/env python3
"""Regenerate docs/steps/*/transcript.md from the Claude Code session logs.

The step docs say what was decided; the reviews say what the reviewer said. This
writes the third record: every prompt and every response, verbatim, so the work
can be audited by someone who trusts none of the summaries.

Sessions are bucketed into steps by tag timestamp. Re-run after each step:

    python3 scripts/build-transcript.py

Account identifiers, addresses and anything credential-shaped are redacted on the way out; the repo is public and
the session logs are not.
"""
import json, os, re, subprocess, sys
from datetime import datetime, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
PROJECT_DIRS = [
    Path.home() / ".claude/projects/-Users-jimzucker-code-GitHub-flink-training",
    Path.home() / ".claude/projects/-Users-jimzucker-code-GitHub-flink-university",
]
# Machine-generated turns that are not the human speaking.
SKIP_PREFIXES = (
    "<task-notification>", "<local-command", "<command-name>", "<command-message>",
    "[Request interrupted", "<system-reminder>", "Caveat:", "Shell cwd was reset",
)


def step_bounds():
    tags = subprocess.run(["git", "tag", "-l", "step-*"], cwd=REPO,
                          capture_output=True, text=True).stdout.split()
    tags.sort(key=lambda t: int(t.split("-")[1]))
    out = []
    for t in tags:
        ts = subprocess.run(["git", "log", "-1", "--format=%at", t], cwd=REPO,
                            capture_output=True, text=True).stdout.strip()
        if ts:
            out.append((t, int(ts)))
    return out


def load_sessions():
    """Every session line, deduped by session id (a renamed cwd files it twice)."""
    seen, lines = set(), []
    for d in PROJECT_DIRS:
        if not d.is_dir():
            continue
        for f in sorted(d.glob("*.jsonl")):
            if f.stem in seen:
                continue
            seen.add(f.stem)
            with f.open() as fh:
                for line in fh:
                    try:
                        lines.append(json.loads(line))
                    except ValueError:
                        continue
    return lines


def epoch(rec):
    ts = rec.get("timestamp")
    if not ts:
        return None
    return datetime.strptime(ts[:19], "%Y-%m-%dT%H:%M:%S").replace(
        tzinfo=timezone.utc).timestamp()


# Redaction. The repo is public and the logs are not: they carry the account
# identifiers and addresses that a working session naturally mentions.
REDACTIONS = [
    # AWS account ids, only where the surrounding text proves what they are.
    (re.compile(r"\b\d{12}(?=\.dkr\.ecr\.)"), "<aws-account>"),
    (re.compile(r"(?<=arn:aws:)([a-z0-9-]+:[a-z0-9-]*:)\d{12}"), r"\1<aws-account>"),
    # A bare id, when the words around it say it is an account.
    (re.compile(r"(?i)\b(account|acct)\b([^\n]{0,20}?)\b\d{12}\b"), r"\1\2<aws-account>"),
    # Credentials, in case one is ever pasted into a session.
    (re.compile(r"\b(?:AKIA|ASIA)[A-Z0-9]{16}\b"), "<aws-key>"),
    (re.compile(r"\b(?:ghp|gho|ghu|ghs|ghr)_[A-Za-z0-9]{20,}"), "<github-token>"),
    (re.compile(r"\bsk-[A-Za-z0-9_-]{20,}"), "<api-key>"),
    (re.compile(r"\bxox[bapsr]-[A-Za-z0-9-]{10,}"), "<slack-token>"),
    # Real addresses. Documentation placeholders are left alone.
    (re.compile(r"\b[A-Za-z0-9._%+-]+@(?!.*\.example\b)"
                r"[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b"), "<email>"),
]


def redact(text):
    for pattern, replacement in REDACTIONS:
        text = pattern.sub(replacement, text)
    return text


def demote(md):
    """Push response headings two levels down so they nest under the exchange."""
    return re.sub(r"^(#{1,4}) ", lambda m: "#" * min(len(m.group(1)) + 3, 6) + " ",
                  md, flags=re.M)


def exchanges(records):
    """Pair each human prompt with the response and tool calls that followed it."""
    out, cur = [], None
    for r in records:
        if r.get("isSidechain"):
            continue
        msg = r.get("message") or {}
        content = msg.get("content")
        if r.get("type") == "user" and isinstance(content, str):
            s = content.strip()
            if not s or s.startswith(SKIP_PREFIXES):
                continue
            if cur:
                out.append(cur)
            cur = {"ts": epoch(r), "prompt": s, "reply": [], "tools": 0}
        elif r.get("type") == "assistant" and cur is not None and isinstance(content, list):
            for b in content:
                if not isinstance(b, dict):
                    continue
                if b.get("type") == "text" and b.get("text", "").strip():
                    cur["reply"].append(b["text"].strip())
                elif b.get("type") == "tool_use":
                    cur["tools"] += 1
    if cur:
        out.append(cur)
    return [e for e in out if e["ts"]]


def main():
    bounds = step_bounds()
    ex = sorted(exchanges(load_sessions()), key=lambda e: e["ts"])

    buckets = {}
    for e in ex:
        label = next((t for t, tt in bounds if e["ts"] <= tt), "post-step-13")
        buckets.setdefault(label, []).append(e)

    titles = {}
    for t, _ in bounds:
        titles[t] = subprocess.run(["git", "log", "-1", "--format=%s", t], cwd=REPO,
                                   capture_output=True, text=True).stdout.strip()

    written = []
    for label, items in sorted(buckets.items()):
        d = REPO / "docs/steps" / label
        d.mkdir(parents=True, exist_ok=True)
        title = titles.get(label, "Work after step 13")
        words = sum(len(e["prompt"].split()) for e in items)
        rwords = sum(len(" ".join(e["reply"]).split()) for e in items)

        lines = [
            f"# {label} — full transcript",
            "",
            f"*{title}*" if label in titles else "*Work after step 13: the deck, "
            "the article, and the skill with its clean-room validations.*",
            "",
            "Every prompt and every response for this step, verbatim and unedited, "
            "in the order they happened. Generated by "
            "[`scripts/build-transcript.py`](../../../scripts/build-transcript.py) "
            "from the session logs — not written by hand, and not summarised.",
            "",
            "The [step doc](.) says what was decided and the "
            "[review](../../reviews) says what the reviewer said. This file exists "
            "so neither has to be taken on trust.",
            "",
            f"**{len(items)} exchanges — {words:,} words of prompt, "
            f"{rwords:,} words of response.**",
            "",
            "---",
            "",
        ]
        for e in items:
            when = datetime.fromtimestamp(e["ts"], timezone.utc).strftime(
                "%Y-%m-%d %H:%M UTC")
            tools = f" · {e['tools']} tool call{'s' if e['tools'] != 1 else ''}" \
                if e["tools"] else ""
            lines += [f"## {when}{tools}", "", "**Prompt**", ""]
            prompt = redact(e["prompt"])
            lines += ["> " + l if l.strip() else ">" for l in prompt.split("\n")]
            lines += ["", "**Response**", ""]
            lines.append(redact(demote("\n\n".join(e["reply"]))) if e["reply"]
                         else "*No text response — the turn was tool calls only.*")
            lines += ["", "---", ""]

        (d / "transcript.md").write_text("\n".join(lines) + "\n")
        written.append((label, len(items), words, rwords))

    print(f"{'step':16} {'exchanges':>9} {'prompt w':>9} {'reply w':>9}")
    for label, n, w, rw in written:
        print(f"{label:16} {n:9} {w:9,} {rw:9,}")
    print(f"\n{len(ex)} exchanges written to {len(written)} files.")


if __name__ == "__main__":
    main()
