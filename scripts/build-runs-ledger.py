#!/usr/bin/env python3
"""Build docs/runs/ledger.csv: one row per measured pass, every run we have.

Sources, merged in this order:

  1. docs/skill-validation/**/suite.json        harness suites, one row per pass
  2. docs/skill-validation/**/*tinyproof*.json  harness tiny proofs, one row per case
  3. .claude/skills/prove-it-scales/harness/record/{run9,run10,plan12}-*.json
                                                rate-only records for runs that
                                                predate suite.json
  4. docs/runs/transcribed.csv                  hand-transcribed from prose and
                                                text logs: clean-room runs 1-8,
                                                steps 10-12

Nothing here is computed from a model: every value is copied from its source,
with percentages converted to fractions. A field the source does not record is
left empty, never filled. Re-run after any new run:

    python3 scripts/build-runs-ledger.py
"""
import csv
import glob
import json
import os
import re
import sys
from datetime import datetime, timezone

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
VAL = os.path.join(ROOT, "docs", "skill-validation")
RECORD = os.path.join(ROOT, ".claude", "skills", "prove-it-scales", "harness", "record")
TRANSCRIBED = os.path.join(ROOT, "docs", "runs", "transcribed.csv")
OUT = os.path.join(ROOT, "docs", "runs", "ledger.csv")

COLUMNS = [
    "study", "run", "suite", "source_kind", "variant",
    "cores", "parallelism", "pass", "status",
    "input_rate", "input_unit", "output_rate", "output_unit", "elapsed_s",
    "tm_cores", "tm_cap_frac", "tm_throttled_pct",
    "kafka_cores", "kafka_cap", "kafka_cap_frac",
    "gc_frac", "gc_ms",
    "tm_memory_config",
    "broker_limit_hits", "broker_refaults", "broker_file_cache_bytes", "broker_limit_bytes",
    "busy_frac", "backpressure_frac", "source_idle", "max_task_backpressured",
    "vantage_disagreement", "headroom_s", "checkpoint_ms",
    "build", "started_at", "source_file", "notes",
]

# Records in harness/record/ that duplicate a suite.json already read.
RECORD_DUPLICATES = {"run11-suite.json", "harness-live-1.json"}


def rel(path):
    return os.path.relpath(path, ROOT)


def locate(path):
    """study and run for a file under docs/skill-validation/."""
    parts = os.path.relpath(path, VAL).split(os.sep)
    if parts[0] == "clean-room":
        return "clean-room", parts[1].replace("run-", "")
    if parts[0] == "demo-under-harness":
        return "demo-under-harness", parts[1]
    if parts[0] == "plan-12":
        return "plan-12", parts[1]
    if parts[0].startswith("rig-"):
        return "rig", parts[0]
    if parts[0].startswith("harness-live"):
        return "harness-live", parts[0]
    return parts[0], parts[1] if len(parts) > 2 else ""


def iso(epoch):
    if not epoch:
        return ""
    return datetime.fromtimestamp(epoch, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def unit_from(rate_source):
    """'kafka committed offsets on block-trades' -> 'block-trades/s'."""
    m = re.search(r" on (\S+)$", rate_source or "")
    return f"{m.group(1)}/s" if m else ""


def pass_row(r, study, run, suite, kind, source, build, started, tm_memory):
    bp = r.get("backpressure") or {}
    peaks = [v.get("backPressured") for v in bp.values()
             if isinstance(v, dict) and v.get("backPressured") is not None]
    return {
        "study": study, "run": run, "suite": suite, "source_kind": kind,
        "cores": r.get("cores"), "parallelism": r.get("parallelism"),
        "pass": r.get("pass"), "status": r.get("status"),
        "input_rate": r.get("recordsPerSec"),
        "input_unit": unit_from(r.get("rateSource")),
        "output_rate": r.get("outputRecsPerSec"),
        "output_unit": "output records/s" if r.get("outputRecsPerSec") is not None else "",
        "elapsed_s": r.get("elapsedS"),
        "tm_cores": r.get("tmCores"), "tm_cap_frac": r.get("tmCapFrac"),
        "tm_throttled_pct": r.get("tmThrottledPeriodsPct"),
        "kafka_cores": r.get("kafkaCores"), "kafka_cap": r.get("kafkaCap"),
        "kafka_cap_frac": r.get("kafkaCapFrac"),
        "gc_frac": r.get("gcFracOfCapacity"), "gc_ms": r.get("gcMsInWindow"),
        "tm_memory_config": tm_memory,
        "broker_limit_hits": r.get("brokerLimitHits"),
        "broker_refaults": r.get("brokerRefaults"),
        "broker_file_cache_bytes": r.get("brokerFileCacheBytes"),
        "broker_limit_bytes": r.get("brokerLimitBytes"),
        "busy_frac": r.get("sourceBusy"),
        "backpressure_frac": r.get("sourceBackpressured"),
        "source_idle": r.get("sourceIdle"),
        "max_task_backpressured": max(peaks) if peaks else None,
        "vantage_disagreement": r.get("vantageDisagreement"),
        "headroom_s": r.get("headroomS"),
        "build": build, "started_at": started or iso(r.get("tSubmit")),
        "source_file": source,
    }


def tm_memory_of(suite_doc):
    held = (suite_doc or {}).get("heldStill") or {}
    return held.get("tmProcessMemory") or ""


def from_suites():
    rows = []
    for path in sorted(glob.glob(os.path.join(VAL, "**", "suite.json"), recursive=True)):
        doc = json.load(open(path))
        study, run = locate(path)
        checkpoint = doc.get("checkpointIntervalMs")
        for r in doc.get("runs", []):
            row = pass_row(r, study, run, "suite", "suite.json", rel(path),
                           doc.get("buildHash"), iso(r.get("tSubmit")), tm_memory_of(doc))
            row["checkpoint_ms"] = checkpoint
            if doc.get("quickLook"):
                row["notes"] = "quick look: not publishable"
            rows.append(row)
    return rows


def from_tinyproofs():
    rows = []
    for path in sorted(glob.glob(os.path.join(VAL, "**", "*tinyproof*.json"), recursive=True)):
        doc = json.load(open(path))
        study, run = locate(path)
        sibling = os.path.join(os.path.dirname(path), "suite.json")
        tm_memory = tm_memory_of(json.load(open(sibling))) if os.path.exists(sibling) else ""
        suite = os.path.splitext(os.path.basename(path))[0]
        for r in doc.get("cases", []):
            row = pass_row(r, study, run, suite, "tinyproof", rel(path),
                           doc.get("build"), "", tm_memory)
            if not row["tm_memory_config"] and os.path.exists(sibling) is False:
                row["notes"] = "no sibling suite.json; memory config not recorded here"
            rows.append(row)
    return rows


def from_records():
    rows = []
    for path in sorted(glob.glob(os.path.join(RECORD, "*.json"))):
        name = os.path.basename(path)
        if not re.match(r"(run9|run10|plan12)-", name) or name in RECORD_DUPLICATES:
            continue
        doc = json.load(open(path))
        run = str(doc.get("run"))
        study = "plan-12" if name.startswith("plan12") else "clean-room"
        if study == "plan-12":
            run = "phase1"
        for cores, rates in sorted(doc.get("rates", {}).items(), key=lambda kv: int(kv[0])):
            for i, rate in enumerate(rates, 1):
                rows.append({
                    "study": study, "run": run, "suite": doc.get("suite"),
                    "source_kind": "record", "cores": int(cores), "pass": i,
                    "input_rate": rate, "input_unit": doc.get("unit"),
                    "source_file": rel(path),
                    "notes": "rate only; step valid: " + json.dumps(doc.get("validSteps")),
                })
    return rows


def from_transcribed():
    rows = []
    with open(TRANSCRIBED, newline="") as f:
        for r in csv.DictReader(f):
            r = {k: v for k, v in r.items() if v != ""}
            r["source_kind"] = "transcribed"
            rows.append(r)
    return rows


def natural(value):
    return [int(t) if t.isdigit() else t for t in re.split(r"(\d+)", str(value or ""))]


def main():
    rows = from_suites() + from_tinyproofs() + from_records() + from_transcribed()
    order = {"step": 0, "clean-room": 1, "rig": 2, "harness-live": 3,
             "plan-12": 4, "demo-under-harness": 5}
    rows.sort(key=lambda r: (order.get(r.get("study"), 9), natural(r.get("run")),
                             r.get("source_kind", ""), natural(r.get("suite")),
                             natural(r.get("variant")), int(r.get("cores") or r.get("parallelism") or 0),
                             natural(r.get("pass"))))
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=COLUMNS, extrasaction="raise")
        w.writeheader()
        for r in rows:
            w.writerow({k: ("" if r.get(k) is None else r.get(k)) for k in COLUMNS})

    by_kind = {}
    for r in rows:
        by_kind[r["source_kind"]] = by_kind.get(r["source_kind"], 0) + 1
    print(f"wrote {rel(OUT)}: {len(rows)} rows", json.dumps(by_kind, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
