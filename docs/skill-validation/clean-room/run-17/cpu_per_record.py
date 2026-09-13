#!/usr/bin/env python3
"""CPU cost per input record, per case, from results/suite.json.

  microseconds of task-manager CPU per input record
     = (task manager CPU seconds consumed inside the window) / (records consumed) * 1e6
     = tmCores / recordsPerSec * 1e6

Both numbers come from the harness's own measurement of the same window: CPU from
the container's cgroup cpu.stat usage_usec at open and close, records from the
committed broker offsets at open and close. Nothing here is re-measured.
"""
import json
import os
import sys

path = sys.argv[1] if len(sys.argv) > 1 else os.path.join(os.path.dirname(__file__), "suite.json")
d = json.load(open(path))
out_per_in = d.get("outputsPerInput", 1)
rows = []
for r in d["runs"]:
    if r.get("status") != "OK":
        continue
    us_in = r["tmCores"] / r["recordsPerSec"] * 1e6
    rows.append((r["cores"], r["pass"], r["recordsPerSec"], r["tmCores"], us_in,
                 us_in / (1 + out_per_in), r.get("kafkaCores")))

print(f"{'cores':>5} {'pass':>10} {'in rec/s':>11} {'tm cores':>9} {'us/in rec':>10} "
      f"{'us/rec incl out':>16} {'broker cores':>12}")
for c, p, rate, tm, us, us_all, k in rows:
    print(f"{c:>5} {p:>10} {rate:>11,.0f} {tm:>9.3f} {us:>10.3f} {us_all:>16.3f} {k:>12.2f}")

print()
by_case = {}
for c, p, rate, tm, us, us_all, k in rows:
    by_case.setdefault(c, []).append((rate, us))
print("per case (mean of the passes measured):")
for c in sorted(by_case):
    v = by_case[c]
    mr = sum(x[0] for x in v) / len(v)
    mu = sum(x[1] for x in v) / len(v)
    print(f"  {c} core(s): {mr:,.0f} input rec/s, {mu:.3f} us of task-manager CPU per input record "
          f"({len(v)} pass(es))")
