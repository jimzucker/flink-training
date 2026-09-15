"""Line up, per 5 s, the tiny proof's committed-offset rate with the VM's memory and refaults
and the job's checkpoint progress. Usage: python3 join.py <tinyproof.json>"""
import json, os, re, sys
S = os.path.dirname(os.path.abspath(sys.argv[1]))  # vm.log and sampler.log sit beside tinyproof.json
tp = json.load(open(sys.argv[1]))

vm = []
for line in open(f"{S}/vm.log"):
    m = re.search(r"ts=(\d+).*MemFree: (\d+) kB MemAvailable: (\d+) kB Cached: (\d+) kB.*workingset_refault_file (\d+) pswpin (\d+) pswpout (\d+) pgmajfault (\d+)", line)
    if m:
        vm.append(tuple(int(x) for x in m.groups()))
cps = []
for line in open(f"{S}/sampler.log"):
    m = re.search(r"ts=(\d+) job=(\w+) cp_completed=(\d+) cp_failed=(\d+) cp_inprog=(\d+) last_id=(\S+) last_e2e_ms=(\S+)", line)
    if m:
        cps.append((int(m.group(1)), m.group(2), int(m.group(3)), int(m.group(4)), int(m.group(5)), m.group(7)))
    mem = re.findall(r"(plf32-\w+) cpu=([\d.]+)% mem=([\d.]+\w+) /", line)

def nearest(rows, ts):
    return min(rows, key=lambda r: abs(r[0] - ts)) if rows else None

print(f"ratio={tp.get('ratio')} result={tp.get('result')}")
for c in tp["cases"]:
    print(f"\n=== {c['cores']}c  rate={c.get('recordsPerSec')}  cap={c.get('tmCapFrac')}  refaults={c.get('brokerRefaults')}  gc={c.get('gcFracOfCapacity')}  srcBusy={c.get('sourceBusy')}  warmup={c.get('warmup')}")
    print(" t(s)   rate/s   | VM free MB  avail MB  cache MB | refault Δ  majflt Δ  swapout Δ | cp done  failed  inprog  last e2e ms")
    pts = sorted([t for t in c["ticks"] if "committed" in t] + [c["open"], c["close"]], key=lambda t: t["ts"])
    last = None
    for t in pts:
        if last is not None and t["ts"] - last["ts"] < 5000:
            continue
        if last is not None:
            ts = t["ts"] / 1000
            v, v0 = nearest(vm, ts), nearest(vm, last["ts"] / 1000)
            cp = nearest(cps, ts)
            rate = (t["committed"] - last["committed"]) / ((t["ts"] - last["ts"]) / 1000)
            vs = (f"{v[1]//1024:>9} {v[2]//1024:>9} {v[3]//1024:>9} | {v[4]-v0[4]:>9} {v[7]-v0[7]:>9} {v[6]-v0[6]:>9}" if v and v0 else " no vm sample")
            cs = f"{cp[2]:>7} {cp[3]:>7} {cp[4]:>7} {cp[5]:>12}" if cp and abs(cp[0] - ts) < 6 else " no cp sample"
            print(f"{ts - c['tSubmit']:6.1f} {rate:>9,.0f} | {vs} | {cs}")
        last = t
