import re, statistics
import os; log = os.path.join(os.path.dirname(os.path.abspath(__file__)), "stages.log")
ns, bad = {}, []
for line in open(log):
    if not line.startswith("rep="):
        continue
    m = re.search(r"filler=(\d+) stage=(\w+) .*nsPerOrder=([\d.]+)", line)
    if not m:
        bad.append(line.strip()); continue
    ns.setdefault((m.group(2), int(m.group(1))), []).append(float(m.group(3)))

order = ["fetch", "decode", "tokens", "skip", "parse", "symbol", "split", "ser", "deser"]
med = {k: statistics.median(v) for k, v in ns.items()}
print("| stage | 326 B µs/order | 2 KB µs/order | added µs | × | runs (spread 2 KB) |")
print("|---|---:|---:|---:|---:|---:|")
for s in order:
    a, b = med.get((s, 0)), med.get((s, 32))
    if a is None or b is None:
        print(f"| {s} | missing |"); continue
    v = ns[(s, 32)]
    print(f"| {s} | {a/1000:.2f} | {b/1000:.2f} | {(b-a)/1000:.2f} | {b/a:.1f} | {len(v)} ({(max(v)-min(v))/b:.0%}) |")

# the job's per-order path: fetch + decode + symbol + split + ser + deser
path = ["fetch", "decode", "symbol", "split", "ser", "deser"]
if all((s, f) in med for s in path for f in (0, 32)):
    a = sum(med[(s, 0)] for s in path); b = sum(med[(s, 32)] for s in path)
    print(f"\njob path ({' + '.join(path)}): 326 B {a/1000:.2f} µs, 2 KB {b/1000:.2f} µs, added {(b-a)/1000:.2f} µs")
    for s in path:
        print(f"  {s:7s} share of added cost: {(med[(s,32)]-med[(s,0)])/(b-a):.0%}")
for l in bad:
    print("UNPARSED:", l)
