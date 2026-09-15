import re, statistics, sys
import os; log = os.path.join(os.path.dirname(os.path.abspath(__file__)), "matrix.log")
rates, gc, bad = {}, {}, []
for line in open(log):
    if not line.startswith("rep="):
        continue
    m = re.search(r"filler=(\d+) bytesPerOrder=([\d.]+) threads=(\d+) cpus=(\d+) mode=\S+ orders/s=(\d+) perThread=\d+ gcPctOfWall=([\d.]+)", line)
    if not m:
        bad.append(line.strip()); continue
    f, b, t, cpus, r, g = m.groups()
    key = (int(f), float(b), int(t))
    rates.setdefault(key, []).append(int(r)); gc.setdefault(key, []).append(float(g))
    if int(cpus) != int(t):
        bad.append("cpus!=threads: " + line.strip())
print("| B/order | CPUs | runs | orders/s (median) | min–max | KB/s | GC % wall | × vs 1 CPU |")
print("|---:|---:|---:|---:|---:|---:|---:|---:|")
for key in sorted(rates):
    f, b, t = key
    med = statistics.median(rates[key]); one = statistics.median(rates.get((f, b, 1), [med]))
    print(f"| {b:,.1f} | {t} | {len(rates[key])} | {med:,.0f} | {min(rates[key]):,}–{max(rates[key]):,} | {med*b/1000:,.0f} | {statistics.median(gc[key]):.2f} | {med/one:.2f} |")
for key in sorted(rates):
    f, b, t = key
    if t == 4 and (f, b, 2) in rates:
        print(f"filler={f}: 1→2 {statistics.median(rates[(f,b,2)])/statistics.median(rates[(f,b,1)]):.3f}  2→4 {statistics.median(rates[key])/statistics.median(rates[(f,b,2)]):.3f}")
for l in bad:
    print("UNPARSED:", l)
