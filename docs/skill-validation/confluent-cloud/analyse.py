"""One rule for every case: the minutes from the one after the statement first
reaches its pool's size, to the one before the last reported minute, leaving
out any minute that starts with nothing waiting and the minute in which the
backlog runs out (the next minute starts with nothing waiting). Mean records read and mean
Kafka bytes sent over those minutes, then the 10 -> 20 ratio per pipeline."""
import json, sys

rec = json.load(open(sys.argv[1]))
rows = {}
for c in rec["cases"]:
    cfu = dict(c["statement_utilization/current_cfus"])
    rin = dict(c["num_records_in"])
    pend = dict(c["pending_records"])
    sent = dict(c["io.confluent.kafka.server/sent_bytes"])
    ecku = dict(c["io.confluent.kafka.server/elastic_cku_count"])
    mins = sorted(rin)
    first_full = min(m for m in sorted(cfu) if float(cfu[m]) >= c["cfu"])
    nxt = {m: mins[i + 1] for i, m in enumerate(mins[:-1])}
    use = [m for m in mins if m > first_full and m in nxt and float(pend.get(m, 1)) > 0
           and float(pend.get(nxt[m], 1)) > 0]
    r = sum(float(rin[m]) for m in use) / len(use)
    b = sum(float(sent[m]) for m in use if m in sent) / len([m for m in use if m in sent])
    e = sorted({float(ecku[m]) for m in use if m in ecku})
    lo, hi = min(float(rin[m]) for m in use), max(float(rin[m]) for m in use)
    rows[c["label"]] = (r, b)
    print(f"{c['label']:7} {c['cfu']:>3} CFU  minutes {use[0]}-{use[-1]} ({len(use)})  "
          f"records read {r / 1e6:6.2f} M/min (lowest {lo / 1e6:.1f}, highest {hi / 1e6:.1f})  "
          f"Kafka sent {b / 1e6:7.0f} MB/min  eCKU {e}")
for kind in ("sum", "copy"):
    a, z = rows.get(f"{kind}10"), rows.get(f"{kind}20")
    if a and z:
        print(f"{kind}: 10 -> 20 CFU reads {z[0] / a[0]:.2f}x by records read, {z[1] / a[1]:.2f}x by Kafka bytes sent")
