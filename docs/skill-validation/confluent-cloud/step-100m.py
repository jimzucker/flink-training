import json, sys
for f in sys.argv[1:]:
    r = json.load(open(f)); out = []
    for c in r["cases"]:
        m = c["minutes"]
        if isinstance(m, dict) and "cfu" in m:
            cfu, rin, pend = (dict(m[k]) if not isinstance(m[k], dict) else m[k] for k in ("cfu", "recordsIn", "pending"))
        else:
            print("minutes shape:", type(m), str(m)[:300]); sys.exit()
        sent = c["kafkaSent"] if isinstance(c["kafkaSent"], dict) else dict(c["kafkaSent"])
        first = min(t for t in sorted(cfu) if float(cfu[t]) >= c["cfu"])
        use = [t for t in sorted(rin) if t > first and float(pend.get(t, 0)) >= 100e6]
        rate = sum(float(rin[t]) for t in use) / len(use)
        b = [float(sent[t]) for t in use if t in sent]
        out.append((c["cfu"], rate, sum(b) / len(b), use[0][-8:-3] if len(use[0]) > 5 else use[0], use[-1][-8:-3] if len(use[-1]) > 5 else use[-1], len(use)))
    for o in out: print(f"{f[-40:]:40} {o[0]:>3} CFU {o[1]/1e6:6.2f} M/min  {o[3]}-{o[4]} ({o[5]} min)  sent {o[2]/1e6:.0f}")
    print(f"   step {out[1][1]/out[0][1]:.2f}x by records read, {out[1][2]/out[0][2]:.2f}x by Kafka bytes")
