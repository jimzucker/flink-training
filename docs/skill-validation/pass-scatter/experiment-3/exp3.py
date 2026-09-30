"""Experiment 3: does one running job's rate vary over time as much as passes
vary between fresh starts? Two fresh starts of the 2-core job; each is measured
in consecutive 60 s windows of committed input offsets (the sampler every
pass uses), after 90 s of warm-up. Nothing else changes."""
import json, os, statistics as st, sys, time
sys.path.insert(0, "<scalable-flink-skill>/harness")
import lib as L

OUT = sys.argv[1]
c = L.cfg()
results = []
for start in ("A", "B"):
    group = f"{c.project}-noise3-{start}-{int(time.time())}"
    print(f"start {start}: group {group}", flush=True)
    L.stop_tm()
    L.delete_group(group)
    L.start_tm(2, slots=2)
    L.start_sampler(group)
    t_sub = time.time()
    jid = L.submit_job(2, group, c.ckpt_ms)
    L.wait_running(jid, 2)
    print(f"  running {jid} after {time.time() - t_sub:.0f}s", flush=True)
    # run until the drain is nearly over: stop when committed stops advancing
    # for 30 s, or after 11 minutes
    last, still, t0 = None, 0, time.time()
    while time.time() - t0 < 660:
        time.sleep(10)
        tk = L.sampler_tail(2)
        cur = tk[-1]["committed"] if tk else None
        if cur is not None and cur == last:
            still += 10
            if still >= 30:
                break
        else:
            still = 0
        last = cur
    ticks = [t for t in L.sampler_ticks_since(int(t_sub * 1000)) if t.get("committed", -1) >= 0]
    L.cancel_job(jid)
    L.stop_sampler()
    L.stop_tm()
    # windows: 60 s, starting 90 s after the job was running, while it still moved
    first = ticks[0]["ts"] / 1000
    series = [(t["ts"] / 1000 - first, t["committed"]) for t in ticks]
    end_move = max(s for s, v in series if v < series[-1][1]) if series else 0
    wins, a = [], 90.0
    while a + 60 <= end_move:
        x0 = min(series, key=lambda p: abs(p[0] - a)); x1 = min(series, key=lambda p: abs(p[0] - a - 60))
        wins.append((x1[1] - x0[1]) / (x1[0] - x0[0]))
        a += 60
    results.append({"start": start, "windows": wins})
    print(f"  {start}: " + "  ".join(f"{w/1e6:.3f}M" for w in wins), flush=True)
json.dump(results, open(OUT, "w"), indent=1)
allw = [w for r in results for w in r["windows"]]
for r in results:
    w = r["windows"]
    if len(w) > 1:
        print(f"start {r['start']}: {len(w)} windows, mean {st.mean(w)/1e6:.3f}M, "
              f"standard deviation {st.stdev(w)/st.mean(w):.1%}, lowest to highest {(max(w)-min(w))/st.mean(w):.1%}")
if all(len(r["windows"]) > 1 for r in results):
    a, b = (st.mean(r["windows"]) for r in results)
    print(f"between the two starts: {b/a - 1:+.1%}")
