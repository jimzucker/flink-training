"""Experiment 6: does a throwaway job before measuring steady a fresh worker?
Both arms: fresh worker, 180 s before the measured job (idle, or running a throwaway job).
Arm "same worker": one Flink worker, the job cancelled and resubmitted 4 times.
Arm "pinned": 4 fresh workers, each pinned to vCPUs 0-1 inside Docker's VM.
Each start: 120 s warm-up, then four 60 s windows of committed input offsets."""
import json, statistics as st, sys, time
sys.path.insert(0, "<scalable-flink-skill>/harness")
import lib as L

OUT = sys.argv[1]
c = L.cfg()
res = {"cold": [], "pre-warmed": []}


def measure(tag):
    group = f"{c.project}-noise4-{tag}-{int(time.time())}"
    L.delete_group(group)
    L.start_sampler(group)
    t_sub = time.time()
    jid = L.submit_job(2, group, c.ckpt_ms)
    L.wait_running(jid, 2)
    time.sleep(120 + 4 * 60 + 5)
    ticks = [t for t in L.sampler_ticks_since(int(t_sub * 1000)) if t.get("committed", -1) >= 0]
    L.cancel_job(jid)
    L.stop_sampler()
    first = ticks[0]["ts"] / 1000
    series = [(t["ts"] / 1000 - first, t["committed"]) for t in ticks]
    wins = []
    for k in range(4):
        a = 120 + 60 * k
        x0 = min(series, key=lambda p: abs(p[0] - a)); x1 = min(series, key=lambda p: abs(p[0] - a - 60))
        wins.append((x1[1] - x0[1]) / (x1[0] - x0[0]))
    print(f"  {tag}: " + "  ".join(f"{w/1e6:.3f}M" for w in wins) + f"   mean {st.mean(wins)/1e6:.3f}M", flush=True)
    return wins




def throwaway(seconds):
    group = f"{c.project}-noise6-warm-{int(time.time())}"
    L.delete_group(group)
    jid = L.submit_job(2, group, c.ckpt_ms)
    L.wait_running(jid, 2)
    time.sleep(seconds)
    L.cancel_job(jid)


order = ["cold", "pre-warmed"] * 6
for i, arm in enumerate(order):
    L.stop_tm()
    L.start_tm(2, slots=2)
    t = time.time()
    if arm == "pre-warmed":
        throwaway(170)
    time.sleep(max(0, 180 - (time.time() - t)))
    print(f"start {i+1:2} {arm:10} worker age {time.time() - t:.0f}s before the measured job", flush=True)
    res[arm].append(measure(f"{'W' if arm == 'pre-warmed' else 'C'}{i+1}"))
L.stop_tm()

json.dump(res, open(OUT, "w"), indent=1)
import math
for arm, starts in res.items():
    means = [st.mean(w) for w in starts]
    within = st.mean(st.stdev(w) / st.mean(w) for w in starts)
    drops = sum(1 for w in starts if st.mean(w[2:]) < 0.96 * st.mean(w[:2]))
    print(f"{arm:10}: " + ", ".join(f"{m/1e6:.3f}M" for m in means)
          + f" | mean {st.mean(means)/1e6:.3f}M | between starts sd {st.stdev(means)/st.mean(means):.1%}"
          f", lowest to highest {(max(means)-min(means))/st.mean(means):.1%} | within a start sd {within:.1%}"
          f" | fell more than 4% during the run: {drops} of {len(starts)}")
a = [st.mean(w) for w in res["cold"]]; b = [st.mean(w) for w in res["pre-warmed"]]
se = math.sqrt(st.variance(a)/len(a) + st.variance(b)/len(b))
print(f"pre-warmed against cold: {st.mean(b)/st.mean(a)-1:+.1%}, {(st.mean(b)-st.mean(a))/se:+.1f} standard errors")
