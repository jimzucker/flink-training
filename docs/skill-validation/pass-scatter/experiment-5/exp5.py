"""Experiment 5: pinned against unpinned fresh workers, alternated.
Arm "same worker": one Flink worker, the job cancelled and resubmitted 4 times.
Arm "pinned": 4 fresh workers, each pinned to vCPUs 0-1 inside Docker's VM.
Each start: 120 s warm-up, then four 60 s windows of committed input offsets."""
import json, statistics as st, sys, time
sys.path.insert(0, "<scalable-flink-skill>/harness")
import lib as L

OUT = sys.argv[1]
c = L.cfg()
res = {"unpinned": [], "pinned": []}


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



order = ["unpinned", "pinned"] * 6
for i, arm in enumerate(order):
    L.stop_tm()
    L.start_tm(2, slots=2)
    if arm == "pinned":
        L.sh(f"docker update --cpuset-cpus 0-1 {c.tm}")
    got = L.sh(f"docker exec {c.tm} cat /sys/fs/cgroup/cpuset.cpus.effective", check=False).stdout.strip()
    want = "0-1" if arm == "pinned" else "0-7"
    print(f"start {i+1:2} {arm:8} cpus {got}", flush=True)
    if got != want:
        raise SystemExit(f"start {i+1}: expected cpus {want}, the worker reports {got!r}")
    res[arm].append(measure(f"{'P' if arm == 'pinned' else 'U'}{i+1}"))
L.stop_tm()

json.dump(res, open(OUT, "w"), indent=1)
import math
for arm, starts in res.items():
    means = [st.mean(w) for w in starts]
    within = st.mean(st.stdev(w) / st.mean(w) for w in starts)
    print(f"{arm:9}: " + ", ".join(f"{m/1e6:.3f}M" for m in means)
          + f" | mean {st.mean(means)/1e6:.3f}M | between starts sd {st.stdev(means)/st.mean(means):.1%}"
          f", lowest to highest {(max(means)-min(means))/st.mean(means):.1%} | within a start sd {within:.1%}")
u = [st.mean(w) for w in res["unpinned"]]; p = [st.mean(w) for w in res["pinned"]]
se = math.sqrt(st.variance(u)/len(u) + st.variance(p)/len(p))
print(f"pinned against unpinned: {st.mean(p)/st.mean(u)-1:+.1%}, {(st.mean(p)-st.mean(u))/se:+.1f} standard errors")
