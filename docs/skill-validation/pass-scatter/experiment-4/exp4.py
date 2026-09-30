"""Experiment 4: what sets a fresh start's speed?
Arm "same worker": one Flink worker, the job cancelled and resubmitted 4 times.
Arm "pinned": 4 fresh workers, each pinned to vCPUs 0-1 inside Docker's VM.
Each start: 120 s warm-up, then four 60 s windows of committed input offsets."""
import json, statistics as st, sys, time
sys.path.insert(0, "<scalable-flink-skill>/harness")
import lib as L

OUT = sys.argv[1]
c = L.cfg()
res = {"same worker": [], "pinned": []}


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


print("arm: same worker, job restarted 4 times", flush=True)
L.stop_tm()
L.start_tm(2, slots=2)
for i in range(4):
    res["same worker"].append(measure(f"J{i+1}"))
L.stop_tm()

print("arm: fresh worker each time, pinned to vCPUs 0-1", flush=True)
for i in range(4):
    L.stop_tm()
    L.start_tm(2, slots=2)
    L.sh(f"docker update --cpuset-cpus 0-1 {c.tm}")
    got = L.sh(f"docker exec {c.tm} cat /sys/fs/cgroup/cpuset.cpus.effective", check=False).stdout.strip()
    print(f"  worker {i+1} pinned to: {got!r}", flush=True)
    if got not in ("0-1", "0,1"):
        raise SystemExit(f"pinning did not apply: {got!r}")
    res["pinned"].append(measure(f"P{i+1}"))
L.stop_tm()

json.dump(res, open(OUT, "w"), indent=1)
for arm, starts in res.items():
    means = [st.mean(w) for w in starts]
    within = st.mean(st.stdev(w) / st.mean(w) for w in starts)
    print(f"{arm:12}: start averages " + ", ".join(f"{m/1e6:.3f}M" for m in means)
          + f" | between starts {(max(means)-min(means))/st.mean(means):.1%} lowest to highest,"
          f" sd {st.stdev(means)/st.mean(means):.1%} | within a start sd {within:.1%}")
