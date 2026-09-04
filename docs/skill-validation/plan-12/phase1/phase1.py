"""Plan 12, phase 1. Arms: a = noise floor (4c x6), b = host-load (4c x3 with host polling), c = baseline structure."""
import sys, json, os, time, subprocess
sys.path.insert(0, os.path.expanduser("~/.claude/skills/prove-it-scales/harness"))
import lib as L
from lib import T, cfg, log, CaseRefused

arm = sys.argv[1]
c = cfg()
man = json.load(open(os.path.join(c.results, "manifest.json")))
out = {"arm": arm, "build": L.build_hash(), "thresholds": dict(T), "startedAt": time.strftime("%Y-%m-%d %H:%M:%S %Z"), "runs": []}
path = os.path.join(c.results, f"phase1{arm}.json")

def save():
    out["savedAt"] = time.strftime("%Y-%m-%d %H:%M:%S %Z")
    json.dump(out, open(path, "w"), indent=1)

def one(cores, pid, par=None):
    log(f"---- arm {arm}: {cores} cores, pass {pid} ----")
    try:
        rec, _ = L.run_case(cores, pid, f"p1{arm}", None, cores == 2, man)
        st = "OK"
    except CaseRefused as e:
        rec, st = e.rec, "REFUSED"
        rec["refusal"] = {"scope": e.refusal.scope, "message": e.refusal.msg}
    rec["status"] = st
    out["runs"].append(rec); save()
    log(f"  {st} {cores}c {pid}: {rec.get('recordsPerSec')} rec/s cap {rec.get('tmCapFrac')} kafka {rec.get('kafkaCapFrac')} "
        f"srcIdle {rec.get('sourceIdle')} bp {rec.get('sourceBackpressured')} warmup {rec.get('warmup')}")

if arm == "a":
    for i in range(1, 7):
        one(4, f"n{i}")
elif arm == "b":
    poll = subprocess.Popen(["bash", "-c",
        f"while true; do df -g / >/dev/null; docker exec {c.kafka} ls /tmp >/dev/null 2>&1; "
        f"docker exec {c.tm} cat /sys/fs/cgroup/cpu.stat >/dev/null 2>&1; docker stats --no-stream >/dev/null 2>&1; sleep 5; done"])
    out["hostLoad"] = "df -g /; docker exec kafka ls; docker exec tm cat cpu.stat; docker stats --no-stream; every 5 s, whole arm"
    try:
        for i in range(1, 4):
            one(4, f"h{i}")
    finally:
        poll.kill()
save()
print(json.dumps([(r["cores"], r["pass"], r["status"], r.get("recordsPerSec"), r.get("tmCapFrac"), r.get("sourceIdle")) for r in out["runs"]]))
