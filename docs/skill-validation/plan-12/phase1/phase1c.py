"""Plan 12, phase 1C: baseline structure. 1 core/p=1, 1 core/p=2 (two slots), 2 cores/p=2; three passes, alternating."""
import sys, json, os, time
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "harness"))
import lib as L
from lib import T, cfg, log, CaseRefused
c = cfg()
man = json.load(open(os.path.join(c.results, "manifest.json")))
out = {"arm": "c", "build": L.build_hash(), "thresholds": dict(T), "startedAt": time.strftime("%Y-%m-%d %H:%M:%S %Z"), "runs": []}
path = os.path.join(c.results, "phase1c.json")
cases = [(1, 1), (1, 2), (2, 2)]
def save():
    out["savedAt"] = time.strftime("%Y-%m-%d %H:%M:%S %Z"); json.dump(out, open(path, "w"), indent=1)
for i in range(3):
    order = cases if i % 2 == 0 else list(reversed(cases))
    for cores, par in order:
        pid = f"c{cores}p{par}-{i+1}"
        log(f"---- arm c: {cores} cores, parallelism {par}, pass {i+1} ----")
        try:
            rec, _ = L.run_case(cores, pid, "p1c", None, cores == 1, man, parallelism=par)
            st = "OK"
        except CaseRefused as e:
            rec, st = e.rec, "REFUSED"; rec["refusal"] = {"scope": e.refusal.scope, "message": e.refusal.msg}
        rec["status"] = st; rec["parallelism"] = par
        out["runs"].append(rec); save()
        log(f"  {st} {cores}c/p{par} #{i+1}: {rec.get('recordsPerSec')} rec/s cap {rec.get('tmCapFrac')} srcIdle {rec.get('sourceIdle')} bp {rec.get('sourceBackpressured')} warmupS {(rec.get('warmup') or {}).get('warmupS')}")
save()
print(json.dumps([(r["cores"], r["parallelism"], r["pass"], r["status"], r.get("recordsPerSec"), r.get("tmCapFrac")) for r in out["runs"]]))
