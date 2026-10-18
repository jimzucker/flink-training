"""4-core tiny case, the dashboard's reader (kafkametrics) off and on, alternating.
One stack, one fill, one build; the only change between cases is whether the
reader container is running."""
import json, os, subprocess, sys, time, urllib.request, urllib.parse
H = os.path.expanduser("~/.claude/skills/scalable-flink-skill/harness")
sys.path.insert(0, H)
import lib as L
from lib import T, cfg, CaseRefused

os.chdir(sys.argv[1])
c = cfg()
KM = f"{c.project}-kafkametrics"
PROM = f"http://localhost:{c.dash['ports']['prometheus']}"
out = {"arms": [], "build": None}

def say(m):
    print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)

def running(name):
    r = subprocess.run(["docker", "inspect", "-f", "{{.State.Running}}", name], capture_output=True, text=True)
    return r.stdout.strip() == "true"

def reader_total():
    q = urllib.parse.quote("sum(kafka_reader_records_total)")
    d = json.loads(urllib.request.urlopen(f"{PROM}/api/v1/query?query={q}", timeout=10).read())
    res = d["data"]["result"]
    return float(res[0]["value"][1]) if res else None

L.stack_up()
say(f"stack up; reader running: {running(KM)}")
man = L.fill("orders-tiny", c.tiny, c.seed + 1, "manifest-tiny.json")
L._CFG.topic_in = "orders-tiny"
T.update(warmupMinS=20.0, minBoundaries=3, minWindowS=30.0, reporterS=2)
try:
    for i, arm in enumerate(["off", "on"] * 3):
        subprocess.run(["docker", "stop" if arm == "off" else "start", KM], check=True, capture_output=True)
        time.sleep(15)
        is_running = running(KM)
        if is_running != (arm == "on"):
            raise SystemExit(f"STOPPED: reader should be {arm} but running={is_running}")
        before = reader_total() if arm == "on" else None
        say(f"case {i+1}/6 arm={arm} reader running={is_running} reader records so far={before}")
        try:
            rec, _ = L.run_case(4, "ab", f"ab-{i+1}-{arm}", None, True, man, warmup_max_s=120.0, reporter_s=2)
            status = "measured"
        except CaseRefused as e:
            rec, status = e.rec, f"thrown out: {e}"
        after = reader_total() if arm == "on" else None
        row = {"case": i + 1, "arm": arm, "status": status, "recordsPerSec": rec.get("recordsPerSec"),
               "otherCoresOpen": rec.get("otherCoresOpen"), "otherCoresClose": rec.get("otherCoresClose"),
               "hostSwapOpenMB": rec.get("hostSwapOpenMB"), "tmCapFrac": rec.get("tmCapFrac"),
               "readerRecordsBefore": before, "readerRecordsAfter": after, "readerRunningAtStart": is_running}
        out["arms"].append(row)
        json.dump(out, open("results/ab.json", "w"), indent=2)
        say(f"  -> {row}")
finally:
    L.stack_down()
    say("AB DONE")
