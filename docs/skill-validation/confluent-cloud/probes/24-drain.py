"""Reproduce cloud completeness's drain on the recount probe's stack, 2026-10-06:
the SQL app's job at 5 CFU through the harness's own set_size and submit, on
orders_small (37,996,160 committed records by the snapshot query and by
Kafka's committed-only consumer). Wait until the output's log end stops, read
Confluent's per-minute records read, count enriched with the snapshot query,
then delete the job and its pool. The probe that owns the stack is paused."""
import json, os, sys, time
sys.path.insert(0, os.path.expanduser("~/.claude/skills/scalable-flink-skill/harness"))
import lib as L
import platform_confluent as PC
D = sys.argv[1]
cfg = json.load(open("~/code/GitHub/flink-training/docs/skill-validation/cloud-sql-app/pipeline-cloud.json"))
say = lambda *a: print(time.strftime("%H:%M:%S"), *a, flush=True)
raw = dict(cfg["platform"]); raw.update(stateDir=D, credentials=os.path.join(D, "creds.env"))
p = PC.ConfluentCloud(raw, log=print)
p.cases = [5, 10, 20]
name = None
try:
    p.create_table("enriched", partitions=40)
    L.P.set_and_read_back(p, 5)
    p.input_now = "orders_small"
    t0 = time.time()
    name = p.submit(5, "sqlapp-complete-repro")
    say(f"job {name} at 5 CFU")
    last, still = None, 0
    while still < 3:
        n = p.log_end("enriched")[0]
        still = still + 1 if n == last else 0
        last = n
        say(f"  enriched log end {n:,}")
        time.sleep(60)
    time.sleep(240)                       # the metrics arrive about three minutes late
    m = p.statement_minutes(name, t0 - 60, time.time())
    say("records read per minute:", m.get("recordsIn"))
    say("SUMS", {k: sum(float(v) for _, v in vs) for k, vs in m.items() if vs and k in ("recordsIn", "recordsOut")})
    rows = p.statement_rows("flink-training-verify-repro", cfg["platform"]["verifySql"])
    say(f"SNAPSHOT enriched: total {sum(int(float(r['n'])) for r in rows):,}; rows {rows}")
except L.Refusal as e:
    say("STOPPED:", e.msg[:600])
finally:
    if name:
        p.cli("flink", "statement", "delete", name, "--cloud", p.cloud, "--region", p.region, "--force")
    cp = p.state.get("casePool")
    if cp:
        p.cli("flink", "compute-pool", "delete", cp, "--force")
        say(f"deleted job and case pool {cp}")
