"""Diagnose the stop of 2026-10-06 01:52: completeness's bounded count
(manifestSql over orders_small, 65M records written by 24 fill jobs that were
deleted while running) gave no complete answer in 30 minutes. Repeat exactly
that, through the harness's own create_table / fill_topic, then run the count
and log, every minute: phase, status detail, scaling status, pages read,
rows held, the next-page link, and the topic's log end. Everything prefixed
flink-training; torn down at the end."""
import json, os, sys, time
sys.path.insert(0, os.path.expanduser("~/.claude/skills/scalable-flink-skill/harness"))
import lib as L
import platform_confluent as PC
OUT = sys.argv[1]
cfg = json.load(open("~/code/GitHub/flink-training/docs/skill-validation/cloud-sql-app/pipeline-cloud.json"))
say = lambda *a: print(time.strftime("%H:%M:%S"), *a, flush=True)
raw = dict(cfg["platform"]); raw.update(stateDir=OUT, credentials=os.path.join(OUT, "creds.env"), estimateUsd=8)
p = PC.ConfluentCloud(raw, log=print)
try:
    p.up()
    p.create_table("orders", as_name="orders_small", partitions=40)
    t = time.time(); n = p.fill_topic("orders_small", 20_000_000)
    say(f"fill done: log end {n:,} after {time.time() - t:.0f} s; fill jobs deleted")
    sql = cfg["platform"]["manifestSql"].replace("{topic}", "orders_small")
    p.run_statement("flink-training-count-diag", sql, p.database(), pool=p.state["pool"])
    nm = p.last_statement
    url, pages, rows, t0, last_log = p._statement_url(nm) + "/results", 0, {}, time.time(), 0
    while time.time() - t0 < 2400:
        st, res = p.rest("GET", url, p._rest_auth())
        if st == 200 and isinstance(res, dict):
            pages += 1
            for item in (res.get("results") or {}).get("data") or []:
                rows[str(item.get("row"))] = item.get("op")
            nxt = (res.get("metadata") or {}).get("next")
            if nxt: url = nxt
        if time.time() - last_log > 60:
            last_log = time.time()
            s = ((p.rest("GET", p._statement_url(nm), p._rest_auth())[1] or {}).get("status") or {})
            say(f"count {time.time() - t0:5.0f} s: phase {s.get('phase')}; detail {str(s.get('detail'))[:160]!r}; "
                f"scaling {s.get('scaling_status')}; HTTP {st}; pages {pages}; distinct rows seen {len(rows)}; "
                f"next link {'yes' if st == 200 and (res.get('metadata') or {}).get('next') else 'no'}; "
                f"log end {p.log_end('orders_small')[0]:,}")
            if s.get("phase") in ("COMPLETED", "FAILED", "STOPPED", "DELETED"):
                say("ROWS last op per row:", json.dumps(rows)[:800]); break
        time.sleep(2)
    say("statement record:", json.dumps(p.rest("GET", p._statement_url(nm), p._rest_auth())[1])[:1500])
except L.Refusal as e:
    say("STOPPED:", e.msg[:500])
finally:
    p.down(); say("surviving after down:", p.surviving())
