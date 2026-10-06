"""Third count diagnostic, 2026-10-06. Probe 2 measured that a plain bounded
GROUP BY gives two change-log items per input record, read at about 500,000
records a minute: ~140 minutes for 70M. Looking for a count form that gives
only its final rows. Cheapest level first: each form on a three-row table
with a known answer; only forms that give it are then timed on the ~70M
backlog the harness's fill writes.
 F0 plain GROUP BY (control; worked in run 21)
 F1 the same as a snapshot query (statement property sql.snapshot.mode=now)
 F2 a view carrying the bounded hint, counted in a tumbling window on $rowtime
Everything prefixed flink-training; torn down at the end."""
import json, os, sys, time
sys.path.insert(0, os.path.expanduser("~/.claude/skills/scalable-flink-skill/harness"))
import lib as L
import platform_confluent as PC
OUT = sys.argv[1]
cfg = json.load(open("~/code/GitHub/flink-training/docs/skill-validation/cloud-sql-app/pipeline-cloud.json"))
say = lambda *a: print(time.strftime("%H:%M:%S"), *a, flush=True)
raw = dict(cfg["platform"]); raw.update(stateDir=OUT, credentials=os.path.join(OUT, "creds.env"), estimateUsd=8)
p = PC.ConfluentCloud(raw, log=print)
BOUNDED = "/*+ OPTIONS('scan.startup.mode'='earliest-offset', 'scan.bounded.mode'='latest-offset') */"
AGG = "account, COUNT(*) AS n, SUM(CAST(qty AS BIGINT)) AS qty"

def count(name, sql, props=None, limit_s=900):
    """Run, read the change log to the end, return (phase, seconds, items, final rows)."""
    t0 = time.time()
    try:
        p.run_statement(name, sql, p.database(), pool=p.state["pool"], properties=props, timeout_s=600)
    except L.Refusal as e:
        return f"did not run: {e.msg[:300]}", time.time() - t0, 0, []
    nm = p.last_statement
    st, desc = p.rest("GET", p._statement_url(nm), p._rest_auth())
    cols = [c.get("name") for c in ((((desc or {}).get("status") or {}).get("traits") or {}).get("schema") or {})
            .get("columns", [])]
    rows, items, url, phase = [], 0, p._statement_url(nm) + "/results", None
    while time.time() - t0 < limit_s:
        st, res = p.rest("GET", url, p._rest_auth())
        if st == 200 and isinstance(res, dict):
            for it in (res.get("results") or {}).get("data") or []:
                items += 1
                row, op = tuple(it.get("row") or []), it.get("op", 0)
                if op in (0, 2, None):
                    rows.append(row)
                elif row in rows:
                    rows.remove(row)
            nxt = (res.get("metadata") or {}).get("next")
            if nxt:
                url = nxt
                if (res.get("results") or {}).get("data"):
                    continue
        phase = (((p.rest("GET", p._statement_url(nm), p._rest_auth())[1] or {}).get("status") or {}).get("phase"))
        if phase in ("COMPLETED", "FAILED", "STOPPED", "DELETED"):
            break
        time.sleep(2)
    try:
        p.cli("flink", "statement", "delete", nm, "--cloud", p.cloud, "--region", p.region, "--force")
    except L.Refusal:
        pass
    keep = [c for c in cols if c in ("account", "n", "qty")]
    out = [{c: r[cols.index(c)] for c in keep} for r in rows] if cols else [list(r) for r in rows]
    return phase or "still running", time.time() - t0, items, out

def forms(table):
    return [("F0 plain GROUP BY", f"SELECT {AGG} FROM `{table}` {BOUNDED} GROUP BY account", None),
            ("F1 snapshot query", f"SELECT {AGG} FROM `{table}` GROUP BY account", {"sql.snapshot.mode": "now"}),
            ("F2 view + window", f"SELECT {AGG} FROM TABLE(TUMBLE(TABLE `v_{table}`, DESCRIPTOR($rowtime), "
                                 f"INTERVAL '3650' DAYS)) GROUP BY window_start, window_end, account", None)]
try:
    p.up()
    db, pool = p.database(), p.state["pool"]
    p.run_statement("flink-training-tiny-ddl", "CREATE TABLE t_tiny (account INT, qty INT) DISTRIBUTED INTO 4 BUCKETS",
                    db, pool=pool)
    p.run_statement("flink-training-tiny-ins", "INSERT INTO t_tiny VALUES (1, 10), (1, 5), (2, 3)", db, pool=pool)
    time.sleep(20)
    p.run_statement("flink-training-tiny-view", f"CREATE VIEW v_t_tiny AS SELECT * FROM t_tiny {BOUNDED}", db, pool=pool)
    want = sorted([(1, 2, 15), (2, 1, 3)])
    works = []
    for label, sql, props in forms("t_tiny"):
        ph, s, items, rows = count("flink-training-" + label.split()[0].lower() + "-tiny", sql, props, limit_s=300)
        got = sorted(tuple(int(float(r[k])) for k in ("account", "n", "qty")) for r in rows if isinstance(r, dict) and len(r) == 3)
        ok = ph == "COMPLETED" and got == want
        say(f"TINY {label}: {ph} in {s:.0f} s; {items} change-log items; rows {rows} -> {'RIGHT' if ok else 'not right'}")
        if ok and not label.startswith("F0"):
            works.append(label)
    if not works:
        say("no form other than the control gave the answer on the tiny table; stopping before the fill")
    else:
        p.create_table("orders", as_name="orders_small", partitions=40)
        n = p.fill_topic("orders_small", 20_000_000)
        time.sleep(60)
        say(f"fill done: log end {n:,} at stop, {p.log_end('orders_small')[0]:,} a minute later")
        if any(w.startswith("F2") for w in works):
            p.run_statement("flink-training-big-view", f"CREATE VIEW v_orders_small AS SELECT * FROM orders_small {BOUNDED}",
                            db, pool=pool)
        for label, sql, props in forms("orders_small"):
            if label in works:
                ph, s, items, rows = count("flink-training-" + label.split()[0].lower() + "-big", sql, props, limit_s=1800)
                tot = sum(int(float(r["n"])) for r in rows if isinstance(r, dict) and "n" in r)
                say(f"BIG {label}: {ph} in {s:.0f} s; {items:,} change-log items; {len(rows)} rows; total n {tot:,}; {rows}")
except L.Refusal as e:
    say("STOPPED:", e.msg[:600])
finally:
    p.down(); say("surviving after down:", p.surviving())
