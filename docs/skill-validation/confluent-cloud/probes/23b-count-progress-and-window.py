"""Second count diagnostic, 2026-10-06. The first showed a plain bounded
GROUP BY account over orders_small (about 70M records) giving a change log
of ~450,000 distinct updated rows a minute through the REST results API,
still running after 12 minutes. On one stack and one fill, two counts:
 A. the same GROUP BY for 6 minutes, logging every minute the largest n seen
    per account -- how far through the input the count has got;
 B. the same count as a tumbling window on $rowtime wide enough to hold the
    whole topic, read through the harness's statement_rows to completion,
    logging how long it took and the rows it gave.
Expected if the change log is the cause: A advances far slower than the
input; B gives 4 rows in minutes. Everything prefixed flink-training."""
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
try:
    p.up()
    p.create_table("orders", as_name="orders_small", partitions=40)
    t = time.time(); n = p.fill_topic("orders_small", 20_000_000)
    time.sleep(60)
    say(f"fill done: log end {n:,} at stop, {p.log_end('orders_small')[0]:,} a minute later; {time.time() - t:.0f} s")
    # A
    p.run_statement("flink-training-count-a", cfg["platform"]["manifestSql"].replace("{topic}", "orders_small"),
                    p.database(), pool=p.state["pool"])
    nm = p.last_statement
    cols = None
    url, t0, last, items, best = p._statement_url(nm) + "/results", time.time(), 0, 0, {}
    while time.time() - t0 < 360:
        st, res = p.rest("GET", url, p._rest_auth())
        if st == 200 and isinstance(res, dict):
            for it in (res.get("results") or {}).get("data") or []:
                items += 1
                row = it.get("row") or []
                if it.get("op") in (0, 2) and len(row) >= 2:
                    best[str(row[0])] = max(best.get(str(row[0]), 0), int(float(row[1])))
            url = (res.get("metadata") or {}).get("next") or url
        if time.time() - last > 60:
            last = time.time()
            say(f"A {time.time() - t0:4.0f} s: change-log items read {items:,}; largest n per account {best}; "
                f"sum {sum(best.values()):,}")
    p.cli("flink", "statement", "delete", nm, "--cloud", p.cloud, "--region", p.region, "--force")
    say(f"A deleted after 6 minutes: {sum(best.values()):,} of the input counted, {items:,} change-log items read")
    # B
    sqlb = ("SELECT account, COUNT(*) AS n, SUM(CAST(qty AS BIGINT)) AS qty FROM TABLE(TUMBLE(TABLE `orders_small` "
            + BOUNDED + ", DESCRIPTOR($rowtime), INTERVAL '3650' DAYS)) GROUP BY window_start, window_end, account")
    t0 = time.time()
    try:
        rows = p.statement_rows("flink-training-count-b", sqlb, timeout_s=1800)
    except L.Refusal as e:
        # A hint inside TABLE(...) may not be accepted; set the bound on the table instead.
        say("B with the hint inside TUMBLE stopped:", e.msg[:400])
        p.run_statement("flink-training-bound-b", "ALTER TABLE `orders_small` SET ('scan.startup.mode' = "
                        "'earliest-offset', 'scan.bounded.mode' = 'latest-offset')", p.database(), pool=p.state["pool"])
        t0 = time.time()
        rows = p.statement_rows("flink-training-count-b2", sqlb.replace(" " + BOUNDED, ""), timeout_s=1800)
    say(f"B COMPLETED in {time.time() - t0:.0f} s: {len(rows)} rows: {rows}; total n {sum(int(float(r['n'])) for r in rows):,}")
except L.Refusal as e:
    say("STOPPED:", e.msg[:600])
finally:
    p.down(); say("surviving after down:", p.surviving())
