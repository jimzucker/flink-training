"""Fourth count diagnostic, 2026-10-06. The cloud chain's completeness counted
44,992,903 records on its input with a snapshot query right after the fill,
and 52,206,970 on the job's output after the drain (16% more). Hypothesis:
the fill jobs keep committing after they are deleted (the log end grew ~4M in
the minute after deletion in every fill), so a count taken right after the
fill misses records committed later. Test: the same fill, then the snapshot
count four times, five minutes apart, with the log end beside each. Results
are read page by page, each page once; the end of the results is the page
with no next link. No Flink job runs. Everything prefixed flink-training."""
import json, os, sys, time
sys.path.insert(0, os.path.expanduser("~/.claude/skills/scalable-flink-skill/harness"))
import lib as L
import platform_confluent as PC
OUT = sys.argv[1]
cfg = json.load(open("~/code/GitHub/flink-training/docs/skill-validation/cloud-sql-app/pipeline-cloud.json"))
say = lambda *a: print(time.strftime("%H:%M:%S"), *a, flush=True)
raw = dict(cfg["platform"]); raw.update(stateDir=OUT, credentials=os.path.join(OUT, "creds.env"), estimateUsd=6)
p = PC.ConfluentCloud(raw, log=print)

def snapshot_count(name, sql):
    t0 = time.time()
    p.run_statement(name, sql, p.database(), pool=p.state["pool"], properties={"sql.snapshot.mode": "now"})
    nm = p.last_statement
    st, desc = p.rest("GET", p._statement_url(nm), p._rest_auth())
    cols = [c.get("name") for c in ((((desc or {}).get("status") or {}).get("traits") or {}).get("schema") or {})
            .get("columns", [])]
    rows, url, pages, items = [], p._statement_url(nm) + "/results", 0, 0
    while time.time() - t0 < 900:
        st, res = p.rest("GET", url, p._rest_auth())
        if st == 409:
            time.sleep(2); continue
        if st != 200:
            raise L.Refusal("rig", f"results HTTP {st}: {str(res)[:200]}")
        pages += 1
        for it in (res.get("results") or {}).get("data") or []:
            items += 1
            row, op = tuple(it.get("row") or []), it.get("op", 0)
            if op in (0, 2, None):
                rows.append(row)
            elif row in rows:
                rows.remove(row)
        nxt = (res.get("metadata") or {}).get("next")
        if not nxt:
            break
        url = nxt
        if not (res.get("results") or {}).get("data"):
            time.sleep(2)
    phase = (((p.rest("GET", p._statement_url(nm), p._rest_auth())[1] or {}).get("status") or {}).get("phase"))
    try:
        p.cli("flink", "statement", "delete", nm, "--cloud", p.cloud, "--region", p.region, "--force")
    except L.Refusal:
        pass
    got = [dict(zip(cols, r)) for r in rows]
    return got, sum(int(float(r["n"])) for r in got), pages, items, phase, time.time() - t0
try:
    p.up()
    p.create_table("orders", as_name="orders_small", partitions=40)
    n = p.fill_topic("orders_small", 20_000_000)
    t_fill = time.time()
    say(f"fill jobs deleted; log end {n:,} when the fill stopped")
    sql = cfg["platform"]["manifestSql"].replace("{topic}", "orders_small")
    for k in range(4):
        if k:
            time.sleep(max(0, t_fill + 300 * k - time.time()))
        le = p.log_end("orders_small")[0]
        got, tot, pages, items, phase, s = snapshot_count(f"flink-training-recount-{k}", sql)
        say(f"COUNT {k} at +{time.time() - t_fill:4.0f} s: snapshot total {tot:,}; log end {le:,}; {len(got)} rows "
            f"from {items} items on {pages} pages in {s:.0f} s, phase {phase}; {sorted((r['account'], r['n']) for r in got)}")
except L.Refusal as e:
    say("STOPPED:", e.msg[:600])
finally:
    p.down(); say("surviving after down:", p.surviving())
