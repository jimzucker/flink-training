"""Parallelism-reading probe, 2026-10-04: one statement copying the built-in sample
stream; ask the metrics API for operator/current_parallelism every way we can think
of, and list the resource attributes it offers. Named flink-training; torn down."""
import base64, json, os, sys, time, urllib.request, urllib.error
sys.path.insert(0, "~/code/GitHub/scalable-flink-skill/harness")
import lib as L
import platform_confluent as PC
OUT = sys.argv[1]
p = PC.ConfluentCloud({"environment": "flink-training", "stateDir": OUT, "estimateUsd": 2.0,
                       "credentials": os.path.join(OUT, "probe-creds.env")}, log=print)
API = "https://api.telemetry.confluent.cloud/v2/metrics/cloud"
say = lambda *a: print(time.strftime("%H:%M:%S"), *a, flush=True)
iso = lambda t: time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(t))

def auth():
    env = dict(l.strip().split("=", 1) for l in open(p.credentials) if "=" in l)
    a = base64.b64encode(f"{env['METRICS_API_KEY']}:{env['METRICS_API_SECRET']}".encode()).decode()
    del env
    return a

def call(path, body=None):
    req = urllib.request.Request(API + path, data=json.dumps(body).encode() if body is not None else None,
                                 headers={"Authorization": "Basic " + auth(), "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()[:300]

try:
    p.up()
    db = [c for c in p.cli_json("kafka", "cluster", "list") if c["id"] == p.state["cluster"]][0]["name"]
    L.P.set_and_read_back(p, 5)
    p.late_table_tries = 21          # up to 10 minutes: run 1 gave up after 2.5 minutes
    t_ddl = time.time()
    p.run_statement("flink-training-copy-ddl", "CREATE TABLE clicks_copy (click_id STRING, user_id INT, url STRING, "
                    "user_agent STRING, view_time INT)", db)
    p.run_statement("flink-training-copy", "INSERT INTO clicks_copy SELECT click_id, user_id, url, user_agent, view_time "
                    "FROM `examples`.`marketplace`.`clicks`", db)
    nm, pool = p.last_statement, p.case_pool()
    say(f"the INSERT ran {time.time() - t_ddl:.0f} s after the CREATE TABLE was sent, as {nm}")
    t0 = time.time()
    say("statement", nm, "running in", pool)
    st, res = call("/descriptors/resources")
    if st == 200:
        for r in res.get("data", []):
            if "flink" in r.get("type", ""):
                say("resource", r["type"], "attributes:", [l.get("key") for l in r.get("labels", [])])
    else:
        say("resource descriptors: HTTP", st, res)
    time.sleep(360)                  # the query API lags about three minutes
    t1 = time.time()
    P = {"field": "resource.compute_pool.id", "op": "EQ", "value": pool}
    S = {"field": "resource.flink_statement.name", "op": "EQ", "value": nm}
    ways = [("pool, no grouping", P, None, None), ("pool, by statement", P, ["resource.flink_statement.name"], None),
            ("statement only", S, None, None), ("statement only, MAX", S, None, "MAX"),
            ("statement only, SUM", S, None, "SUM"), ("pool and statement", {"op": "AND", "filters": [P, S]}, None, None)]
    for metric in ("io.confluent.flink/operator/current_parallelism", "io.confluent.flink/operator/max_parallelism",
                   "io.confluent.flink/operator/num_records_in", "io.confluent.flink/num_records_in"):
        for label, flt, grp, agg in ways:
            a = {"metric": metric}
            if agg:
                a["agg"] = agg
            body = {"aggregations": [a], "filter": flt, "granularity": "PT1M",
                    "intervals": [f"{iso(t0 - 60)}/{iso(t1)}"], "limit": 100}
            if grp:
                body["group_by"] = grp
            st, r = call("/query", body)
            rows = " ".join(f"{x['timestamp'][11:16]}={x['value']}" for x in sorted(r.get("data", []), key=lambda x: x["timestamp"])) if st == 200 else r
            say(f"{metric.split('flink/')[1]:28} {label:22} HTTP {st}: {rows or '(no points)'}"[:300])
finally:
    p.down()
    print("surviving after down:", p.surviving(), flush=True)
