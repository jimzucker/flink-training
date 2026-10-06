"""Live harness case, 2026-10-06: one 20 CFU case through the harness's own run_case_cloud (submit,
measure_window, check_case_cloud) on a filled 40-partition stack, 50 eCKU.
Named flink-training; torn down at the end."""
import base64, json, os, sys, time, urllib.request, urllib.error
sys.path.insert(0, os.path.expanduser("~/.claude/skills/scalable-flink-skill/harness"))
import lib as L
import platform_confluent as PC

OUT = sys.argv[1]
PARTS, CFU, FILL_JOBS, TARGET = 40, 20, 24, 400_000_000
p = PC.ConfluentCloud({"environment": "flink-training", "stateDir": OUT, "estimateUsd": 20.0, "setupCfu": 30, "maxEcku": 50,
                       "jobSql": "INSERT INTO out_copy SELECT order_id, symbol, account, qty, price FROM orders /*+ OPTIONS('scan.startup.mode'='earliest-offset') */",
                       "_cases": [5, 10, 20], "_topicIn": "orders",
                       "credentials": os.path.join(OUT, "probe-creds.env")}, log=print)
say = lambda *a: print(time.strftime("%H:%M:%S"), *a, flush=True)
iso = lambda t: time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(t))
V50 = ",".join(f"({i})" for i in range(1, 51)); V40 = ",".join(f"({i})" for i in range(1, 41))
record = {}

def kafka_minutes(metric, t0, t1):
    env = dict(l.strip().split("=", 1) for l in open(p.credentials) if "=" in l)
    auth = base64.b64encode(f"{env['METRICS_API_KEY']}:{env['METRICS_API_SECRET']}".encode()).decode(); del env
    body = {"aggregations": [{"metric": metric}], "filter": {"field": "resource.kafka.id", "op": "EQ", "value": p.state["cluster"]},
            "granularity": "PT1M", "intervals": [f"{iso(t0)}/{iso(t1)}"], "limit": 1000}
    st, r = p.http(f"{PC.METRICS_API}/query", auth, body)
    return PC.minutes_of(r) if st == 200 else []

try:
    p.up()
    # Exercise the baseline's REST path before anything is spent on the fill.
    st, reply = p.rest("GET", p._statement_url(f"{p.prefix}-ready0"), p._rest_auth())
    say(f"REST check: organization {p._organization()}, statement read over REST: HTTP {st}")
    if st != 200:
        raise L.Refusal("rig", f"the Flink REST API did not answer for a statement that exists: HTTP {st} {str(reply)[:200]}")
    db = [c for c in p.cli_json("kafka", "cluster", "list") if c["id"] == p.state["cluster"]][0]["name"]
    p.run_statement("flink-training-orders-ddl", "CREATE TABLE orders (order_id STRING, symbol INT, account INT, "
                    f"qty INT, price DOUBLE) DISTRIBUTED INTO {PARTS} BUCKETS", db, pool=p.state["pool"])
    fills = []
    for i in range(1, FILL_JOBS + 1):
        p.run_statement(f"flink-training-base{i}",
            f"CREATE TABLE fake_base_{i} (order_id STRING, symbol INT, account INT, qty INT, price DOUBLE) WITH ("
            f"'connector' = 'faker', 'number-of-rows' = '100000', 'rows-per-second' = '100000', "
            f"'fields.order_id.expression' = '#{{Internet.uuid}}', "
            f"'fields.symbol.expression' = '#{{number.numberBetween ''0'',''4096''}}', "
            f"'fields.account.expression' = '#{{number.numberBetween ''0'',''4''}}', "
            f"'fields.qty.expression' = '#{{number.numberBetween ''1'',''100''}}', "
            f"'fields.price.expression' = '#{{number.randomDouble ''2'',''1'',''500''}}')", db, pool=p.state["pool"])
    for i in range(1, FILL_JOBS + 1):
        p.run_statement(f"flink-training-amp{i}",
            "INSERT INTO orders SELECT CONCAT(b.order_id, '-', CAST(a.x AS STRING), '-', CAST(c.y AS STRING)), "
            f"b.symbol, b.account, b.qty, b.price FROM fake_base_{i} AS b CROSS JOIN (VALUES {V50}) AS a(x) "
            f"CROSS JOIN (VALUES {V40}) AS c(y)", db, pool=p.state["pool"])
        fills.append(p.last_statement)
    say(f"{FILL_JOBS} fill jobs running")
    t0 = time.time(); first = None
    while True:
        time.sleep(120)
        n, per = p.log_end("orders")
        say(f"  fill, {time.time() - t0:.0f} s: orders holds {n:,} in {len(per)} partitions")
        first = first or (time.time(), n)
        if n >= TARGET or time.time() - t0 > 3000:
            break
    say(f"fill: {(n - first[1]) / (time.time() - first[0]):,.0f} records/s written by {FILL_JOBS} jobs")
    for nm in fills:
        p.cli("flink", "statement", "delete", nm, "--cloud", p.cloud, "--region", p.region, "--force")
    time.sleep(30)
    n, per = p.log_end("orders"); record["backlog"] = n
    say(f"backlog: orders holds {n:,} records; partition guard:", p.check_partitions("orders", [5, 10, 20]), "partitions accepted")

    # ---- one case through the harness's own run_case_cloud
    import types
    p.run_statement("flink-training-out-ddl", f"CREATE TABLE out_copy (order_id STRING, symbol INT, account INT, "
                    f"qty INT, price DOUBLE) DISTRIBUTED INTO {PARTS} BUCKETS", db, pool=p.state["pool"])
    L._CFG = types.SimpleNamespace(plat=p, project="flink-training", out_per_in=1.0, topics_out=["out_copy"])
    try:
        rec, _ = L.run_case_cloud(20, "live1", "live", None, False, {})
        say("CASE OK")
    except L.CaseRefused as e:
        rec = e.rec
        say(f"CASE {rec.get('status')}: {e.refusal.msg}")
    show = {k: rec.get(k) for k in ("status", "recordsPerSec", "recordsReadPerSec", "vantageDisagreement",
                                    "tmCores", "tmCapFrac", "kafkaEcku", "kafkaEckuLimit", "kafkaEckuMinutesAtLimit",
                                    "sourceBusy", "sourceBackpressured", "sourceIdle", "backlogRemaining",
                                    "headroomS", "wholeMinutes", "elapsedS", "readings", "jobId")}
    say("RECORD " + json.dumps(show, default=str))
    try:
        say("BOTTLENECK " + L.bottleneck(rec))
    except Exception as e:
        say(f"BOTTLENECK could not be worded: {e!r}")
    record["case"] = rec
    json.dump(record, open(os.path.join(OUT, "record.json"), "w"), indent=1, default=str)

    # ---- the Flink REST results API, for counting a backlog the way a reader sees it
    try:
        p.run_statement("flink-training-count", "SELECT COUNT(*) AS n, account, SUM(CAST(qty AS BIGINT)) AS qty "
                        "FROM orders /*+ OPTIONS('scan.startup.mode'='earliest-offset', "
                        "'scan.bounded.mode'='latest-offset') */ GROUP BY account", db, pool=p.state["pool"],
                        timeout_s=1800)
        nm = p.last_statement
        for _ in range(120):
            st, desc = p.rest("GET", p._statement_url(nm), p._rest_auth())
            phase = (desc.get("status") or {}).get("phase") if isinstance(desc, dict) else None
            if phase in ("COMPLETED", "FAILED", "STOPPED"):
                break
            time.sleep(15)
        say(f"COUNT statement phase {phase}; status keys {sorted((desc.get('status') or {}).keys()) if isinstance(desc, dict) else desc}")
        say("COUNT schema " + json.dumps(((desc.get("status") or {}).get("traits") or {}).get("schema"))[:800])
        url = p._statement_url(nm) + "/results"
        pages = 0
        while url and pages < 20:
            st, res = p.rest("GET", url, p._rest_auth())
            pages += 1
            say(f"COUNT results page {pages}: HTTP {st}; keys {sorted(res.keys()) if isinstance(res, dict) else type(res)}; "
                + json.dumps(res)[:1500])
            nxt = ((res.get("metadata") or {}).get("next") if isinstance(res, dict) else None)
            url = nxt if nxt and nxt != url else None
        p.cli("flink", "statement", "delete", nm, "--cloud", p.cloud, "--region", p.region, "--force")
    except Exception as e:
        say(f"COUNT check stopped: {e!r}"[:600])
finally:
    p.down()
    print("surviving after down:", p.surviving(), flush=True)
