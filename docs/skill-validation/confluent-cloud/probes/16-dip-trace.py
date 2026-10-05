"""Dip probe, 2026-10-05: the confirming case again (copy at 20 CFU, 40 partitions, alignment off,
baseline 20), traced every 30 s: output log end, phase, scaling status, exceptions.
Named flink-training; torn down at the end."""
import base64, json, os, sys, time, urllib.request, urllib.error
sys.path.insert(0, os.path.expanduser("~/code/GitHub/scalable-flink-skill/harness"))
import lib as L
import platform_confluent as PC

OUT = sys.argv[1]
PARTS, CFU, FILL_JOBS, TARGET = 40, 20, 24, 400_000_000
p = PC.ConfluentCloud({"environment": "flink-training", "stateDir": OUT, "estimateUsd": 12.0, "setupCfu": 30,
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

    say(f"pool size {CFU} CFU ->", L.P.set_and_read_back(p, CFU))
    p.run_statement("flink-training-out-ddl", f"CREATE TABLE out_copy (order_id STRING, symbol INT, account INT, qty INT, "
                    f"price DOUBLE) DISTRIBUTED INTO {PARTS} BUCKETS", db)
    t_case = time.time()
    nm = p.start_job("flink-training-copy", "INSERT INTO out_copy SELECT order_id, symbol, account, qty, price FROM orders "
                     "/*+ OPTIONS('scan.startup.mode'='earliest-offset') */", db, CFU)
    say(f"job {nm} started with alignment off and baseline {CFU}, both read back")
    full = p.wait_at_size(nm, CFU, t_case)
    say(f"the job used its whole {CFU} CFU pool from {full[11:16]} UTC, {time.time() - t_case:.0f} s after it started")
    # Every 30 s for 12 minutes: the output topic's log end (one record per input, so a transport
    # rate), the statement's phase and scaling status over REST, and its exception list.
    trace = []
    while time.time() - t_case < 348 + 720 + 60 and (not trace or time.time() - trace[0]["t"] < 720):
        t = time.time()
        out_end, _ = p.log_end("out_copy")
        st, reply = p.rest("GET", p._statement_url(nm), p._rest_auth())
        status = reply.get("status", {}) if isinstance(reply, dict) else {}
        exc = p.cli_json("flink", "statement", "exception", "list", nm, "--cloud", p.cloud, "--region", p.region) or []
        row = {"t": t, "outEnd": out_end, "phase": status.get("phase"), "scaling": status.get("scaling_status"),
               "exceptions": len(exc), "latestException": (exc[0] if exc else None)}
        if trace:
            row["perS"] = (out_end - trace[-1]["outEnd"]) / (t - trace[-1]["t"])
        trace.append(row)
        say(f"  trace {time.strftime('%H:%M:%S', time.gmtime(t))} UTC: out_copy {out_end:,}"
            + (f" (+{row['perS']:,.0f}/s)" if "perS" in row else "")
            + f"; phase {row['phase']}; scaling {(row['scaling'] or {}).get('scaling_state')}; exceptions {len(exc)}"
            + (f"; latest: {str(exc[0])[:300]}" if exc else ""))
        time.sleep(max(0, 30 - (time.time() - t)))
    record["trace"] = trace
    t_end = time.time()
    time.sleep(200)
    m = p.statement_minutes(nm, t_case - 60, time.time())
    sent = kafka_minutes("io.confluent.kafka.server/sent_bytes", t_case - 60, time.time())
    record.update({"statement": nm, "fullFrom": full, "tCase": t_case, "tEnd": t_end, "minutes": m, "kafkaSent": sent})
    for k in ("cfu", "recordsIn", "pending", "busyMsPerS", "heldBackMsPerS"):
        say(f"    {k}: " + " ".join(f"{t[11:16]}={v:,.0f}" for t, v in m[k]))
    say("    kafka sent MB: " + " ".join(f"{t[11:16]}={v / 1e6:,.0f}" for t, v in sent))
    json.dump(record, open(os.path.join(OUT, "record.json"), "w"), indent=1, default=str)
    looked = os.path.join(OUT, "looked")
    say(f"HOLDING FOR THE QUERY PROFILER: statement {nm}, environment {p.environment}; touch {looked} when done")
    while time.time() - t_end < 900 and not os.path.exists(looked):
        time.sleep(15)
finally:
    p.down()
    print("surviving after down:", p.surviving(), flush=True)
