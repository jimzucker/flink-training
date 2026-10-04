"""Cloud fill and drain probe, 2026-10-03. Named flink-training; everything deleted at the end.

Fill: one amplification job alone, then eight side by side, into one topic `orders`.
Drain: a per-key sum reads `orders` from the start at 5, 10 and 20 CFU.
Readings per sample: output topic end offsets, every consumer group's committed
offsets on `orders`, CFU in use (Prometheus export). After the cases: per-minute
records in, busy, back-pressure and idle per statement (Metrics query API).
Secrets stay in owner-only files that are deleted at the end; none are printed."""
import base64, json, os, re, subprocess, sys, time, urllib.request, urllib.error
sys.path.insert(0, "~/code/GitHub/scalable-flink-skill/harness")
import lib as L
import platform_confluent as PC

OUT = sys.argv[1]
creds = os.path.join(OUT, "probe-creds.env")
client = os.path.join(OUT, "client.properties")
secret_file = os.path.join(OUT, "cloud-key.secret")
API = "https://api.telemetry.confluent.cloud/v2/metrics/cloud"
PARTS = 24
p = PC.ConfluentCloud({"environment": "flink-training", "stateDir": OUT, "estimateUsd": 10.0,
                       "credentials": creds}, log=print)
cloud_key = None
extra_pools = []
say = lambda *a: print(time.strftime("%H:%M:%S"), *a, flush=True)
record = {"fill": [], "cases": []}


def call(url, auth, body=None):
    req = urllib.request.Request(url, data=json.dumps(body).encode() if body else None,
                                 headers={"Authorization": "Basic " + auth, "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            return r.status, r.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()


def kafka_tool(*args):
    r = subprocess.run(["docker", "run", "--rm", "-v", f"{client}:/tmp/client.properties:ro",
                        "apache/kafka:3.9.2", f"/opt/kafka/bin/{args[0]}", "--bootstrap-server", BOOT,
                        "--command-config", "/tmp/client.properties", *args[1:]],
                       capture_output=True, text=True, timeout=240)
    return r.returncode, r.stdout, r.stderr


def end_offsets(topic):
    rc, out, err = kafka_tool("kafka-get-offsets.sh", "--topic", topic)
    if rc:
        return None, None, err.strip()[-300:]
    rows = [l for l in out.split() if l.count(":") >= 2]
    return sum(int(l.rsplit(":", 1)[1]) for l in rows), len(rows), ""


def committed_on(topic):
    """Every consumer group's committed offsets on `topic`: {group: total}."""
    rc, out, err = kafka_tool("kafka-consumer-groups.sh", "--describe", "--all-groups")
    if rc:
        return {"_error": err.strip()[-300:]}
    groups = {}
    for line in out.splitlines():
        f = line.split()
        if len(f) >= 4 and f[1] == topic and f[3].isdigit():
            groups[f[0]] = groups.get(f[0], 0) + int(f[3])
    return groups


def cfus():
    st, body = call(f"{API}/export?" + "&".join(f"resource.compute_pool.id={x}" for x in [fill_pool] + extra_pools), AUTH)
    if st != 200:
        return {"_http": st}
    got = {}
    for line in body.splitlines():
        m = re.match(r'confluent_flink_(compute_pool|statement)_utilization_current_cfus\{(.*)\} ([0-9.]+)', line)
        if m:
            lab = dict(re.findall(r'(\w+)="([^"]*)"', m.group(2)))
            key = (lab.get("flink_statement_name") or lab.get("statement_name") or "statement?") \
                if m.group(1) == "statement" else "pool " + lab.get("compute_pool_name", "?")
            got[key] = got.get(key, 0) + float(m.group(3))
    return got


def query(metric, t0, t1, group=("resource.flink_statement.name",)):
    iv = f"{time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(t0))}/{time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(t1))}"
    body = {"aggregations": [{"metric": metric}], "group_by": list(group),
            "filter": {"op": "OR", "filters": [{"field": "resource.compute_pool.id", "op": "EQ", "value": x}
                                               for x in [fill_pool] + extra_pools]},
            "granularity": "PT1M", "intervals": [iv], "limit": 1000}
    st, txt = call(f"{API}/query", AUTH, body)
    try:
        return st, json.loads(txt)
    except ValueError:
        return st, {"raw": txt[:300]}


def run_retrying(name, sql):
    for attempt in range(1, 6):
        nm = f"{name}t{attempt}"
        try:
            p.run_statement(nm, sql, DB)
            return nm
        except L.Refusal as e:
            if "Cannot find table" not in e.msg or attempt == 5:
                raise
            say(f"  {nm}: the new table was not visible yet; retrying in 30 s")
            p.cli("flink", "statement", "delete", nm, "--cloud", p.cloud, "--region", p.region, "--force")
            time.sleep(30)


def delete(nm):
    p.cli("flink", "statement", "delete", nm, "--cloud", p.cloud, "--region", p.region, "--force")


V50 = ",".join(f"({i})" for i in range(1, 51))
V40 = ",".join(f"({i})" for i in range(1, 41))


def base(i):
    p.run_statement(f"flink-training-base{i}",
        f"CREATE TABLE fake_base_{i} (order_id STRING, symbol INT, account INT, qty INT, price DOUBLE) WITH ("
        f"'connector' = 'faker', 'number-of-rows' = '100000', 'rows-per-second' = '100000', "
        f"'fields.order_id.expression' = '#{{Internet.uuid}}', "
        f"'fields.symbol.expression' = '#{{number.numberBetween ''0'',''4096''}}', "
        f"'fields.account.expression' = '#{{number.numberBetween ''0'',''4''}}', "
        f"'fields.qty.expression' = '#{{number.numberBetween ''1'',''100''}}', "
        f"'fields.price.expression' = '#{{number.randomDouble ''2'',''1'',''500''}}')", DB)


def amp(i):
    return run_retrying(f"flink-training-amp{i}",
        f"INSERT INTO orders SELECT CONCAT(b.order_id, '-', CAST(a.x AS STRING), '-', CAST(c.y AS STRING)), "
        f"b.symbol, b.account, b.qty, b.price FROM fake_base_{i} AS b "
        f"CROSS JOIN (VALUES {V50}) AS a(x) CROSS JOIN (VALUES {V40}) AS c(y)")


def fill_window(label, jobs, spans):
    t0 = time.time(); samples = []
    for wait in spans:
        time.sleep(max(0, t0 + wait - time.time()))
        n, parts, err = end_offsets("orders")
        cf = cfus()
        say(f"  fill, {label}, {time.time() - t0:.0f} s: orders holds {n if n is None else f'{n:,}'} "
            f"in {parts} partitions; CFU in use {cf} {err}")
        if n is not None:
            samples.append((time.time(), n))
    (ta, na), (tb, nb) = samples[0], samples[-1]
    rate = (nb - na) / (tb - ta)
    say(f"fill, {label}: {rate:,.0f} records/s written, over {tb - ta:.0f} s")
    record["fill"].append({"label": label, "jobs": jobs, "rate": rate, "overS": tb - ta})


try:
    p.up()
    env = dict(l.strip().split("=", 1) for l in open(creds) if "=" in l)
    BOOT = env["KAFKA_BOOTSTRAP"]
    fd = os.open(client, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as f:
        f.write("security.protocol=SASL_SSL\nsasl.mechanism=PLAIN\n"
                f"sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required "
                f"username=\"{env['KAFKA_API_KEY']}\" password=\"{env['KAFKA_API_SECRET']}\";\n")
    del env
    k = p.cli_json("api-key", "create", "--resource", "cloud", "--description", "flink-training metrics", quiet=True)
    cloud_key = k["api_key"]
    fd = os.open(secret_file, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as f:
        f.write(base64.b64encode(f"{k['api_key']}:{k['api_secret']}".encode()).decode())
    del k
    AUTH = open(secret_file).read().strip()
    DB = [c for c in p.cli_json("kafka", "cluster", "list") if c["id"] == p.state["cluster"]][0]["name"]

    # ---- fill
    fill_pool = p.state["pool"]
    say("pool size 20 CFU ->", L.P.set_and_read_back(p, 20))
    p.run_statement("flink-training-orders-ddl",
        f"CREATE TABLE orders (order_id STRING, symbol INT, account INT, qty INT, price DOUBLE) "
        f"DISTRIBUTED INTO {PARTS} BUCKETS", DB)
    for i in range(1, 9):
        base(i)
    t_fill = time.time()
    fills = [amp(i) for i in range(1, 9)]
    say("eight fill jobs running:", ", ".join(fills))
    fill_window("eight jobs", 8, (60, 300, 540))
    for nm in fills:
        delete(nm)
    time.sleep(30)
    n_total, parts, err = end_offsets("orders")
    say(f"backlog: orders holds {n_total:,} records in {parts} partitions {err}")
    record["backlog"] = {"records": n_total, "partitions": parts}

    # ---- drain
    for cfu in (5, 10, 20):
        pl = p.cli_json("flink", "compute-pool", "create", f"flink-training-drain{cfu}", "--cloud", p.cloud,
                        "--region", p.region, "--max-cfu", str(cfu))
        extra_pools.append(pl["id"])
        p._wait(lambda: p.cli_json("flink", "compute-pool", "describe", pl["id"]), f"pool {pl['id']}")
        p.state["pool"] = pl["id"]
        say(f"new pool {pl['id']} for this case; its size read back:", p.read_size())
        p.run_statement(f"flink-training-sums{cfu}-ddl",
            f"CREATE TABLE sums_{cfu} (account INT, symbol INT, qty_sum BIGINT, n BIGINT, "
            f"PRIMARY KEY (account, symbol) NOT ENFORCED) DISTRIBUTED INTO {PARTS} BUCKETS", DB)
        sel = (f"INSERT INTO sums_{cfu} SELECT account, symbol, SUM(CAST(qty AS BIGINT)), COUNT(*) "
               f"FROM orders /*+ OPTIONS('scan.startup.mode'='earliest-offset') */ GROUP BY account, symbol")
        t_case = time.time()
        nm = run_retrying(f"flink-training-drain{cfu}", sel)
        case = {"cfu": cfu, "statement": nm, "t0": t_case, "samples": []}
        for wait in range(60, 601, 60):
            time.sleep(max(0, t_case + wait - time.time()))
            out_n, _, err = end_offsets(f"sums_{cfu}")
            groups = committed_on("orders")
            cf = cfus()
            st = p.statement_status(nm)[0]
            s = {"t": round(time.time() - t_case), "outEnd": out_n, "committed": groups, "cfu": cf, "status": st}
            case["samples"].append(s)
            say(f"  drain {cfu} CFU, {s['t']} s: status {st}; sums_{cfu} end {out_n}; committed on orders "
                f"{groups}; CFU in use {cf} {err}")
        case["t1"] = time.time()
        record["cases"].append(case)
        delete(nm)
        p.state["pool"] = fill_pool
        json.dump(record, open(os.path.join(OUT, "record.json"), "w"), indent=1, default=str)

    # ---- the query API, after its data has landed
    say("waiting 4 min for the query API to catch up")
    time.sleep(240)
    t0, t1 = record["fill"] and t_fill, time.time()
    for metric in ("io.confluent.flink/num_records_in", "io.confluent.flink/num_records_out",
                   "io.confluent.flink/task/busy_time_ms_per_second",
                   "io.confluent.flink/task/backpressure_time_ms_per_second",
                   "io.confluent.flink/task/idle_time_ms_per_second",
                   "io.confluent.flink/statement_utilization/current_cfus"):
        st, data = query(metric, t0 - 60, t1)
        record.setdefault("query", {})[metric] = {"http": st, "data": data}
        rows = data.get("data") or []
        say(f"query {metric.split('flink/')[1]}: HTTP {st}, {len(rows)} points "
            f"{(data.get('errors') or [{}])[0].get('detail', '') if st != 200 else ''}")
        by = {}
        for r in rows:
            by.setdefault(r.get("resource.flink_statement.name", "-"), []).append((r["timestamp"][11:16], r["value"]))
        for name in sorted(by):
            if "drain" in name or "amp" in name:
                say(f"    {name}: " + " ".join(f"{t}={v:,.0f}" for t, v in sorted(by[name])))
    json.dump(record, open(os.path.join(OUT, "record.json"), "w"), indent=1, default=str)
finally:
    try:
        if cloud_key:
            p.cli("api-key", "delete", cloud_key, "--force")
    finally:
        for f in (secret_file, client):
            if os.path.exists(f):
                os.remove(f)
        for x in extra_pools:
            try:
                p.state["pool"] = x
                p.clear_size()
                p.cli("flink", "compute-pool", "delete", x, "--force")
                print("deleted pool", x, flush=True)
            except Exception as e:
                print("pool", x, "not deleted:", e, flush=True)
        p.state["pool"] = fill_pool if "fill_pool" in globals() else p.state.get("pool")
        p.down()
        print("surviving after down:", p.surviving(), flush=True)
