"""Baseline probe, 2026-10-04: does a drain in a 20 CFU pool reach 20 CFU, and read faster, when its baseline CFU is set to 20? Named flink-training; everything deleted at the end.

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
p = PC.ConfluentCloud({"environment": "flink-training", "stateDir": OUT, "estimateUsd": 14.0,
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


def query(metric, t0, t1, group=("resource.flink_statement.name",), pool_ids=None):
    iv = f"{time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(t0))}/{time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(t1))}"
    body = {"aggregations": [{"metric": metric}], "group_by": list(group),
            "filter": {"op": "OR", "filters": [{"field": "resource.compute_pool.id", "op": "EQ", "value": x}
                                               for x in (pool_ids or [fill_pool] + extra_pools)]},
            "granularity": "PT1M", "intervals": [iv], "limit": 1000}
    st, txt = call(f"{API}/query", AUTH, body)
    try:
        return st, json.loads(txt)
    except ValueError:
        return st, {"raw": txt[:300]}


ORG = "<organization-id>"


def flink_rest(method, name, body=None):
    """The Flink REST API for one statement, with the run's Flink key. Never prints the key."""
    env = dict(l.strip().split("=", 1) for l in open(creds) if "=" in l)
    auth = base64.b64encode(f"{env['FLINK_API_KEY']}:{env['FLINK_API_SECRET']}".encode()).decode()
    del env
    url = (f"https://flink.{p.region}.{p.cloud}.confluent.cloud/sql/v1/organizations/{ORG}"
           f"/environments/{p.environment}/statements/{name}")
    req = urllib.request.Request(url, method=method, data=json.dumps(body).encode() if body is not None else None,
                                 headers={"Authorization": "Basic " + auth,
                                          "Content-Type": "application/json-patch+json" if method == "PATCH"
                                          else "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            txt, st = r.read().decode(), r.status
    except urllib.error.HTTPError as e:
        txt, st = e.read().decode(), e.code
    try:
        return st, json.loads(txt)
    except ValueError:
        return st, txt


def kquery(metric, t0, t1):
    iv = f"{time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(t0))}/{time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(t1))}"
    body = {"aggregations": [{"metric": metric}],
            "filter": {"field": "resource.kafka.id", "op": "EQ", "value": p.state["cluster"]},
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


def amp(i, table="orders"):
    return run_retrying(f"flink-training-amp{i}",
        f"INSERT INTO {table} SELECT CONCAT(b.order_id, '-', CAST(a.x AS STRING), '-', CAST(c.y AS STRING)), "
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

    # ---- fill: one topic, sixteen jobs
    fill_pool = p.state["pool"]
    say(f"setup pool {fill_pool} for the fill; its size read back:", p.read_size())
    p.run_statement("flink-training-orders-ddl",
        "CREATE TABLE orders (order_id STRING, symbol INT, account INT, qty INT, price DOUBLE) "
        "DISTRIBUTED INTO 24 BUCKETS", DB)
    for i in range(1, 17):
        base(i)
    fills = [amp(i, "orders") for i in range(1, 17)]
    say("sixteen fill jobs running")
    t0 = time.time()
    while True:
        time.sleep(120)
        n, parts, err = end_offsets("orders")
        say(f"  fill, {time.time() - t0:.0f} s: orders holds {n} in {parts} partitions {err}")
        if (n or 0) >= 230_000_000 or time.time() - t0 > 2100:
            break
    for nm in fills:
        delete(nm)
    time.sleep(30)
    n, parts, err = end_offsets("orders")
    say(f"backlog: orders holds {n:,} records in {parts} partitions {err}")
    record["backlog"] = {"records": n, "partitions": parts}

    # ---- two drains, one variable: the baseline CFU
    for arm, baseline in (("A", None), ("B", 20)):
        pl = p.cli_json("flink", "compute-pool", "create", f"flink-training-arm{arm.lower()}", "--cloud", p.cloud,
                        "--region", p.region, "--max-cfu", "20")
        extra_pools.append(pl["id"])
        p._wait(lambda: p.cli_json("flink", "compute-pool", "describe", pl["id"]), f"pool {pl['id']}")
        p.state["casePool"] = pl["id"]
        say(f"arm {arm}: new pool {pl['id']}, size read back {p.read_size()}, baseline {baseline}")
        p.run_statement(f"flink-training-sums{arm.lower()}-ddl",
            f"CREATE TABLE sums_{arm.lower()} (account INT, symbol INT, qty_sum BIGINT, n BIGINT, "
            f"PRIMARY KEY (account, symbol) NOT ENFORCED) DISTRIBUTED INTO 24 BUCKETS", DB)
        t_case = time.time()
        nm = run_retrying(f"flink-training-drain{arm.lower()}",
            f"INSERT INTO sums_{arm.lower()} SELECT account, symbol, SUM(CAST(qty AS BIGINT)), COUNT(*) "
            f"FROM orders /*+ OPTIONS('scan.startup.mode'='earliest-offset') */ GROUP BY account, symbol")
        if baseline:
            st, reply = flink_rest("PATCH", nm, [{"op": "add", "path": "/spec/scaling",
                                                  "value": {"baseline_cfu": baseline}}])
            got = (reply.get("spec") or {}).get("scaling") if isinstance(reply, dict) else None
            say(f"arm {arm}: baseline set, HTTP {st}; the statement now reports spec.scaling {got} "
                + ("" if st in (200, 202) else str(reply)[:400]))
            if st not in (200, 202):
                raise L.Refusal("rig", f"the baseline did not apply: HTTP {st}")
        case = {"arm": arm, "baseline": baseline, "statement": nm, "pool": pl["id"], "t0": t_case, "scaling": []}
        for wait in range(60, 661, 60):
            time.sleep(max(0, t_case + wait - time.time()))
            st, reply = flink_rest("GET", nm)
            stat = reply.get("status") if isinstance(reply, dict) else reply
            sc = (stat or {}).get("scaling_status") if isinstance(stat, dict) else None
            spec_sc = (reply.get("spec") or {}).get("scaling") if isinstance(reply, dict) else None
            case["scaling"].append({"t": round(time.time() - t_case), "http": st, "scaling_status": sc,
                                    "spec_scaling": spec_sc, "phase": (stat or {}).get("phase") if isinstance(stat, dict) else None})
            say(f"  arm {arm}, {time.time() - t_case:.0f} s: HTTP {st}; phase {case['scaling'][-1]['phase']}; "
                f"scaling status {sc}; spec.scaling {spec_sc}; CFU in use (export) {cfus()}")
            if wait == 60 and isinstance(stat, dict):
                say(f"  arm {arm}: status fields returned: {sorted(stat)}")
        time.sleep(180)          # the query API lags about three minutes
        for metric in ("statement_utilization/current_cfus", "num_records_in", "pending_records",
                       "task/busy_time_ms_per_second", "task/backpressure_time_ms_per_second"):
            st, data = query("io.confluent.flink/" + metric, t_case - 60, time.time(), pool_ids=[pl["id"]])
            rows = sorted((r["timestamp"][11:16], r["value"]) for r in data.get("data") or [])
            case[metric] = rows
            say(f"    arm {arm} {metric}: " + " ".join(f"{t}={v}" for t, v in rows)
                + ("" if st == 200 else f" HTTP {st} {str(data)[:200]}"))
        st, data = kquery("io.confluent.kafka.server/sent_bytes", t_case - 60, time.time())
        case["kafka_sent_bytes"] = sorted((r["timestamp"][11:16], r["value"]) for r in data.get("data") or [])
        say(f"    arm {arm} kafka sent bytes per minute: " + " ".join(f"{t}={v}" for t, v in case["kafka_sent_bytes"])
            + ("" if st == 200 else f" HTTP {st} {str(data)[:200]}"))
        record["cases"].append(case)
        delete(nm)
        p.state["casePool"] = None
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
