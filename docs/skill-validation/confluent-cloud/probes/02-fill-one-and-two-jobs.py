"""Unbounded faker into a topic at 5 then 10 CFU; the rate read from the
output topic's end offsets in Kafka itself (kafka-get-offsets, SASL with the
run's Kafka key from the owner-only credentials file, never printed)."""
import os, subprocess, sys, time
sys.path.insert(0, "~/code/GitHub/scalable-flink-skill/harness")
import lib as L
import platform_confluent as PC
OUT = sys.argv[1]
creds = os.path.join(OUT, "probe-creds.env")
p = PC.ConfluentCloud({"environment": "flink-training", "stateDir": OUT, "estimateUsd": 5.0,
                       "credentials": creds}, log=print)
client = os.path.join(OUT, "client.properties")
def end_offsets(topic):
    r = subprocess.run(["docker", "run", "--rm", "-v", f"{client}:/tmp/client.properties:ro",
                        "apache/kafka:3.9.2", "/opt/kafka/bin/kafka-get-offsets.sh",
                        "--bootstrap-server", BOOT, "--command-config", "/tmp/client.properties",
                        "--topic", topic], capture_output=True, text=True, timeout=180)
    if r.returncode:
        return None, r.stderr.strip()[-300:]
    return sum(int(l.rsplit(":", 1)[1]) for l in r.stdout.split() if l.count(":") >= 2), ""
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
    db = [c for c in p.cli_json("kafka", "cluster", "list") if c["id"] == p.state["cluster"]][0]["name"]
    cfu = 10
    print(f"pool size {cfu} CFU ->", L.P.set_and_read_back(p, cfu), flush=True)
    def make(i):
        p.run_statement(f"flink-training-base{i}",
            f"CREATE TABLE fake_base_{i} (order_id STRING, symbol INT, account INT, qty INT, price DOUBLE) WITH ("
            f"'connector' = 'faker', 'number-of-rows' = '100000', 'rows-per-second' = '100000', "
            f"'fields.order_id.expression' = '#{{Internet.uuid}}', "
            f"'fields.symbol.expression' = '#{{number.numberBetween ''0'',''4096''}}', "
            f"'fields.account.expression' = '#{{number.numberBetween ''0'',''4''}}', "
            f"'fields.qty.expression' = '#{{number.numberBetween ''1'',''100''}}', "
            f"'fields.price.expression' = '#{{number.randomDouble ''2'',''1'',''500''}}')", db)
        p.run_statement(f"flink-training-ampdst{i}",
            f"CREATE TABLE amplified_{i} (order_id STRING, symbol INT, account INT, qty INT, price DOUBLE)", db)
    def start(i):
        name = f"flink-training-amp{i}"
        for attempt in range(1, 6):
            nm = f"{name}t{attempt}"
            try:
                p.run_statement(nm,
                    f"INSERT INTO amplified_{i} SELECT CONCAT(b.order_id, '-', CAST(a.x AS STRING), '-', CAST(c.y AS STRING)), "
                    f"b.symbol, b.account, b.qty, b.price FROM fake_base_{i} AS b "
                    f"CROSS JOIN (VALUES (1),(2),(3),(4),(5),(6),(7),(8),(9),(10),(11),(12),(13),(14),(15),(16),(17),(18),(19),(20),(21),(22),(23),(24),(25),(26),(27),(28),(29),(30),(31),(32),(33),(34),(35),(36),(37),(38),(39),(40),(41),(42),(43),(44),(45),(46),(47),(48),(49),(50)) AS a(x) CROSS JOIN (VALUES (1),(2),(3),(4),(5),(6),(7),(8),(9),(10),(11),(12),(13),(14),(15),(16),(17),(18),(19),(20),(21),(22),(23),(24),(25),(26),(27),(28),(29),(30),(31),(32),(33),(34),(35),(36),(37),(38),(39),(40)) AS c(y)", db)
                return nm
            except L.Refusal as e:
                if "Cannot find table" not in e.msg or attempt == 5:
                    raise
                print(f"  job {i} attempt {attempt}: the new table was not visible yet; retrying in 30 s", flush=True)
                p.cli("flink", "statement", "delete", nm, "--cloud", p.cloud, "--region", p.region, "--force")
                time.sleep(30)
    def measure(label, topics):
        t_run = time.time(); samples = []
        for wait in (60, 180, 360):
            time.sleep(max(0, t_run + wait - time.time()))
            counts = [end_offsets(t) for t in topics]
            errs = [e for _, e in counts if e]
            ns = [n for n, _ in counts]
            print(f"  {label}, {time.time()-t_run:.0f} s: " + ", ".join(f"{t} {n if n is None else f'{n:,}'}" for t, n in zip(topics, ns)) + (f" {errs}" if errs else ""), flush=True)
            if None not in ns:
                samples.append((time.time(), ns))
        if len(samples) >= 2:
            (ta, na), (tb, nb) = samples[0], samples[-1]
            per = [(b - a) / (tb - ta) for a, b in zip(na, nb)]
            print(f"{label}: " + " + ".join(f"{r:,.0f}" for r in per) + f" = {sum(per):,.0f} records/s written, over {tb - ta:.0f} s", flush=True)
    # arm A: one job alone
    make(1); a1 = start(1)
    measure("arm A, one job", ["amplified_1"])
    p.cli("flink", "statement", "delete", a1, "--cloud", p.cloud, "--region", p.region, "--force")
    # arm B: two jobs side by side, fresh tables, same pool
    make(2); make(3); b2 = start(2); b3 = start(3)
    measure("arm B, two jobs", ["amplified_2", "amplified_3"])
    for nm in (b2, b3):
        p.cli("flink", "statement", "delete", nm, "--cloud", p.cloud, "--region", p.region, "--force")
finally:
    if os.path.exists(client):
        os.remove(client)
    p.down(); print("surviving:", p.surviving())
