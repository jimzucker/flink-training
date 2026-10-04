"""Unbounded faker into a topic at 5 then 10 CFU; the rate read from the
output topic's end offsets in Kafka itself (kafka-get-offsets, SASL with the
run's Kafka key from the owner-only credentials file, never printed)."""
import os, subprocess, sys, time
sys.path.insert(0, "~/code/GitHub/scalable-flink-skill/harness")
import lib as L
import platform_confluent as PC
OUT = sys.argv[1]
creds = os.path.join(OUT, "probe-creds.env")
p = PC.ConfluentCloud({"environment": "flink-training", "stateDir": OUT, "estimateUsd": 4.0,
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
    for cfu in (5, 10):
        print(f"pool size {cfu} CFU ->", L.P.set_and_read_back(p, cfu), flush=True)
        src, dst = f"fake_base_{cfu}", f"amplified_{cfu}"
        p.run_statement(f"flink-training-base{cfu}",
            f"CREATE TABLE {src} (order_id STRING, symbol INT, account INT, qty INT, price DOUBLE) WITH ("
            f"'connector' = 'faker', 'number-of-rows' = '100000', 'rows-per-second' = '100000', "
            f"'fields.order_id.expression' = '#{{Internet.uuid}}', "
            f"'fields.symbol.expression' = '#{{number.numberBetween ''0'',''4096''}}', "
            f"'fields.account.expression' = '#{{number.numberBetween ''0'',''4''}}', "
            f"'fields.qty.expression' = '#{{number.numberBetween ''1'',''100''}}', "
            f"'fields.price.expression' = '#{{number.randomDouble ''2'',''1'',''500''}}')", db)
        p.run_statement(f"flink-training-ampdst{cfu}",
            f"CREATE TABLE {dst} (order_id STRING, symbol INT, account INT, qty INT, price DOUBLE)", db)
        name = f"flink-training-amp{cfu}"
        def start_amp(nm):
          return p.run_statement(nm,
            f"INSERT INTO {dst} SELECT CONCAT(b.order_id, '-', CAST(a.x AS STRING), '-', CAST(c.y AS STRING)), "
            f"b.symbol, b.account, b.qty, b.price FROM {src} AS b "
            f"CROSS JOIN (VALUES (1),(2),(3),(4),(5),(6),(7),(8),(9),(10),(11),(12),(13),(14),(15),(16),(17),(18),(19),(20),(21),(22),(23),(24),(25),(26),(27),(28),(29),(30),(31),(32),(33),(34),(35),(36),(37),(38),(39),(40),(41),(42),(43),(44),(45),(46),(47),(48),(49),(50)) AS a(x) CROSS JOIN (VALUES (1),(2),(3),(4),(5),(6),(7),(8),(9),(10),(11),(12),(13),(14),(15),(16),(17),(18),(19),(20),(21),(22),(23),(24),(25),(26),(27),(28),(29),(30),(31),(32),(33),(34),(35),(36),(37),(38),(39),(40)) AS c(y)", db)
        for attempt in range(1, 6):
            nm = f"{name}t{attempt}"
            try:
                start_amp(nm); name = nm; break
            except L.Refusal as e:
                if "Cannot find table" not in e.msg or attempt == 5:
                    raise
                print(f"  attempt {attempt}: the new table was not visible yet; retrying in 30 s", flush=True)
                p.cli("flink", "statement", "delete", nm, "--cloud", p.cloud, "--region", p.region, "--force")
                time.sleep(30)
        t_run = time.time()
        samples = []
        for wait in (60, 180, 360):
            time.sleep(max(0, t_run + wait - time.time()))
            n, err = end_offsets(dst)
            print(f"  {cfu} CFU, {time.time()-t_run:.0f} s after running: {dst} holds "
                  f"{n if n is None else f'{n:,}'} records {err}", flush=True)
            if n is not None:
                samples.append((time.time(), n))
        if len(samples) >= 2:
            (ta, na), (tb, nb) = samples[0], samples[-1]
            print(f"{cfu} CFU: {(nb - na) / (tb - ta):,.0f} records/s written, over {tb - ta:.0f} s", flush=True)
        p.cli("flink", "statement", "delete", name, "--cloud", p.cloud, "--region", p.region, "--force")
finally:
    if os.path.exists(client):
        os.remove(client)
    p.down(); print("surviving:", p.surviving())
