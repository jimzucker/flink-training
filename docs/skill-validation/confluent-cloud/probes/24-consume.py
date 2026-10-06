"""Count orders_small with Kafka's own consumer, from the start to the end:
once as a committed-only reader (isolation.level=read_committed), once
reading everything (read_uncommitted). The ground truth for what a reader
gets, against the snapshot query's 37,996,160 and the log end's 66,092,231.
Keys are read from the adapter's owner-only file and never printed."""
import os, sys, tempfile, subprocess, time
sys.path.insert(0, os.path.expanduser("~/.claude/skills/scalable-flink-skill/harness"))
import json
import platform_confluent as PC
D = sys.argv[1]; iso = sys.argv[2]
cfg = json.load(open("~/code/GitHub/flink-training/docs/skill-validation/cloud-sql-app/pipeline-cloud.json"))
raw = dict(cfg["platform"]); raw.update(stateDir=D, credentials=os.path.join(D, "creds.env"))
p = PC.ConfluentCloud(raw, log=lambda *a: None)
d = tempfile.mkdtemp(prefix="fsk-consume-")
client = os.path.join(d, "client.properties")
fd = os.open(client, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
with os.fdopen(fd, "w") as f:
    f.write("security.protocol=SASL_SSL\nsasl.mechanism=PLAIN\n"
            "sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required "
            f"username=\"{p._secret('KAFKA_API_KEY')}\" password=\"{p._secret('KAFKA_API_SECRET')}\";\n"
            f"isolation.level={iso}\nauto.offset.reset=earliest\nfetch.max.bytes=104857600\n"
            "max.partition.fetch.bytes=10485760\n")
t0 = time.time()
try:
    r = subprocess.run(["docker", "run", "--rm", "-v", f"{d}:/fsk:ro", p.kafka_image,
                        "/opt/kafka/bin/kafka-consumer-perf-test.sh", "--bootstrap-server", p._secret("KAFKA_BOOTSTRAP"),
                        "--consumer.config", "/fsk/client.properties", "--topic", sys.argv[3] if len(sys.argv) > 3 else "orders_small",
                        "--messages", "500000000", "--timeout", "60000", "--group", f"flink-training-count-{iso}-{sys.argv[3] if len(sys.argv) > 3 else 0}"],
                       capture_output=True, text=True)
    print(time.strftime("%H:%M:%S"), iso, f"{time.time() - t0:.0f} s", "rc", r.returncode)
    print(r.stdout[-1500:]); print(r.stderr[-1500:])
finally:
    os.remove(client); os.rmdir(d)
