#!/usr/bin/env python3
"""
prove-it-scales: the measurement harness.

This file is the skill's guards, as code. It is shipped with the skill and used
verbatim: an agent following the skill supplies a pipeline (see README.md) and
does NOT rewrite any of this. Every threshold below names the measurement it
was set from, and `prove.py replay` checks each one against the recorded runs
before it is allowed to refuse anything new.

Rules this file implements (one guard per rule; `prove.py selftest` breaks
every one on purpose, because a guard that has never fired is a guess):

  * the resource cap is read back from the container, never from the env var
  * parallelism == cap == allocated slots, all three read back from the engine
  * the job graph shape is read off the running plan and compared across cases
  * the component under test must be the constraint (cap consumption floors)
  * the job is torn down on every exit path, refused cases included
  * the window is anchored on the committed offset advancing, not wall clock
  * throughput comes from the transport (committed broker offsets), not Flink
  * CPU comes from the cumulative cgroup counter, not `docker stats`
  * the backlog must have a full checkpoint interval of headroom at close
  * two vantage points must agree
  * a case's passes spread past the ceiling: that case and its ratios are
    unreportable; the other cases still report

A refusal about the RIG stops the suite. A refusal about one case's DATA marks
that case and moves on.
"""

import calendar
import hashlib
import json
import os
import re
import subprocess
import sys
import time
import urllib.error
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))

# quick look: set by `prove.py <cmd> --quick`. One pass per case instead of the
# configured number. The chain runs end to end and every per-case guard fires;
# the table it produces is marked unpublishable and must never be quoted as a
# result. It answers "does this rig run clean, and roughly how fast", nothing more.
QUICK = False

# ------------------------------------------------------------------ thresholds
#
# Each value names where it came from. `prove.py replay` re-derives the table
# for every recorded suite in record/ with these numbers and refuses to run if
# one of them would void a table the record marks valid.

T = {
    # constraint ownership — run 5 published a 2.82x from a worker at 94% of cap.
    # The baseline floor was 98% until 2026-09-04. Replayed by hand against every
    # baseline pass it had refused (the record does not carry per-pass cap
    # fractions, so `replay` cannot): four refusals at 95.9-97.4% (runs 11, 12,
    # rig Phase 3, incl. run 12's sentinel) carried the same rate as the accepted
    # passes (x0.97-x1.05); the two at 95.3% and 94.2% (Phase 1) were 31% high and
    # 24% low, which the spread guard refuses on its own. One floor for every
    # case; run 5's 94% stays out. The good/bad gap on record is 0.6 points at
    # n=6 — 95% is the existing floor the record does not contradict, not a
    # floor measured from noise.
    "capFloorBaseline": 0.95,
    "capFloorOther": 0.95,
    # external boundary: a starved source idles (run 5: the broker was the ceiling
    # at 43% back-pressure with the TM under cap). Measured 2026-09-04: at-cap
    # 2-core cases idle 8.1-16.8% at the same throughput (14 cases, sd 2.3%), and
    # the one recorded broker-constrained step (ceiling run, TM at 91.7%) idled
    # 16.7% — idle alone does not separate the two; the cap floor does. Ceiling:
    # max at-cap idle + one sd = 19%. 15% was a round number inside the band.
    "sourceIdleCeil": 0.20,
    # two vantage points: 0.5% measured after anchoring on commit boundaries (run 7);
    # 5% is ten times that
    "vantageTol": 0.05,
    # pass-to-pass spread. Measured over 13 suites in runs 9-10: 2- and 4-core
    # cases 0.1%-15.8%, one-core cases up to 42%, one broken suite at 86%.
    # 20% sits above the whole valid band and below every outlier. The 10% it
    # replaces was set below the band and voided six valid tables in run 9.
    "spreadCeil": 0.20,
    "minPasses": 2,
    # warm-up: a flat least-squares slope through four commit intervals, and the
    # scatter bounded too — run 10 suite B admitted a still-accelerating ramp on
    # slope alone. 10% scatter was unsatisfiable at 4 cores (checkpoint jitter is
    # 13-15% and stationary); 20% still rejects the 40% ramp.
    "warmupIntervals": 4,
    "warmupFlatTol": 0.10,
    "warmupScatterTol": 0.20,
    "warmupMinS": 90.0,        # run 10: the ramp ran 46-136 s; outlast it
    "warmupMaxS": 240.0,
    # window: >=3 commit boundaries is the rule; six 10 s boundaries average the
    # checkpoint jitter (run 10). Windows of 20 s, 45 s and 60 s were tried in
    # run 9 and did not move the spread.
    "minBoundaries": 6,
    "minWindowS": 60.0,
    "windowMaxS": 300.0,
    # the reporter samples busy/idle/back-pressured on this interval; a window
    # must hold at least three samples or the external-boundary guard is a guess
    # (harness live test: a 6 s window against a 10 s reporter held none)
    "reporterS": 10,
    "minBpSamples": 3,
    # the tiny proof: superlinear has been an artefact every time (run 8: 3.73x
    # from a chained baseline)
    "tinyRatioLo": 1.5,
    "tinyRatioHi": 2.5,
    # a full disk takes the shell down with it (run 5)
    "diskFloorBytes": 20e9,
    # retention on undrained sink topics is a periodic sweep, not a bound (run 5:
    # 27 GB against a 4 GB cap between sweeps); 2 GB/partition let run 10's
    # sinks run without a sweep inside a window
    "sinkRetentionBytes": 2 * 1024 ** 3,
}


# ---------------------------------------------------------------------- config

class Refusal(Exception):
    def __init__(self, scope, msg):
        super().__init__(msg)
        self.scope = scope   # "rig" or "case"
        self.msg = msg


class CaseRefused(Exception):
    def __init__(self, rec, refusal):
        super().__init__(refusal.msg)
        self.rec = rec
        self.refusal = refusal


class Cfg:
    """pipeline.json, resolved. Everything the harness needs to know about the
    pipeline under test comes from here; everything about *measurement* does not."""

    def __init__(self, path):
        self.path = os.path.abspath(path)
        self.root = os.path.dirname(self.path)
        with open(self.path) as f:
            c = json.load(f)
        self.raw = c
        req = ["project", "topics", "partitions", "outputsPerInput", "checkpointMs",
               "job", "generator", "verifier", "cases", "baseline", "passes", "backlog",
               "caps", "images", "jdk", "axis", "apiLevel", "guarantee"]
        missing = [k for k in req if k not in c]
        if missing:
            raise Refusal("rig", f"pipeline.json is missing {missing}")
        p = c["project"]
        if not re.fullmatch(r"[a-z][a-z0-9]{1,15}", p):
            raise Refusal("rig", f"project must be a short lowercase token, got {p!r}")
        self.project = p
        self.results = os.path.join(self.root, "results")
        self.stack_dir = os.path.join(self.root, "stack")
        self.net = f"{p}_default"
        self.kafka = f"{p}-kafka"
        self.jm = f"{p}-jm"
        self.tm = f"{p}-tm"
        self.sampler = f"{p}-sampler"
        self.ckpt_vol = f"{p}_ckpt"
        self.kafka_vol = f"{p}_kafkadata"
        self.boot_int = f"{self.kafka}:9092"
        port = int(c.get("ports", {}).get("kafka", 19092))
        self.kafka_port = port
        self.rest_port = int(c.get("ports", {}).get("rest", 18081))
        self.boot_ext = f"localhost:{port}"
        self.rest = f"http://localhost:{self.rest_port}"
        self.topic_in = c["topics"]["in"]
        self.topics_out = list(c["topics"]["out"])
        self.partitions = int(c["partitions"])
        self.out_per_in = float(c["outputsPerInput"])
        self.ckpt_ms = int(c["checkpointMs"])
        self.ckpt_s = self.ckpt_ms / 1000.0
        self.jar = os.path.join(self.root, c["job"]["jar"])
        self.jar_dir = os.path.dirname(self.jar)
        self.jar_in_ctr = "/jobs/" + os.path.basename(self.jar)
        self.main_class = c["job"]["mainClass"]
        self.job_args = c["job"]["args"]
        self.source_match = c["job"].get("sourceVertexMatch", "Source")
        self.gen_cmd = c["generator"]["cmd"]
        self.manifest_cmd = c["generator"].get("manifestCmd")
        self.count_field = c["generator"].get("manifestCountField", "count")
        self.verify_cmd = c["verifier"]["cmd"]
        self.cases = [int(x) for x in c["cases"]]
        self.baseline = int(c["baseline"])
        self.passes = int(c["passes"])
        self.backlog = int(c["backlog"]["count"])
        self.seed = int(c["backlog"].get("seed", 1))
        self.small = int(c["backlog"].get("smallCount", 2_000_000))
        self.tiny = int(c["backlog"].get("tinyCount", 8_000_000))
        self.kill_frac = float(c["backlog"].get("killAtFraction", 0.35))
        caps = c["caps"]
        self.kafka_cap = float(caps.get("kafka", 2.5))
        self.jm_cap = float(caps.get("jobmanager", 0.5))
        self.tm_mem = caps.get("tmMemory", "4096m")
        self.tm_mem_limit = caps.get("tmMemoryLimit", "6g")
        self.kafka_mem = caps.get("kafkaMemory", "4g")
        self.kafka_heap = caps.get("kafkaHeap", "3G")
        self.flink_img = c["images"]["flink"]
        self.kafka_img = c["images"]["kafka"]
        self.jdk = c["jdk"]
        self.java = os.path.join(self.jdk, "bin", "java")
        self.axis = c["axis"]
        self.api_level = c["apiLevel"]
        self.guarantee = c["guarantee"]
        self.log_path = os.path.join(self.results, "harness.log")
        if self.baseline not in self.cases:
            raise Refusal("rig", f"baseline {self.baseline} is not one of the cases {self.cases}")
        if QUICK:
            # quick look: one pass per case, still followed by the sentinel, so
            # the baseline is measured twice and drift is visible. Every per-case
            # guard stays live; what is lost is the spread, and with it the right
            # to publish. Replayed against the record 2026-09-04: single passes of
            # the recorded suites read 2.039-2.273x where the suite reported
            # 2.154x (run 12, 2->4) and 1.837-1.859x against 1.850x (run 13) —
            # a one-pass number lands anywhere in a band wider than the accept
            # line, which is why this mode marks its table unpublishable.
            self.passes = 1
        elif self.passes < T["minPasses"]:
            raise Refusal("rig", f"passes must be >= {T['minPasses']} (every case at least twice)")

    def fmt(self, s, **kw):
        """Fill a command template from pipeline.json."""
        d = dict(java=self.java, jar=self.jar, bootstrap=self.boot_int, bootstrapExt=self.boot_ext,
                 partitions=self.partitions, ckptMs=self.ckpt_ms, seed=self.seed,
                 **{"in": self.topic_in})
        for i, t in enumerate(self.topics_out):
            d[f"out{i}"] = t
        d["outs"] = ",".join(self.topics_out)
        d.update(kw)
        return s.format(**d)


_CFG = None


def cfg():
    global _CFG
    if _CFG is None:
        path = os.environ.get("PIPELINE_JSON", "pipeline.json")
        _CFG = Cfg(path)
    return _CFG


# ---------------------------------------------------------------------- basics

def log(*a):
    c = cfg()
    line = " ".join(str(x) for x in a)
    print(f"[{time.strftime('%H:%M:%S')}] {line}", flush=True)
    os.makedirs(c.results, exist_ok=True)
    with open(c.log_path, "a") as f:
        f.write(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {line}\n")


def sh(cmd, check=True, timeout=600):
    """Never silence a command while you are still finding out whether it works."""
    r = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=timeout)
    if r.returncode != 0 and check:
        raise Refusal("rig", f"command failed ({r.returncode}): {cmd}\n"
                             f"stdout: {r.stdout[-2000:]}\nstderr: {r.stderr[-2000:]}")
    return r


def rest(path, timeout=15):
    with urllib.request.urlopen(cfg().rest + path, timeout=timeout) as r:
        return json.loads(r.read().decode())


def rest_patch(path, timeout=30):
    req = urllib.request.Request(cfg().rest + path, method="PATCH")
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read().decode()


def kafka(args, check=True, timeout=300):
    return sh(f"docker exec {cfg().kafka} /opt/kafka/bin/{args}", check=check, timeout=timeout)


def host_free_bytes():
    r = sh(f"df -k {cfg().root} | tail -1")   # the HOST filesystem the results live on
    return int(r.stdout.split()[3]) * 1024


def topic_bytes(topics):
    """Bytes on the broker's disk for these topics, read from the log dir."""
    c = cfg()
    # summed here, not in the container: the first live test died on awk quoting three shells deep
    pat = " ".join(f"/var/lib/kafka/data/{t}-*" for t in topics)
    r = sh(f"docker exec {c.kafka} sh -c 'du -sk {pat}'", check=False)
    sizes = [ln.split()[0] for ln in r.stdout.splitlines() if ln.split()]
    if r.returncode != 0 or not sizes or not all(x.isdigit() for x in sizes):
        raise Refusal("rig", f"could not read the broker log dir size for {topics}: {r.stdout} {r.stderr}")
    return sum(int(x) for x in sizes) * 1024


def volume_bytes(vol):
    r = sh(f"docker run --rm -v {vol}:/v alpine du -sk /v", check=False)
    if r.returncode != 0 or not r.stdout.split():
        raise Refusal("rig", f"could not read the size of volume {vol}: {r.stdout} {r.stderr}")
    return int(r.stdout.split()[0]) * 1024


def disk_verdict(free, in_bytes_per_rec, backlog, sink_bytes_per_in, partitions, n_out_topics, ckpt_bytes):
    """Pure. Project the suite's disk from the tiny proof's measured shape, before
    the fill. The sinks are bounded by retention, so the projection is too — run
    11 rebuilt its sink payload against a 75 GB figure that retention would have
    capped at 34 GB, and re-ran both gates for the new build."""
    inp = in_bytes_per_rec * backlog
    sink_unbounded = sink_bytes_per_in * backlog
    sink_cap = partitions * n_out_topics * T["sinkRetentionBytes"]
    sink = min(sink_unbounded, sink_cap)
    need = inp + sink + ckpt_bytes + T["diskFloorBytes"]
    d = {"hostFreeBytes": int(free), "inputBytesPerRecord": round(in_bytes_per_rec, 1),
         "inputBytes": int(inp), "sinkBytesPerInput": round(sink_bytes_per_in, 1),
         "sinkBytesUnbounded": int(sink_unbounded), "sinkRetentionCapBytes": int(sink_cap),
         "sinkBytes": int(sink), "sinkBoundedByRetention": sink_unbounded > sink_cap,
         "checkpointBytes": int(ckpt_bytes), "floorBytes": int(T["diskFloorBytes"]),
         "neededBytes": int(need), "fits": need <= free}
    if need > free:
        e = Refusal("rig", f"the suite would not fit on disk: input {inp/1e9:.1f} GB + sinks {sink/1e9:.1f} GB "
                           f"(retention-capped at {sink_cap/1e9:.1f} GB) + checkpoints {ckpt_bytes/1e9:.1f} GB + "
                           f"floor {T['diskFloorBytes']/1e9:.0f} GB = {need/1e9:.1f} GB against {free/1e9:.1f} GB free once the tiny proof's topics are gone "
                           f"— shrink the backlog or the records before the fill, not after the suite")
        e.detail = d
        raise e
    return d


def disk_projection(tiny_topic, tiny_count, last_case_rec):
    """The measured shape: input bytes per record from the tiny topic, sink bytes
    per input from what the last tiny case wrote, checkpoint bytes from the volume."""
    c = cfg()
    tiny_bytes = topic_bytes([tiny_topic])
    in_bpr = tiny_bytes / tiny_count
    consumed = int(last_case_rec["close"]["committed"])
    sink_bytes = topic_bytes(c.topics_out)
    sink_bpi = sink_bytes / consumed if consumed else 0.0
    ckpt = volume_bytes(c.ckpt_vol)
    # The tiny topic and the tiny cases' sinks are still on disk when this runs and are
    # deleted before the suite fill; measured on the noise rig, deleting the 29.6 GB tiny
    # topic returned 29 GB to the host within three minutes (92.4 -> 121.3 GB free). The
    # first live projection counted them as used and refused a suite that fitted.
    host_free = host_free_bytes()
    reclaimable = tiny_bytes + sink_bytes
    d = disk_verdict(host_free + reclaimable, in_bpr, c.backlog, sink_bpi, c.partitions, len(c.topics_out), ckpt)
    d.update(hostFreeBytesNow=int(host_free), reclaimableBytes=int(reclaimable),
             measuredOn={"tinyTopicRecords": tiny_count, "tinyTopicBytes": int(tiny_bytes),
                         "sinkRecordsConsumed": consumed, "sinkBytesOnDisk": int(sink_bytes)})
    return d


def build_hash():
    h = hashlib.sha256()
    with open(cfg().jar, "rb") as f:
        for b in iter(lambda: f.read(1 << 20), b""):
            h.update(b)
    return h.hexdigest()[:16]


def save_json(name, obj):
    c = cfg()
    os.makedirs(c.results, exist_ok=True)
    p = os.path.join(c.results, name)
    with open(p, "w") as f:
        json.dump(obj, f, indent=2, default=str)
    return p


def load_json(name):
    with open(os.path.join(cfg().results, name)) as f:
        return json.load(f)


# ----------------------------------------------------------------------- stack

def compose_text():
    c = cfg()
    return f"""name: {c.project}

services:
  kafka:
    image: {c.kafka_img}
    container_name: {c.kafka}
    hostname: {c.kafka}
    cpus: {c.kafka_cap}
    mem_limit: {c.kafka_mem}
    ports:
      - "{c.kafka_port}:{c.kafka_port}"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093,EXTERNAL://:{c.kafka_port}
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://{c.kafka}:9092,EXTERNAL://localhost:{c.kafka_port}
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT,EXTERNAL:PLAINTEXT
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@{c.kafka}:9093
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      KAFKA_LOG_DIRS: /var/lib/kafka/data
      KAFKA_NUM_PARTITIONS: {c.partitions}
      KAFKA_DEFAULT_REPLICATION_FACTOR: 1
      KAFKA_LOG_SEGMENT_BYTES: "268435456"
      KAFKA_NUM_IO_THREADS: 8
      KAFKA_NUM_NETWORK_THREADS: 5
      KAFKA_SOCKET_SEND_BUFFER_BYTES: "1048576"
      KAFKA_SOCKET_RECEIVE_BUFFER_BYTES: "1048576"
      KAFKA_HEAP_OPTS: "-Xmx{c.kafka_heap} -Xms{c.kafka_heap}"
    volumes:
      - kafkadata:/var/lib/kafka/data

  jobmanager:
    image: {c.flink_img}
    container_name: {c.jm}
    hostname: {c.jm}
    command: jobmanager
    user: "0:0"
    cpus: {c.jm_cap}
    mem_limit: 2g
    depends_on:
      - kafka
    ports:
      - "{c.rest_port}:8081"
    environment:
      FLINK_PROPERTIES: |
        jobmanager.rpc.address: {c.jm}
        jobmanager.memory.process.size: 1600m
        rest.address: 0.0.0.0
        rest.bind-address: 0.0.0.0
        state.checkpoint-storage: filesystem
        state.checkpoints.dir: file:///ckpt
        state.backend.type: hashmap
        parallelism.default: {c.baseline}
        heartbeat.timeout: 120000
    volumes:
      - ckpt:/ckpt
      - {c.jar_dir}:/jobs:ro

volumes:
  kafkadata:
  ckpt:
"""


def compose_path():
    c = cfg()
    os.makedirs(c.stack_dir, exist_ok=True)
    p = os.path.join(c.stack_dir, "compose.yml")
    with open(p, "w") as f:
        f.write(compose_text())
    return p


def stack_up():
    """Bring the stack up from cold. Idempotent. Asserts every effect."""
    c = cfg()
    save_json("volumes-before.json", dangling_anonymous_volumes())
    p = compose_path()
    sh(f"docker compose -f {p} up -d", timeout=900)
    # The flink entrypoint drops privileges to uid 9999; a named volume is
    # created root-owned, so the checkpoint coordinator cannot mkdir under it.
    sh(f"docker run --rm --user 0:0 -v {c.ckpt_vol}:/ckpt --entrypoint sh {c.flink_img} "
       f"-c 'chown -R 9999:9999 /ckpt'")
    sh(f"docker run --rm --user 9999:9999 -v {c.ckpt_vol}:/ckpt --entrypoint sh {c.flink_img} "
       f"-c 'mkdir -p /ckpt/.probe/shared && rmdir /ckpt/.probe/shared /ckpt/.probe'")
    for _ in range(120):
        try:
            rest("/overview")
            break
        except Exception:
            time.sleep(1)
    else:
        raise Refusal("rig", "job manager REST never came up")
    for _ in range(120):
        if kafka(f"kafka-topics.sh --bootstrap-server {c.boot_int} --list", check=False).returncode == 0:
            break
        time.sleep(1)
    else:
        raise Refusal("rig", "kafka never answered")
    build_sampler()
    log(f"stack up: {c.kafka} (cap {c.kafka_cap}), {c.jm} (cap {c.jm_cap}); "
        f"task manager is started per case")


def dangling_anonymous_volumes():
    """Anonymous volumes the engine image declares, left by any container removed
    without -v. The harness removes with -v; this catches the ones it did not."""
    r = sh("docker volume ls -q -f dangling=true", check=False)
    return [v for v in r.stdout.split() if re.fullmatch(r"[0-9a-f]{64}", v)]


def stack_down(trim=True):
    """Tear down everything this project started, and assert nothing survives."""
    c = cfg()
    for n in (c.sampler, c.tm, f"{c.project}-capprobe"):
        sh(f"docker rm -f -v {n}", check=False)
    p = os.path.join(c.stack_dir, "compose.yml")
    if os.path.exists(p):
        sh(f"docker compose -f {p} down -v --remove-orphans", check=False, timeout=600)
    for v in (c.ckpt_vol, c.kafka_vol):
        sh(f"docker volume rm {v}", check=False)
    left = surviving()
    if left:
        raise Refusal("rig", f"teardown left these behind: {left}")
    # dangling anonymous volumes were recorded at `up`; anything newer is ours
    vb = os.path.join(c.results, "volumes-before.json")
    before = set(load_json("volumes-before.json")) if os.path.exists(vb) else set()
    ours = [v for v in dangling_anonymous_volumes() if v not in before]
    for v in ours:
        sh(f"docker volume rm {v}", check=False)
    if [v for v in dangling_anonymous_volumes() if v not in before]:
        raise Refusal("rig", "anonymous volumes created during the run survive teardown")
    if ours:
        log(f"removed {len(ours)} anonymous volume(s) left by containers removed without -v")
    killed = reap_host_watchers()
    if killed:
        log(f"killed {len(killed)} host process(es) watching this run: " +
            "; ".join(f"{pid} {cmd[:80]}" for pid, cmd in killed))
    left = host_watchers()
    if left:
        raise Refusal("rig", "host processes watching this run survive teardown: " +
                             "; ".join(f"{pid} {cmd[:80]}" for pid, cmd in left))
    if trim:
        r = sh("docker run --rm --privileged --pid=host alpine nsenter -t 1 -m -u -n -i -- "
               "fstrim -v /var/lib/docker", check=False, timeout=600)
        log("fstrim:", (r.stdout + r.stderr).strip()[-200:])
    log("stack down; nothing with prefix", c.project, "survives")


def host_watchers(ignore_children=False):
    """Host processes the harness did not start but that watch this run: anything
    holding a file open under results/, naming the project directory on its
    command line, or naming prove.py on its command line *and* running from
    inside the project. Run 11 left two `until ! pgrep -f "prove.py suite"`
    shells alive, each matching its own command line and so waiting on itself;
    the assertion on containers, volumes and networks could not see them. Own
    process, its ancestors and its children are excluded. Scoped this tightly
    on purpose: a first version matched any command line naming prove.py and
    its self-test killed a live tiny proof in another directory; a second
    matched any process whose working directory was the project and killed
    the `tail` its own shell was piping into."""
    c = cfg()
    root = os.path.realpath(c.root)
    me = os.getpid()
    ps = sh("ps -axo pid=,ppid=,command=", check=False).stdout.splitlines()
    parent, cmd = {}, {}
    for line in ps:
        parts = line.split(None, 2)
        if len(parts) < 2:
            continue
        pid, ppid = int(parts[0]), int(parts[1])
        parent[pid] = ppid
        cmd[pid] = parts[2] if len(parts) > 2 else ""
    skip = {me}
    p = me
    while p in parent and parent[p] not in skip and parent[p] > 1:
        p = parent[p]; skip.add(p)
    def descends(pid):
        while pid in parent and pid > 1:
            if pid == me:
                return True
            pid = parent[pid]
        return False
    holders = set()
    r = sh(f"lsof -t +D {c.results}", check=False)
    for tok in r.stdout.split():
        if tok.isdigit():
            holders.add(int(tok))
    # working directories inside the project: `lsof -d cwd` prints one 'n<path>' per process
    inside = set()
    r = sh("lsof -a -d cwd -F pn", check=False)
    pid = None
    for line in r.stdout.splitlines():
        if line.startswith("p"):
            pid = int(line[1:])
        elif line.startswith("n") and pid is not None:
            path = line[1:]
            if path == root or path.startswith(root + os.sep):
                inside.add(pid)
    found = []
    for pid, line in cmd.items():
        if pid in skip or (descends(pid) and not ignore_children):
            continue
        if pid in holders or root in line or c.root in line or ("prove.py" in line and pid in inside):
            found.append((pid, line))
    return sorted(found)


def reap_host_watchers(ignore_children=False):
    found = host_watchers(ignore_children)
    for pid, _ in found:
        sh(f"kill -TERM {pid}", check=False)
    if found:
        time.sleep(1.0)
        for pid, _ in host_watchers(ignore_children):
            sh(f"kill -KILL {pid}", check=False)
        time.sleep(0.5)
    return found


def surviving():
    """No child the run started survives: containers, volumes, networks."""
    c = cfg()
    out = []
    for kind, cmd in (("container", "docker ps -a --format '{{.Names}}'"),
                      ("volume", "docker volume ls --format '{{.Name}}'"),
                      ("network", "docker network ls --format '{{.Name}}'")):
        names = sh(cmd, check=False).stdout.split()
        out += [f"{kind}:{n}" for n in names if n.startswith(c.project + "-") or n.startswith(c.project + "_")]
    return out


# --------------------------------------------------------------------- sampler

def build_sampler():
    """Compile the offset sampler on the host JDK against the broker image's own
    kafka-clients jar, so the transport vantage point does not live in the
    pipeline's jar and the pipeline cannot influence it."""
    c = cfg()
    out = os.path.join(c.stack_dir, "sampler")
    src = os.path.join(HERE, "sampler", "OffsetSampler.java")
    cls = os.path.join(out, "OffsetSampler.class")
    if os.path.exists(cls) and os.path.getmtime(cls) >= os.path.getmtime(src):
        return out
    os.makedirs(out, exist_ok=True)
    cid = sh(f"docker create {c.kafka_img}").stdout.strip()
    try:
        libs = sh(f"docker run --rm --entrypoint sh {c.kafka_img} -c 'ls /opt/kafka/libs'").stdout.split()
        cl = [l for l in libs if l.startswith("kafka-clients-") and l.endswith(".jar")]
        if not cl:
            raise Refusal("rig", "no kafka-clients jar in the broker image")
        sh(f"docker cp {cid}:/opt/kafka/libs/{cl[0]} {out}/{cl[0]}")
    finally:
        sh(f"docker rm -v {cid}", check=False)
    sh(f"{c.jdk}/bin/javac --release 17 -cp {out}/{cl[0]} -d {out} {HERE}/sampler/OffsetSampler.java")
    if not os.path.exists(os.path.join(out, "OffsetSampler.class")):
        raise Refusal("rig", "sampler did not compile")
    return out


def start_sampler(group):
    c = cfg()
    sdir = build_sampler()
    sh(f"docker rm -f -v {c.sampler}", check=False)
    sh(f"docker run -d --name {c.sampler} --network {c.net} -v {sdir}:/sampler:ro "
       f"--entrypoint java {c.kafka_img} -cp '/opt/kafka/libs/*:/sampler' OffsetSampler "
       f"--bootstrap={c.boot_int} --group={group} --inTopic={c.topic_in} "
       f"--outTopics={','.join(c.topics_out)} --intervalMs=500")
    # assert anything launched unattended is alive before waiting on it
    for _ in range(60):
        lg = sh(f"docker logs {c.sampler}", check=False)
        if '"sampler":"up"' in lg.stdout:
            return
        if not sh(f"docker ps -q -f name=^{c.sampler}$", check=False).stdout.strip():
            raise Refusal("rig", f"offset sampler died at startup:\n{lg.stdout}\n{lg.stderr}")
        time.sleep(1)
    raise Refusal("rig", "offset sampler never reported up")


def stop_sampler():
    sh(f"docker rm -f -v {cfg().sampler}", check=False)


def sampler_ticks_since(ts_ms):
    """Every tick the sampler printed at or after ts_ms (its whole log is read)."""
    r = sh(f"docker logs {cfg().sampler}", check=False)
    out = []
    for line in r.stdout.splitlines():
        line = line.strip()
        if line.startswith("{") and '"ts"' in line:
            try:
                t = json.loads(line)
            except Exception:
                continue
            if t.get("ts", 0) >= ts_ms:
                out.append(t)
    return out


def sampler_tail(n=8):
    r = sh(f"docker logs --tail {n} {cfg().sampler}", check=False)
    ticks = []
    for line in r.stdout.strip().splitlines():
        line = line.strip()
        if line.startswith("{") and '"ts"' in line:
            try:
                ticks.append(json.loads(line))
            except Exception:
                pass
    return ticks


# ---------------------------------------------------------------------- topics

def topic_exists(t):
    return t in kafka(f"kafka-topics.sh --bootstrap-server {cfg().boot_int} --list").stdout.split()


def create_topic(t, partitions=None, retention_bytes=None):
    c = cfg()
    partitions = partitions or c.partitions
    extra = ""
    if retention_bytes is not None:
        extra = f" --config retention.bytes={retention_bytes} --config segment.bytes=134217728"
    kafka(f"kafka-topics.sh --bootstrap-server {c.boot_int} --create --topic {t} "
          f"--partitions {partitions} --replication-factor 1{extra}")
    d = kafka(f"kafka-topics.sh --bootstrap-server {c.boot_int} --describe --topic {t}").stdout
    n = d.count("\tPartition:")
    if n != partitions:
        raise Refusal("rig", f"topic {t} created with {n} partitions, wanted {partitions}")
    if retention_bytes is not None and f"retention.bytes={retention_bytes}" not in d:
        raise Refusal("rig", f"retention.bytes did not apply on {t}: {d}")
    return d


def delete_topic(t):
    if not topic_exists(t):
        return
    kafka(f"kafka-topics.sh --bootstrap-server {cfg().boot_int} --delete --topic {t}")
    for _ in range(60):
        if not topic_exists(t):
            return
        time.sleep(1)
    raise Refusal("rig", f"topic {t} still present after delete")


def log_end(topic):
    r = kafka(f"kafka-get-offsets.sh --bootstrap-server {cfg().boot_int} --topic {topic} --time -1")
    tot, per = 0, {}
    for line in r.stdout.strip().splitlines():
        parts = line.strip().split(":")
        if len(parts) == 3 and parts[0] == topic:
            per[int(parts[1])] = int(parts[2])
            tot += int(parts[2])
    return tot, per


def delete_group(g):
    kafka(f"kafka-consumer-groups.sh --bootstrap-server {cfg().boot_int} --delete --group {g}", check=False)


def recreate_output_topics():
    c = cfg()
    for t in c.topics_out:
        delete_topic(t)
        create_topic(t, retention_bytes=T["sinkRetentionBytes"])
    for t in c.topics_out:
        tot, _ = log_end(t)
        if tot != 0:
            raise Refusal("rig", f"output topic {t} is not empty after recreate: {tot}")


def verify_backlog(manifest, topic=None):
    """After any recreating infrastructure command, re-verify the backlog against its manifest."""
    c = cfg()
    topic = topic or c.topic_in
    want = int(manifest[c.count_field])
    tot, per = log_end(topic)
    if tot != want:
        raise Refusal("rig", f"backlog on {topic} is {tot} records, manifest says {want}")
    if len(per) != c.partitions:
        raise Refusal("rig", f"backlog has {len(per)} partitions, pipeline.json says {c.partitions}")
    return tot


def fill(topic, count, seed, manifest_name):
    """Fill a backlog, write its manifest, read the log end back against it."""
    c = cfg()
    delete_topic(topic)
    create_topic(topic)
    man = os.path.join(c.results, manifest_name)
    cmd = c.fmt(c.gen_cmd, count=count, seed=seed, topic=topic, manifest=man)
    log("fill:", cmd)
    r = sh(cmd, timeout=7200)
    tail = r.stdout.strip().splitlines()[-1:] 
    log("fill done:", tail[0] if tail else "")
    m = json.load(open(man))
    if int(m[c.count_field]) != count:
        raise Refusal("rig", f"manifest says {m[c.count_field]} records, asked for {count}")
    verify_backlog(m, topic)
    log(f"backlog {topic}: {count:,} records read back over {c.partitions} partitions")
    return m


# ---------------------------------------------------------------- task manager

def tm_running():
    return bool(sh(f"docker ps -q -f name=^{cfg().tm}$", check=False).stdout.strip())


def stop_tm():
    c = cfg()
    sh(f"docker rm -f -v {c.tm}", check=False)
    for _ in range(60):
        if not tm_running():
            break
        time.sleep(1)
    else:
        raise Refusal("rig", "task manager container would not die")
    for _ in range(90):
        try:
            if rest("/overview")["taskmanagers"] == 0:
                return
        except Exception:
            pass
        time.sleep(1)
    raise Refusal("rig", "engine still reports a registered task manager after teardown")


def start_tm(cores, slots=None, reporter_s=None):
    c = cfg()
    slots = slots if slots is not None else cores
    reporter_s = reporter_s or T["reporterS"]
    stop_tm()
    props = (f"jobmanager.rpc.address: {c.jm}\n"
             f"taskmanager.numberOfTaskSlots: {slots}\n"
             f"taskmanager.memory.process.size: {c.tm_mem}\n"
             f"taskmanager.memory.managed.fraction: 0.1\n"
             f"taskmanager.memory.network.fraction: 0.15\n"
             f"taskmanager.memory.network.max: 512m\n"
             f"state.checkpoint-storage: filesystem\n"
             f"state.checkpoints.dir: file:///ckpt\n"
             # busy / idle / back-pressured come from the SLF4J reporter plugin on
             # the worker: the JobManager REST back-pressure path is deprecated on
             # 1.20 and per-vertex metrics come back empty under load.
             f"metrics.reporter.slf4j.factory.class: org.apache.flink.metrics.slf4j.Slf4jReporterFactory\n"
             f"metrics.reporter.slf4j.interval: {reporter_s} SECONDS\n"
             f"metrics.reporter.slf4j.scope.variables.excludes: job_id;task_id;task_attempt_id;tm_id\n")
    sh(f"docker run -d --name {c.tm} --hostname {c.tm} --network {c.net} --user 0:0 "
       f"--cpus {cores} --memory {c.tm_mem_limit} "
       f"-v {c.ckpt_vol}:/ckpt -v {c.jar_dir}:/jobs:ro "
       f"-e FLINK_PROPERTIES=$'{props}' {c.flink_img} taskmanager")
    nano = assert_cap(c.tm, cores)
    for _ in range(120):
        try:
            o = rest("/overview")
            if o["taskmanagers"] == 1 and o["slots-total"] == slots:
                return nano
        except Exception:
            pass
        if not tm_running():
            tail = sh(f"docker logs --tail 40 {c.tm}", check=False).stdout
            raise Refusal("rig", f"task manager died during startup:\n{tail}")
        time.sleep(1)
    raise Refusal("rig", f"task manager never registered {slots} slots")


def assert_cap(container, cores):
    """GUARD: read the cap back from the container, never from the env var."""
    nano = int(sh(f"docker inspect -f '{{{{.HostConfig.NanoCpus}}}}' {container}").stdout.strip())
    if nano != int(round(cores * 1_000_000_000)):
        raise Refusal("rig", f"cpu cap did not apply on {container}: NanoCpus={nano}, wanted {int(cores*1e9)}")
    cpumax = sh(f"docker exec {container} cat /sys/fs/cgroup/cpu.max").stdout.split()
    if len(cpumax) != 2 or cpumax[0] == "max" or abs(int(cpumax[0]) / int(cpumax[1]) - cores) > 1e-6:
        raise Refusal("rig", f"cgroup cpu.max={cpumax} on {container} does not equal {cores} cores")
    return nano


def cgroup_cpu(container):
    r = sh(f"docker exec {container} cat /sys/fs/cgroup/cpu.stat")
    d = {}
    for line in r.stdout.strip().splitlines():
        k, v = line.split()
        d[k] = int(v)
    return d


# ------------------------------------------------------------------------- job

def submit_job(par, group, ckpt_ms=None):
    c = cfg()
    args = c.fmt(c.job_args, par=par, group=group, ckptMs=ckpt_ms or c.ckpt_ms)
    r = sh(f"docker exec {c.jm} flink run -d -p {par} -c {c.main_class} {c.jar_in_ctr} {args}", timeout=300)
    m = re.search(r"JobID\s+([0-9a-f]{32})", r.stdout + r.stderr)
    if not m:
        raise Refusal("rig", f"could not read a JobID back from submit:\n{r.stdout}\n{r.stderr}")
    return m.group(1)


def wait_running(jid, par, timeout=180):
    """GUARD: no job is actually running / parallelism != requested."""
    for _ in range(timeout):
        try:
            j = rest(f"/jobs/{jid}")
            if j["state"] == "RUNNING" and all(v["status"] == "RUNNING" for v in j["vertices"]):
                bad = [(v["name"], v["parallelism"]) for v in j["vertices"] if v["parallelism"] != par]
                if bad:
                    raise Refusal("rig", f"vertex parallelism != {par}: {bad}")
                return j
            if j["state"] in ("FAILED", "CANCELED", "FINISHED"):
                raise Refusal("rig", f"job reached {j['state']} instead of RUNNING")
        except Refusal:
            raise
        except Exception:
            pass
        time.sleep(1)
    raise Refusal("rig", "job never reached RUNNING")


def graph_shape(jid):
    """Read the shape off the RUNNING plan: vertex descriptions and edge ship strategies."""
    plan = rest(f"/jobs/{jid}/plan")["plan"]
    sig = []
    for n in sorted(plan["nodes"], key=lambda x: x["description"]):
        ships = sorted(i.get("ship_strategy", "?") for i in n.get("inputs", []))
        sig.append([n["description"], ships])
    return {"vertexCount": len(plan["nodes"]), "signature": sig}


def cancel_job(jid):
    c = cfg()
    if not jid:
        return
    try:
        rest_patch(f"/jobs/{jid}?mode=cancel")
    except Exception:
        sh(f"docker exec {c.jm} flink cancel {jid}", check=False)
    for _ in range(90):
        try:
            if rest(f"/jobs/{jid}")["state"] in ("CANCELED", "FINISHED", "FAILED"):
                return
        except Exception:
            return
        time.sleep(1)
    raise Refusal("rig", f"job {jid} would not cancel")


def assert_cluster_idle():
    """GUARD: assert idle by asking the engine, not by killing what you think is there."""
    o = rest("/overview")
    if o["jobs-running"] != 0:
        raise Refusal("rig", f"cluster is still busy: {o['jobs-running']} jobs running")
    if o["taskmanagers"] != 0:
        raise Refusal("rig", f"a task manager from the last case is still registered: {o}")


GC_RE = re.compile(r"\.Status\.JVM\.GarbageCollector\.(?P<gc>[^.]+)\.(?P<metric>Time|Count): (?P<val>[\d.]+)\s*$")
BP_RE = re.compile(r"\.(?P<vertex>[^.]+?)\.(?P<sub>\d+)\."
                   r"(?P<metric>busyTimeMsPerSecond|idleTimeMsPerSecond|backPressuredTimeMsPerSecond): (?P<val>[\d.]+)\s*$")


def backpressure_in_window(t_open, t_close):
    """busy / idle / back-pressured per vertex, averaged over the reporter samples
    that fall INSIDE the window, read from the worker's SLF4J reporter log.
    Internal back-pressure inside a capped worker is reported and gated on
    nothing; source IDLE is the external boundary and is gated."""
    c = cfg()
    r = sh(f"docker logs --timestamps {c.tm}", check=False, timeout=120)
    acc, gc = {}, {}
    for line in (r.stdout + r.stderr).splitlines():
        sp = line.split(" ", 1)
        if len(sp) != 2:
            continue
        try:
            ts = calendar.timegm(time.strptime(sp[0][:19], "%Y-%m-%dT%H:%M:%S"))  # docker emits UTC
        except Exception:
            continue
        if not (t_open <= ts <= t_close):
            continue
        g = GC_RE.search(sp[1])
        if g:
            gc.setdefault(f"{g['gc']}.{g['metric']}", []).append(float(g["val"]))
            continue
        m = BP_RE.search(sp[1])
        if m:
            acc.setdefault(m["vertex"], {}).setdefault(m["metric"], []).append(float(m["val"]))
    out = {}
    for vtx, mm in acc.items():
        out[vtx] = {k.replace("TimeMsPerSecond", ""): round(sum(v) / len(v) / 1000.0, 4) for k, v in mm.items()}
        out[vtx]["samples"] = min(len(v) for v in mm.values())
    out["_gc"] = {k: (max(v) - min(v)) for k, v in gc.items()}
    return out


# ------------------------------------------------------------------- the case

def next_boundary(after=None, timeout=90):
    """The window is anchored on the committed offset advancing, never on wall clock."""
    base = after
    t0 = time.time()
    while time.time() - t0 < timeout:
        for t in sampler_tail(8):
            if t.get("committed", -1) < 0:
                continue
            if base is None:
                base = t
                continue
            if t["committed"] > base["committed"] and t["ts"] > base["ts"]:
                return t
            drained(t)
        time.sleep(0.4)
    raise Refusal("case", f"committed offset did not advance within {timeout}s")


def drained(tick):
    """The backlog ran out under the job: a sizing error of the caller's, named as such."""
    if tick.get("endIn", 0) > 0 and tick["committed"] >= tick["endIn"]:
        raise Refusal("case", f"backlog drained ({tick['endIn']:,} records) before the window closed — "
                              f"size it for warm-up + window at the largest case's rate")


def warmup_verdict(rates, elapsed_s):
    """Pure: is this sequence of interval rates a flat trend? Returns (ok, detail)."""
    n = len(rates)
    mean = sum(rates) / n
    if mean <= 0:
        return False, {"mean": mean}
    xs = list(range(n))
    xm = sum(xs) / n
    slope = sum((xs[i] - xm) * (rates[i] - mean) for i in range(n)) / sum((x - xm) ** 2 for x in xs)
    drift = abs(slope * (n - 1)) / mean
    scatter = (max(rates) - min(rates)) / mean
    ok = (drift < T["warmupFlatTol"] and scatter <= T["warmupScatterTol"] and elapsed_s >= T["warmupMinS"])
    return ok, {"rates": rates, "mean": mean, "drift": round(drift, 4), "scatter": round(scatter, 4),
                "warmupS": round(elapsed_s, 1)}


def wait_flat(deadline_s):
    """Warm up to a flat trend across N commit intervals, not a round number."""
    boundaries, last, detail = [], None, {}
    t0 = time.time()
    k = T["warmupIntervals"]
    while time.time() - t0 < deadline_s:
        for t in sampler_tail(6):
            if t.get("committed", -1) < 0:
                continue
            if last is None or t["committed"] > last[1]:
                if last is not None and t["ts"] > last[0]:
                    boundaries.append((t["ts"], t["committed"]))
                last = (t["ts"], t["committed"])
            drained(t)
        if len(boundaries) >= k + 1:
            b = boundaries[-(k + 1):]
            rates = [(b[i + 1][1] - b[i][1]) / ((b[i + 1][0] - b[i][0]) / 1000.0) for i in range(k)]
            ok, detail = warmup_verdict(rates, time.time() - t0)
            if ok:
                return detail
        time.sleep(0.5)
    raise Refusal("case", f"warm-up never reached a flat trend (slope <{T['warmupFlatTol']:.0%}, "
                          f"scatter <={T['warmupScatterTol']:.0%}, >= {T['warmupMinS']:.0f}s) within {deadline_s:.0f}s; "
                          f"last intervals {[round(r) for r in detail.get('rates', [])]} "
                          f"drift {detail.get('drift')} scatter {detail.get('scatter')}")


def check_case(rec, cores, is_baseline):
    """Pure guards on a finished case record. Every refusal here is about the
    case's DATA. `selftest` feeds this synthetic records."""
    c = cfg()
    if rec["boundaries"] < 3:
        raise Refusal("case", f"only {rec['boundaries']} commit boundaries inside the window")
    if rec["recordsConsumed"] <= 0 or rec["elapsedS"] <= 0:
        raise Refusal("case", f"measured rate is not positive: {rec['recordsConsumed']} records in {rec['elapsedS']}s")
    if rec.get("rateSource", "").startswith("engine"):
        raise Refusal("case", "rate came from the engine, not the transport")
    if rec["vantageDisagreement"] > T["vantageTol"]:
        raise Refusal("case", f"vantage points disagree by {rec['vantageDisagreement']:.1%} "
                              f"(source committed {rec['recordsConsumed']}, sinks imply {rec['vantageSinkRecords']:.0f}) "
                              f"> tolerance {T['vantageTol']:.0%}")
    if rec["backlogRemaining"] < rec["recordsPerSec"] * c.ckpt_s:
        raise Refusal("case", f"backlog headroom at close is {rec['backlogRemaining']} records "
                              f"({rec['headroomS']:.1f}s) < one checkpoint interval at the measured rate")
    if rec.get("sourceIdle") is None or (rec.get("bpSamples") or 0) < T["minBpSamples"]:
        raise Refusal("case", f"{rec.get('bpSamples') or 0} source back-pressure samples inside the window "
                              f"(< {T['minBpSamples']}): the external-boundary guard would not have been evaluated")
    floor = T["capFloorBaseline"] if is_baseline else T["capFloorOther"]
    if rec["tmCapFrac"] < floor:
        raise Refusal("case", f"task manager used {rec['tmCapFrac']:.1%} of its {cores}-core cap "
                              f"(floor {floor:.0%}) — it is not the constraint")
    if rec["sourceIdle"] > T["sourceIdleCeil"]:
        raise Refusal("case", f"source idle {rec['sourceIdle']:.1%} > {T['sourceIdleCeil']:.0%}: "
                              f"the source waited on input for more of the window than any at-cap case on record")


def check_shape(shape, shape_ref):
    """GUARD: the job graph differs from the other cases — a RIG refusal."""
    if shape_ref is not None and shape != shape_ref:
        raise Refusal("rig", f"job graph shape differs from the other cases:\n{shape}\nvs\n{shape_ref}")


def run_case(cores, pass_id, run_id, shape_ref, is_baseline, manifest,
             min_boundaries=None, min_window_s=None, ckpt_ms=None, warmup_max_s=None, reporter_s=None,
             kafka_cap=None, parallelism=None):
    """One measured case. The job is torn down on every exit path.
    kafka_cap: the broker's cap *during this case* — the ceiling run steps it
    below the configured one, and the fraction must be read against the step.
    parallelism: slots and job parallelism, when they are not the core count
    (plan 12 phase 1C: the one-core case at the suite's parallelism)."""
    c = cfg()
    kafka_cap = kafka_cap or c.kafka_cap
    par = parallelism or cores
    min_boundaries = min_boundaries or T["minBoundaries"]
    min_window_s = min_window_s if min_window_s is not None else T["minWindowS"]
    ckpt_ms = ckpt_ms or c.ckpt_ms
    ckpt_s = ckpt_ms / 1000.0
    group = f"{c.project}-{run_id}-c{cores}-{pass_id}"
    jid = None
    rec = {"cores": cores, "pass": pass_id, "group": group}
    try:
        free = host_free_bytes()
        rec["hostFreeGB"] = round(free / 1e9, 1)
        # GUARD: host free disk before the case, not after the disk is full
        if free < T["diskFloorBytes"]:
            raise Refusal("rig", f"host free disk {free/1e9:.1f} GB is below the floor {T['diskFloorBytes']/1e9:.0f} GB")
        assert_cluster_idle()
        delete_group(group)
        recreate_output_topics()
        verify_backlog(manifest)

        rec["nanoCpus"] = start_tm(cores, slots=par, reporter_s=reporter_s)
        rec["reporterS"] = reporter_s or T["reporterS"]
        rec["parallelism"] = par
        o = rest("/overview")
        # GUARD: parallelism == allocated slots (== cap unless overridden)
        if o["slots-total"] != par:
            raise Refusal("rig", f"slots-total {o['slots-total']} != parallelism {par}")
        rec["slotsTotal"] = o["slots-total"]

        start_sampler(group)
        rec["tSubmit"] = time.time()
        jid = submit_job(par, group, ckpt_ms)
        wait_running(jid, par)
        rec["jobId"] = jid
        o = rest("/overview")
        if o["slots-available"] != 0:
            raise Refusal("rig", f"job is not using every slot: {o}")
        shape = graph_shape(jid)
        rec["shape"] = shape
        check_shape(shape, shape_ref)

        rec["warmup"] = wait_flat(warmup_max_s or T["warmupMaxS"])
        rec["tSteady"] = time.time()

        open_tick = next_boundary()
        cpu0_tm, cpu0_k = cgroup_cpu(c.tm), cgroup_cpu(c.kafka)
        rec["tOpen"] = time.time()
        rec["open"] = open_tick
        boundaries, last, close_tick = 1, open_tick, None
        while True:
            tick = next_boundary(after=last)
            boundaries += 1
            last = tick
            elapsed = (tick["ts"] - open_tick["ts"]) / 1000.0
            if boundaries - 1 >= min_boundaries and elapsed >= min_window_s:
                close_tick = tick
                break
            if elapsed > T["windowMaxS"]:
                raise Refusal("case", f"window never closed within {T['windowMaxS']:.0f} s")
        cpu1_tm, cpu1_k = cgroup_cpu(c.tm), cgroup_cpu(c.kafka)
        rec["tClose"] = time.time()
        rec["close"] = close_tick
        rec["boundaries"] = boundaries - 1

        bp = backpressure_in_window(rec["tOpen"], rec["tClose"])
        elapsed = (close_tick["ts"] - open_tick["ts"]) / 1000.0
        d_committed = close_tick["committed"] - open_tick["committed"]
        rec["elapsedS"] = round(elapsed, 2)
        rec["recordsConsumed"] = d_committed
        rate = d_committed / elapsed if elapsed > 0 else 0.0
        rec["rateSource"] = "kafka committed offsets on " + c.topic_in
        rec["recordsPerSec"] = round(rate, 1)
        rec["outputRecsPerSec"] = round(rate * c.out_per_in, 1)

        d_out = sum(close_tick[f"end_{t}"] - open_tick[f"end_{t}"] for t in c.topics_out)
        implied = d_out / c.out_per_in
        rec["vantageSinkRecords"] = round(implied, 1)
        rec["vantageDisagreement"] = round(abs(implied - d_committed) / d_committed, 4) if d_committed else 1.0

        remaining = close_tick["endIn"] - close_tick["committed"]
        rec["backlogRemaining"] = remaining
        rec["headroomS"] = round(remaining / rate, 1) if rate else 0.0

        tm_cores = (cpu1_tm["usage_usec"] - cpu0_tm["usage_usec"]) / 1e6 / elapsed
        k_cores = (cpu1_k["usage_usec"] - cpu0_k["usage_usec"]) / 1e6 / elapsed
        rec["tmCores"] = round(tm_cores, 3)
        rec["tmCapFrac"] = round(tm_cores / cores, 4)
        d_per = max(1, cpu1_tm["nr_periods"] - cpu0_tm["nr_periods"])
        rec["tmThrottledPeriodsPct"] = round(100.0 * (cpu1_tm["nr_throttled"] - cpu0_tm["nr_throttled"]) / d_per, 1)
        rec["kafkaCores"] = round(k_cores, 3)
        rec["kafkaCap"] = kafka_cap
        rec["kafkaCapFrac"] = round(k_cores / kafka_cap, 4)

        gcm = bp.pop("_gc", {})
        gc_ms = sum(v for k, v in gcm.items() if k.endswith(".Time"))
        rec["gcMsInWindow"] = round(gc_ms, 1)
        rec["gcFracOfCapacity"] = round(gc_ms / 1000.0 / (elapsed * cores), 4)
        src_name = [n for n in bp if c.source_match.lower() in n.lower()]
        src = bp[src_name[0]] if src_name else {}
        rec["sourceIdle"] = src.get("idle")
        rec["sourceBusy"] = src.get("busy")
        rec["sourceBackpressured"] = src.get("backPressured")
        rec["bpSamples"] = src.get("samples")
        rec["backpressure"] = bp

        check_case(rec, cores, is_baseline)
        rec["status"] = "OK"
        return rec, shape
    except Refusal as e:
        rec["status"] = "REFUSED"
        rec["refusalScope"] = e.scope
        rec["refusal"] = e.msg
        raise CaseRefused(rec, e)
    finally:
        try:
            cancel_job(jid)
        finally:
            # keep the ticks: a vantage refusal on the noise rig (4c, 33.9%, 2026-09-04)
            # could not be examined because the sampler went down with its log
            try:
                rec["ticks"] = sampler_ticks_since(int((rec.get("tOpen") or time.time()) * 1000) - 20000)
            except Exception:
                rec["ticks"] = None
            stop_sampler()
            stop_tm()


# ------------------------------------------------------------------ the table

def build_table(runs, cases_order=None):
    """Pure: per-case means, spreads, reportability, step ratios, order effect.
    Fed by the suite, by `selftest`, and by `replay` over the record."""
    ok = {}
    for r in runs:
        if r.get("status", "OK") == "OK":
            ok.setdefault(int(r["cores"]), []).append(r)
    cases = {}
    for cores, rs in sorted(ok.items()):
        rates = [r["recordsPerSec"] for r in rs]
        mean = sum(rates) / len(rates)
        spread = (max(rates) - min(rates)) / mean if mean else 1.0
        entry = {"cores": cores, "passes": len(rates), "meanRecordsPerSec": round(mean, 1),
                 "minRecordsPerSec": min(rates), "maxRecordsPerSec": max(rates),
                 "spread": round(spread, 4), "reportable": True}
        if QUICK:
            entry.update(quickLook=True, publishable=False)
        if len(rates) < T["minPasses"] and not QUICK:
            entry.update(reportable=False, unreportableReason=f"{len(rates)} pass(es) < {T['minPasses']}")
        elif spread > T["spreadCeil"]:
            entry.update(reportable=False,
                         unreportableReason=f"spread {spread:.1%} > {T['spreadCeil']:.0%} ceiling")
        for k in ("tmCores", "tmCapFrac", "tmThrottledPeriodsPct", "kafkaCores", "sourceIdle",
                  "sourceBackpressured", "headroomS", "gcFracOfCapacity"):
            vals = [r[k] for r in rs if r.get(k) is not None]
            if vals:
                entry[k] = round(sum(vals) / len(vals), 4)
        vd = [r["vantageDisagreement"] for r in rs if r.get("vantageDisagreement") is not None]
        entry["vantageDisagreementMax"] = round(max(vd), 4) if vd else None
        cases[cores] = entry
    ratios = []
    ks = sorted(cases)
    for i in range(len(ks) - 1):
        a, b = ks[i], ks[i + 1]
        ca, cb = cases[a], cases[b]
        entry = {"step": f"{a}->{b}", "from": a, "to": b, "idealRatio": b / a}
        if ca["reportable"] and cb["reportable"]:
            r = cb["meanRecordsPerSec"] / ca["meanRecordsPerSec"]
            entry.update(ratio=round(r, 3),
                         ratioLow=round(cb["minRecordsPerSec"] / ca["maxRecordsPerSec"], 3),
                         ratioHigh=round(cb["maxRecordsPerSec"] / ca["minRecordsPerSec"], 3),
                         efficiency=round(r / (b / a), 4), reportable=True)
        else:
            entry.update(reportable=False,
                         voidedBy=[x for x in (a, b) if not cases[x]["reportable"]],
                         reason="voided: " + "; ".join(f"{x}c {cases[x]['unreportableReason']}"
                                                      for x in (a, b) if not cases[x]["reportable"]))
        ratios.append(entry)
    order = {}
    for cores, rs in sorted(ok.items()):
        asc = [r["recordsPerSec"] for r in rs if str(r.get("pass", "")).endswith("asc")]
        desc = [r["recordsPerSec"] for r in rs if str(r.get("pass", "")).endswith("desc")]
        o = {"ascendingMean": round(sum(asc) / len(asc), 1) if asc else None,
             "descendingMean": round(sum(desc) / len(desc), 1) if desc else None}
        if asc and desc:
            o["descOverAsc"] = round((sum(desc) / len(desc)) / (sum(asc) / len(asc)), 4)
        order[cores] = o
    # sentinel: the baseline measured first and last. No threshold of its own —
    # a drift shows up as baseline spread against the 20% ceiling, which the
    # record has replayed; plan 12 phase 1 measured a 10% spread with no drift.
    sentinel = None
    sent = [r for r in runs if r.get("pass") == "sentinel" and r.get("status", "OK") == "OK"]
    if sent:
        last = sent[-1]
        first = next((r for r in runs if int(r["cores"]) == int(last["cores"]) and r is not last
                      and r.get("status", "OK") == "OK"), None)
        if first:
            f, l = first["recordsPerSec"], last["recordsPerSec"]
            sentinel = {"cores": int(last["cores"]), "firstPass": first.get("pass"), "firstRecordsPerSec": f,
                        "lastRecordsPerSec": l, "drift": round((l - f) / ((f + l) / 2), 4)}
    return {"cases": cases, "stepRatios": ratios, "orderEffect": order, "sentinel": sentinel,
            "quickLook": QUICK, "publishable": not QUICK}


def render_table(out):
    """The table as text, from the results file. Every artifact that carries a
    number is generated from here."""
    t = out["table"]
    c = cfg()
    L = []
    L.append("=" * 118)
    if out.get("table", {}).get("quickLook"):
        L.append("!! " + "QUICK LOOK — one pass per case. No spread, so no table: these numbers say the rig ran clean and roughly how fast, and nothing about how repeatable the ratio is. The record's own passes read 2.04-2.27x where a suite reported 2.15x. Do not publish or quote.")
        L.append("=" * 118)
    L.append(f"axis                 : {out['axis']}")
    L.append(f"API level            : {out['apiLevel']}")
    L.append(f"guarantee            : state = {out['guarantee']['state']}; sink = {out['guarantee']['sink']}")
    L.append(f"checkpoint interval  : {out['checkpointIntervalMs']} ms")
    L.append(f"build hash           : {out['buildHash']}  (completeness passed for {out['completenessBuild']})")
    L.append(f"passes per case      : {out['passesPerCase']}")
    L.append(f"backlog              : {out['backlogRecords']:,} records, {out['partitions']} partitions, "
             f"{c.out_per_in:g} outputs per input")
    L.append("=" * 118)
    hdr = (f"{'cores':>5} {'pass':>8} {'records/s':>11} {'out rec/s':>11} {'tm cpu':>10} {'%cap':>6} "
           f"{'thr%':>5} {'kafka':>10} {'srcIdle':>8} {'srcBP':>7} {'hdrm':>6} {'vant':>6} {'status':>8}")
    L.append(hdr)
    L.append("-" * len(hdr))
    for r in out["runs"]:
        if r.get("status") == "OK":
            L.append(f"{r['cores']:>5} {r['pass']:>8} {r['recordsPerSec']:>11,.0f} {r['outputRecsPerSec']:>11,.0f} "
                     f"{r['tmCores']:>6.2f}/{r['cores']:<3} {r['tmCapFrac']:>5.1%} {r['tmThrottledPeriodsPct']:>5.0f} "
                     f"{r['kafkaCores']:>6.2f}/{c.kafka_cap:<3g} {r['sourceIdle']:>7.1%} {r['sourceBackpressured']:>6.1%} "
                     f"{r['headroomS']:>5.0f}s {r['vantageDisagreement']:>5.1%} {'OK':>8}")
        else:
            L.append(f"{r['cores']:>5} {r['pass']:>8} {'—':>11} {'—':>11} {'—':>10} {'—':>6} {'—':>5} "
                     f"{'—':>10} {'—':>8} {'—':>7} {'—':>6} {'—':>6} {'REFUSED':>8}")
            L.append(f"        refusal ({r.get('refusalScope')}): {r.get('refusal')}")
    L.append("-" * len(hdr))
    for cs in t["cases"].values():
        mark = "" if cs["reportable"] else f"   UNREPORTABLE ({cs['unreportableReason']})"
        L.append(f"{cs['cores']:>5} {'MEAN':>8} {cs['meanRecordsPerSec']:>11,.0f} "
                 f"{cs['meanRecordsPerSec']*c.out_per_in:>11,.0f} "
                 f"{cs.get('tmCores',0):>6.2f}/{cs['cores']:<3} {cs.get('tmCapFrac',0):>5.1%} "
                 f"{cs.get('tmThrottledPeriodsPct',0):>5.0f} {cs.get('kafkaCores',0):>6.2f}/{c.kafka_cap:<3g} "
                 f"{cs.get('sourceIdle',0):>7.1%} {cs.get('sourceBackpressured',0):>6.1%} "
                 f"{cs.get('headroomS',0):>5.0f}s {cs.get('vantageDisagreementMax') or 0:>5.1%} "
                 f"  spread {cs['spread']:.1%}{mark}")
    L.append("=" * 118)
    for r in t["stepRatios"]:
        if r["reportable"]:
            L.append(f"STEP {r['step']} cores: {r['ratio']:.3f}x  (ideal {r['idealRatio']:.0f}x, "
                     f"efficiency {r['efficiency']:.1%}, range across passes {r['ratioLow']:.3f}x-{r['ratioHigh']:.3f}x)")
        else:
            L.append(f"STEP {r['step']} cores: NOT REPORTED — {r['reason']}")
    L.append("order effect (descending / ascending): " +
             str({k: v.get("descOverAsc") for k, v in t["orderEffect"].items()}))
    sd = t.get("sentinel")
    if sd:
        L.append(f"sentinel: {sd['cores']}c first {sd['firstRecordsPerSec']:,.0f} -> last {sd['lastRecordsPerSec']:,.0f} "
                 f"rec/s, drift {sd['drift']:+.1%} across the suite (counted in the {sd['cores']}c spread)")
    if out.get("stoppedEarly"):
        L.append(f"STOPPED EARLY: {out['stoppedEarly']['reason']} — {out['stoppedEarly']['message']}")
    L.append("=" * 118)
    return "\n".join(L)


def render_markdown(out):
    """The same table for a report."""
    t = out["table"]
    c = cfg()
    L = ([f"> **QUICK LOOK — not a result.** QUICK LOOK — one pass per case. No spread, so no table: these numbers say the rig ran clean and roughly how fast, and nothing about how repeatable the ratio is.", ""]
         if out.get("table", {}).get("quickLook") else [])
    L += ["| field | value |", "|---|---|",
         f"| axis | {out['axis']} |", f"| API level | {out['apiLevel']} |",
         f"| guarantee | state: {out['guarantee']['state']}; sink: {out['guarantee']['sink']} |",
         f"| checkpoint interval | {out['checkpointIntervalMs']} ms |",
         f"| build hash | `{out['buildHash']}` (completeness passed for `{out['completenessBuild']}`) |",
         f"| passes per case | {out['passesPerCase']} |",
         f"| rate source | committed broker offsets on `{c.topic_in}` |",
         f"| CPU source | cgroup `cpu.stat usage_usec` |", ""]
    for r in t["stepRatios"]:
        if r["reportable"]:
            L.append(f"**{r['step'].replace('->', '→')} cores: {r['ratio']:.2f}× ({r['efficiency']:.0%} of linear), "
                     f"range {r['ratioLow']:.2f}–{r['ratioHigh']:.2f}× across passes.**")
        else:
            L.append(f"**{r['step'].replace('->', '→')} cores: not reported — {r['reason']}.**")
    L += ["", "| cores | pass | records/s | tm cores | % of cap | throttled | broker cores | src idle | src BP | headroom | vantage |",
          "|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|"]
    for r in out["runs"]:
        if r.get("status") == "OK":
            L.append(f"| {r['cores']} | {r['pass']} | {r['recordsPerSec']:,.0f} | {r['tmCores']:.2f} | {r['tmCapFrac']:.1%} | "
                     f"{r['tmThrottledPeriodsPct']:.0f}% | {r['kafkaCores']:.2f} / {c.kafka_cap:g} | {r['sourceIdle']:.1%} | "
                     f"{r['sourceBackpressured']:.1%} | {r['headroomS']:.0f} s | {r['vantageDisagreement']:.2%} |")
        else:
            L.append(f"| {r['cores']} | {r['pass']} | REFUSED ({r.get('refusalScope')}) — {r.get('refusal','')[:80]} | | | | | | | | |")
    L += ["", "| cores | passes | mean records/s | spread | reportable |", "|---:|---:|---:|---:|---|"]
    for cs in t["cases"].values():
        L.append(f"| {cs['cores']} | {cs['passes']} | {cs['meanRecordsPerSec']:,.0f} | {cs['spread']:.1%} | "
                 f"{'yes' if cs['reportable'] else 'no — ' + cs['unreportableReason']} |")
    sd = t.get("sentinel")
    if sd:
        L += ["", f"Sentinel: the {sd['cores']}-core case first ({sd['firstRecordsPerSec']:,.0f} rec/s) and last "
                  f"({sd['lastRecordsPerSec']:,.0f} rec/s), drift {sd['drift']:+.1%} across the suite; "
                  f"counted in that case's spread."]
    return "\n".join(L) + "\n"
