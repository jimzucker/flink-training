"""Build the reference pipeline's Grafana dashboard in the skill's four rows
(SKILL.md section 7): pipeline output, lag and job health, Kafka, Flink.

    python3 build_dashboard.py grafana/dashboards/refpipe.json

Each panel is titled "<what it measures> — <what to look for>"; why it matters
is the panel's description. The data source is the provisioned uid st44prom.
Late records dropped is left out: this job writes its hourly window by hand in
a KeyedProcessFunction, and Flink counts late records only in its own window
operators, so the panel would read "No data".
"""
import json, sys

AGG = 'operator_name=~".*hourly_average_by_location.*"'
PARSE = 'operator_name=~".*parse_reading.*"'
KAFKA = 'name=~".*-kafka"'
panels, y = [], 0
pid = [0]

def nid():
    pid[0] += 1
    return pid[0]

def row(title):
    global y
    panels.append({"type": "row", "id": nid(), "title": title, "collapsed": False,
                   "gridPos": {"h": 1, "w": 24, "x": 0, "y": y}, "panels": []})
    y += 1

def ts(title, desc, targets, x, w, h=8, unit="short", log=False, maxv=None, thresh=None):
    d = {"unit": unit, "custom": {"lineWidth": 2, "fillOpacity": 8, "showPoints": "never",
                                  "spanNulls": 60000, "axisSoftMin": 0}}
    if log:
        d["custom"]["scaleDistribution"] = {"type": "log", "log": 10}
        del d["custom"]["axisSoftMin"]
    if maxv is not None:
        d["max"] = maxv
    if thresh is not None:
        d["thresholds"] = {"mode": "absolute", "steps": [{"color": "green", "value": None},
                                                          {"color": "red", "value": thresh}]}
        d["custom"]["thresholdsStyle"] = {"mode": "line+area"}
    panels.append({"type": "timeseries", "id": nid(), "title": title, "description": desc,
                   "datasource": {"type": "prometheus", "uid": "st44prom"},
                   "gridPos": {"h": h, "w": w, "x": x, "y": y},
                   "fieldConfig": {"defaults": d, "overrides": []},
                   "options": {"legend": {"displayMode": "list", "placement": "bottom"},
                               "tooltip": {"mode": "multi", "sort": "desc"}},
                   "targets": [{"refId": chr(65 + i), "expr": e, "legendFormat": l}
                               for i, (l, e) in enumerate(targets)]})

def stat(title, desc, expr, x, w, h=4, unit="short", red_above=None):
    steps = [{"color": "green", "value": None}]
    if red_above is not None:
        steps.append({"color": "red", "value": red_above})
    panels.append({"type": "stat", "id": nid(), "title": title, "description": desc,
                   "datasource": {"type": "prometheus", "uid": "st44prom"},
                   "gridPos": {"h": h, "w": w, "x": x, "y": y},
                   "fieldConfig": {"defaults": {"unit": unit, "thresholds": {"mode": "absolute", "steps": steps}},
                                   "overrides": []},
                   "options": {"reduceOptions": {"calcs": ["lastNotNull"]}, "colorMode": "background",
                               "graphMode": "area", "textMode": "value"},
                   "targets": [{"refId": "A", "expr": expr}]})

row("1. Pipeline output")
ts("Input and output rate, log scale — both flat during a case",
   "Rate per stage, in this pipeline's shape: one aggregation and an hourly output, so the two lines "
   "sit orders of magnitude apart and the scale is logarithmic.",
   [("readings parsed per second", f"sum(flink_taskmanager_job_task_operator_numRecordsOutPerSecond{{{PARSE}}})"),
    ("hourly rows published per second", f"sum(flink_taskmanager_job_task_operator_numRecordsOutPerSecond{{{AGG}}})")],
   0, 8, log=True)
ts("Readings per output row — stays at 36,000",
   "The cardinality question in this pipeline's shape: each hourly row per location averages a fixed "
   "number of readings. A line away from 36,000 means keys or windows are not the ones predicted.",
   [("readings per published row", f"sum(rate(flink_taskmanager_job_task_operator_numRecordsIn{{{AGG}}}[2m])) / "
     f"clamp_min(sum(rate(flink_taskmanager_job_task_operator_numRecordsOut{{{AGG}}}[2m])), 0.0001)")],
   8, 8)
ts("Input rate per subtask — lines overlap",
   "The two-paths question in this pipeline's shape: every subtask of the aggregation should read the "
   "same. One falling away from the others is a key spread or a partition problem.",
   [("subtask {{subtask_index}}", f"sum by (subtask_index) (flink_taskmanager_job_task_operator_numRecordsInPerSecond{{{AGG}}})")],
   16, 8)
y += 8

row("2. Lag and job health")
ts("Records waiting in Kafka — falls steadily in each case",
   "Records waiting in Kafka for the job to read. In a measured drain it falls steadily; flat at zero "
   "means the case ran out of input.",
   [("readings not yet read", "sum(flink_taskmanager_job_task_operator_pendingRecords)")], 0, 12)
ts("Event-time lag — flat on a live pipeline",
   "How far the time in the records being read lags behind the clock. On a pipeline left running this "
   "is how stale the outputs are. In a drain it only reads the age of the backlog.",
   [("lag", "max(flink_taskmanager_job_task_operator_currentEmitEventTimeLag) / 1000")], 12, 12, unit="s")
y += 8
stat("Job restarts — 0", "Times the job failed and recovered. Anything above zero outside a planned kill "
     "means readings were taken across a failure.", "max(flink_jobmanager_job_numRestarts)", 0, 6, red_above=1)
stat("Failed checkpoints — 0", "Checkpoints that did not complete. While they fail, a crash would replay "
     "from further back.", "max(flink_jobmanager_job_numberOfFailedCheckpoints)", 6, 6, red_above=1)
stat("Completed checkpoints — keeps rising", "The guarantee being kept, for comparison with the failures beside it.",
     "max(flink_jobmanager_job_numberOfCompletedCheckpoints)", 12, 6)
stat("Job uptime — resets on a restart", "How long the running job has been up since its last start.",
     "max(flink_jobmanager_job_uptime)", 18, 6, unit="ms")
y += 4

row("3. Kafka")
ts("CPU by container, cores — pipeline at its cap, the rest low",
   "Every container's CPU. The pipeline sitting at its cap while the broker idles means the pipeline "
   "is the constraint; the broker pinned while the pipeline falls off its cap means the reverse.",
   [("{{name}}", "rate(docker_container_cpu_seconds_total[1m])")], 0, 12)
ts("Broker memory and limit — filling to the limit is normal",
   "The broker's memory, page cache included, against its container limit. Page cache grows until it "
   "meets the limit, so touching the line is normal. The harness's own broker-memory check is what "
   "says whether reads were pushed to disk.",
   [("in use", f"max(docker_container_memory_bytes{{{KAFKA}}})"),
    ("limit", f"max(docker_container_memory_limit_bytes{{{KAFKA}}})")], 12, 12, unit="bytes")
y += 8

row("4. Flink")
ts("Task busy, back-pressured and idle time — busy near 100%",
   "Busy near 100% is working at the limit. Back-pressured is waiting on the task after it. Idle with "
   "no back-pressure is starved of input.",
   [("busiest task", "max(flink_taskmanager_job_task_busyTimeMsPerSecond) / 1000"),
    ("most back-pressured task", "max(flink_taskmanager_job_task_backPressuredTimeMsPerSecond) / 1000"),
    ("most idle task", "max(flink_taskmanager_job_task_idleTimeMsPerSecond) / 1000")],
   0, 12, unit="percentunit", maxv=1)
ts("Garbage collection, % of cores — under 5.5%",
   "Time spent collecting garbage, as a share of the cores the pipeline has. The harness calls a case "
   "over 5.5% a ceiling; the red line is that limit.",
   [("garbage collection", 'sum({__name__=~"flink_taskmanager_Status_JVM_GarbageCollector_G1_.*_TimeMsPerSecond"}) / 1000'
     ' / scalar(max(flink_jobmanager_taskSlotsTotal))')],
   12, 12, unit="percentunit", thresh=0.055)
y += 8
ts("Checkpoint duration — flat",
   "How long the last checkpoint took. Rising against a steady rate means the state is getting harder to save.",
   [("last checkpoint", "max(clamp_min(flink_jobmanager_job_lastCheckpointDuration, 0))")], 0, 12, unit="ms")
ts("Checkpoint size — levels off",
   "The size of the last checkpoint. On a pipeline left running it should level off; a line that keeps "
   "climbing is keys that never expire.",
   [("last checkpoint", "max(flink_jobmanager_job_lastCheckpointSize)")], 12, 12, unit="bytes")
y += 8
stat("Free task slots", "Slots no job is using. During a measured case this is 0 on purpose: the harness "
     "gives the job exactly the slots it uses. A pipeline left running needs spares to restart or rescale.", "max(flink_jobmanager_taskSlotsAvailable)", 0, 12)
stat("Worker threads — not climbing", "Threads in the worker. A slow climb over hours is a leak.",
     "max(flink_taskmanager_Status_JVM_Threads_Count)", 12, 12)
y += 4

dash = {"title": "Reference pipeline — hourly average by location",
        "uid": "refpipe-rows", "timezone": "America/New_York", "schemaVersion": 39,
        "time": {"from": "now-90m", "to": "now"}, "refresh": "10s",
        "tags": ["scalable-flink-skill"], "panels": panels, "editable": True}
json.dump(dash, open(sys.argv[1], "w"), indent=1)
print(f"{sum(1 for p in panels if p['type'] != 'row')} panels in {sum(1 for p in panels if p['type'] == 'row')} rows")
