"""Per-container CPU and memory for the dashboard's "CPU per component" panel.

Section 7 asks for a panel that shows which component is in the way, *including
the idle one* -- section 5's broker at 0.48 cores that is still the ceiling. The
engine's metrics reporter reaches the job manager and the workers and nothing
else, so the broker half of that panel had no source at all.

cAdvisor, the obvious answer, does not work on Docker Desktop for macOS: its
Docker factory registers against the socket and it still reports one series,
`container_cpu_usage_seconds_total{id="/"}`, because the cgroup tree it reads
inside the VM does not contain the containers. Clean-room run 36 tried it twice
and then wrote this, and noted that every run would have to write it again.

This reads the same numbers from the Docker API: cumulative CPU nanoseconds per
container, which is exactly what a Prometheus counter wants. It runs as a
compose service with the project's name prefix and a CPU cap, because section 6
says anything watching the stack runs as part of the stack.

**Nothing here is ever quoted as a result.** The harness reads the cgroup
counter directly for the table; this is for looking at.

Copy it next to your Grafana and Prometheus provisioning, mount that directory
(not this file -- a single-file bind mount breaks the moment the host file is
rewritten by a tool that replaces the inode), and add the service in
`harness/README.md` to `extraServices`.

Environment: PREFIX is the project's container-name prefix, and nothing outside
it is exported. PORT defaults to 9110.
"""
import http.client
import json
import os
import socket
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

SOCK = "/var/run/docker.sock"
PREFIX = os.environ.get("PREFIX", "")
PORT = int(os.environ.get("PORT", "9110"))
INTERVAL = float(os.environ.get("INTERVAL_S", "5"))
SAMPLE = {}


class UnixHTTPConnection(http.client.HTTPConnection):
    """http.client over the Docker socket."""

    def __init__(self):
        super().__init__("localhost")

    def connect(self):
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        s.settimeout(10)
        s.connect(SOCK)
        self.sock = s


def api(path):
    conn = UnixHTTPConnection()
    try:
        conn.request("GET", path)
        return json.loads(conn.getresponse().read())
    finally:
        conn.close()


def poll():
    while True:
        try:
            out = {}
            for container in api("/v1.43/containers/json"):
                name = container["Names"][0].lstrip("/")
                if not name.startswith(PREFIX):
                    continue
                stats = api("/v1.43/containers/%s/stats?stream=false&one-shot=true" % container["Id"])
                cpu = stats.get("cpu_stats", {}).get("cpu_usage", {}).get("total_usage")
                mem = stats.get("memory_stats", {}).get("usage")
                limit = stats.get("memory_stats", {}).get("limit")
                if cpu is not None:
                    out[name] = (cpu, mem or 0, limit or 0)
            SAMPLE.clear()
            SAMPLE.update(out)
        except Exception as e:                       # never silence it
            print("poll failed:", repr(e), flush=True)
        time.sleep(INTERVAL)


class Metrics(BaseHTTPRequestHandler):

    def do_GET(self):
        lines = [
            "# HELP docker_container_cpu_seconds_total Cumulative CPU seconds per container",
            "# TYPE docker_container_cpu_seconds_total counter",
            "# HELP docker_container_memory_bytes Memory in use per container",
            "# TYPE docker_container_memory_bytes gauge",
            "# HELP docker_container_memory_limit_bytes Memory the container is allowed",
            "# TYPE docker_container_memory_limit_bytes gauge",
        ]
        for name, (cpu, mem, limit) in sorted(SAMPLE.items()):
            lines.append('docker_container_cpu_seconds_total{name="%s"} %.6f' % (name, cpu / 1e9))
            lines.append('docker_container_memory_bytes{name="%s"} %d' % (name, mem))
            lines.append('docker_container_memory_limit_bytes{name="%s"} %d' % (name, limit))
        body = ("\n".join(lines) + "\n").encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; version=0.0.4")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


if __name__ == "__main__":
    if not PREFIX:
        raise SystemExit("PREFIX is not set: without it this would export every container on the "
                         "machine, including other people's")
    threading.Thread(target=poll, daemon=True).start()
    print(f"exporting containers named {PREFIX}* on :{PORT}", flush=True)
    ThreadingHTTPServer(("0.0.0.0", PORT), Metrics).serve_forever()
