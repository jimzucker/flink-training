#!/bin/bash
# Side sampler for a tiny proof. Stops when the STOP file appears.
#  - one long-lived container (name "memsampler", not the rig prefix) prints the Docker VM's
#    /proc/meminfo and /proc/vmstat every 2 s -> vm.log
#  - every 5 s: host swap, docker stats, and the running job's checkpoint counts -> sampler.log
S=$(cd "$(dirname "$0")" && pwd)
OUT=${OUT_DIR:-$S}/sampler.log
STOP=${OUT_DIR:-$S}/sampler.stop
rm -f $STOP
docker rm -f memsampler 2>&1
docker run -d --name memsampler --entrypoint bash flink:1.20.1-scala_2.12-java17 -c \
  'while true; do echo "ts=$(date +%s) $(grep -E "^(MemTotal|MemFree|MemAvailable|Cached|SwapFree):" /proc/meminfo | tr -s " " | tr "\n" " ") $(grep -E "^(workingset_refault_file|pswpin|pswpout|pgmajfault) " /proc/vmstat | tr "\n" " ")"; sleep 2; done'
echo "start $(date -u +%FT%TZ)" > $OUT
while [ ! -f $STOP ]; do
  ts=$(date +%s)
  swap=$(sysctl -n vm.swapusage)
  stats=$(docker stats --no-stream --format '{{.Name}} cpu={{.CPUPerc}} mem={{.MemUsage}}' 2>&1 | tr "\n" ";")
  cp=$(curl -s --max-time 2 localhost:18081/jobs/overview | python3 -c '
import sys, json, urllib.request
try:
    j = [x for x in json.load(sys.stdin)["jobs"] if x["state"] == "RUNNING"]
    if not j:
        print("job=none"); sys.exit()
    jid = j[0]["jid"]
    d = json.load(urllib.request.urlopen("http://localhost:18081/jobs/%s/checkpoints" % jid, timeout=2))
    c = d["counts"]; l = (d.get("latest") or {}).get("completed") or {}
    print("job=%s cp_completed=%s cp_failed=%s cp_inprog=%s last_id=%s last_e2e_ms=%s last_state_bytes=%s last_processed_bytes=%s" % (
        jid[:8], c["completed"], c["failed"], c["in_progress"], l.get("id"), l.get("end_to_end_duration"),
        l.get("state_size"), l.get("processed_data")))
except SystemExit:
    pass
except Exception as e:
    print("cp_err=%r" % e)' 2>&1)
  echo "ts=$ts $cp | host swap: $swap | $stats" >> $OUT
  sleep 5
done
docker logs memsampler > ${OUT_DIR:-$S}/vm.log 2>&1
docker rm -f memsampler 2>&1
echo "DONE $(date -u +%FT%TZ)" >> $OUT
