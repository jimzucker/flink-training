#!/usr/bin/env bash
# Offer a fixed load at a given parallelism and record whether the pipeline keeps up.
#
#   PARALLELISM=4 TRADES_PER_SECOND=100000 RUN_SECONDS=90 scripts/scale-saturate.sh
#
# The verdict is the lag trend, not the throughput number: if the gap between
# orders produced and orders processed grows steadily, the pipeline is saturated.
set -euo pipefail
cd "$(dirname "$0")/.."

PARALLELISM="${PARALLELISM:-4}"
TRADES_PER_SECOND="${TRADES_PER_SECOND:-100000}"
PRICES_PER_SECOND="${PRICES_PER_SECOND:-1000}"
RUN_SECONDS="${RUN_SECONDS:-90}"
GENERATORS="${GENERATORS:-1}"      # one JVM tops out near 120k/s; use more to saturate
SETTLE_SECONDS="${SETTLE_SECONDS:-15}"   # ignore the ramp before measuring
COMPOSE="docker/compose.yml"
TOPICS=(orders prices positions-by-symbol positions-by-account mv-by-symbol mv-by-account)

kafka() { docker exec ft-kafka "/opt/kafka/bin/$@"; }
promq() {
  curl -sfG 'http://localhost:9090/api/v1/query' --data-urlencode "query=$1" \
    | jq -r '.data.result[0].value[1] // "0"' | cut -d. -f1
}
produced() { kafka kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic orders 2>/dev/null | awk -F: '{s+=$3} END {print s+0}'; }
processed() { promq 'sum(flink_taskmanager_job_task_operator_numRecordsOut{operator_name="by_symbol"})'; }

echo "=== parallelism=$PARALLELISM  offered=${TRADES_PER_SECOND}/s x${GENERATORS}  run=${RUN_SECONDS}s ==="

echo "-- resetting topics"
for t in "${TOPICS[@]}"; do kafka kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic "$t" >/dev/null 2>&1 || true; done
for t in "${TOPICS[@]}"; do
  until ! kafka kafka-topics.sh --bootstrap-server localhost:9092 --list 2>/dev/null | grep -qx "$t"; do sleep 1; done
done
docker compose -f "$COMPOSE" run --rm topics >/dev/null 2>&1

echo "-- restarting job at parallelism $PARALLELISM"
for id in $(curl -sf http://localhost:8081/jobs | jq -r '.jobs[]|select(.status=="RUNNING")|.id'); do
  docker compose -f "$COMPOSE" exec -T jobmanager flink cancel "$id" >/dev/null 2>&1 || true
done
PARALLELISM="$PARALLELISM" docker compose -f "$COMPOSE" run --rm submit >/dev/null 2>&1
running=$(curl -sf http://localhost:8081/jobs | jq -r '[.jobs[]|select(.status=="RUNNING")]|length')
[ "$running" = "2" ] || { echo "ABORT: expected 2 running jobs, got $running"; exit 1; }

echo "-- generating ($GENERATORS generator JVM(s))"
PIDS=()
for i in $(seq 1 "$GENERATORS"); do
  TRADES_PER_SECOND="$TRADES_PER_SECOND" PRICES_PER_SECOND="$PRICES_PER_SECOND" \
    MAX_TRADES=100000000 MAX_PRICES=10000000 \
    java -jar generators/target/generators.jar >/dev/null 2>&1 &
  PIDS+=($!)
done
trap 'kill ${PIDS[@]} 2>/dev/null || true' EXIT

sleep "$SETTLE_SECONDS"
p0=$(produced); c0=$(processed); t0=$(date +%s)
printf "\n  %6s %12s %12s %10s %8s %8s %8s %6s\n" t produced lag offered/s used/s busy bp ckpt
lag_first=""; lag_last=""
while [ $(( $(date +%s) - t0 )) -lt "$RUN_SECONDS" ]; do
  sleep 10
  now=$(date +%s); el=$(( now - t0 ))
  p=$(produced); c=$(processed); lag=$(( p - c ))
  busy=$(promq 'max(flink_taskmanager_job_task_busyTimeMsPerSecond)')
  bp=$(promq 'max(flink_taskmanager_job_task_backPressuredTimeMsPerSecond)')
  ckpt=$(promq 'max(flink_jobmanager_job_lastCheckpointDuration)')
  printf "  %5ss %12s %12s %10s %8s %7s%% %7s%% %5sms\n" \
    "$el" "$p" "$lag" "$(( (p - p0) / el ))" "$(( (c - c0) / el ))" \
    "$(( busy / 10 ))" "$(( bp / 10 ))" "$ckpt"
  [ -z "$lag_first" ] && lag_first="$lag"
  lag_last="$lag"
done

el=$(( $(date +%s) - t0 ))
p=$(produced); c=$(processed)
offered=$(( (p - p0) / el )); used=$(( (c - c0) / el ))
lat50=$(promq 'flink_taskmanager_job_task_operator_processingLatencyMillis{operator_name="aggregate_by_symbol",quantile="0.5"}')
lat95=$(promq 'flink_taskmanager_job_task_operator_processingLatencyMillis{operator_name="aggregate_by_symbol",quantile="0.95"}')
lat99=$(promq 'flink_taskmanager_job_task_operator_processingLatencyMillis{operator_name="aggregate_by_symbol",quantile="0.99"}')
lag_growth=$(( (lag_last - lag_first) / (el > 10 ? el - 10 : 1) ))

echo
echo "  offered          ${offered}/s"
echo "  sustained        ${used}/s"
echo "  latency          p50=${lat50}ms p95=${lat95}ms p99=${lat99}ms"
echo "  lag drift        ${lag_growth}/s  (${lag_first} -> ${lag_last})"
if [ "$lag_growth" -gt 2000 ]; then
  echo "  VERDICT          SATURATED - lag diverging, sustained rate is the ceiling"
else
  echo "  VERDICT          KEEPS UP - lag bounded, offered load is below the ceiling"
fi
