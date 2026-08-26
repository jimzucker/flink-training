#!/usr/bin/env bash
# Runs the assignment's two scale cases, and the parallelism comparison.
#
#   case 1  orders to 1000/sec -- throughput must hold; latency may rise
#   case 2  price rate very high -- order latency must NOT move
#
# Case 2 is the interesting one. Prices are broadcast to every subtask, because
# the account side is keyed on account/sub-account/symbol and cannot be joined to
# a symbol-keyed stream by key alone. That puts every price through the same
# threads doing order work, which is exactly the coupling this case exists to
# expose. Whether it actually bites is measured here rather than guessed at.
set -uo pipefail

cd "$(dirname "$0")/.." || exit 1

COMPOSE="docker/compose.yml"
JAR=generators/target/generators.jar
RUN_SECONDS="${RUN_SECONDS:-60}"
MEASURE_SECONDS="${MEASURE_SECONDS:-30}"
OUT="${OUT:-docs/steps/step-10/results.txt}"

mkdir -p "$(dirname "$OUT")"
: > "$OUT"

say() { echo "$@" | tee -a "$OUT"; }

prom() {  # query -> single value
  curl -sfG 'http://localhost:9090/api/v1/query' --data-urlencode "query=$1" \
    | jq -r '.data.result[0].value[1] // "0"'
}

reset_and_submit() {  # parallelism
  for t in orders prices positions-by-symbol positions-by-account mv-by-symbol mv-by-account; do
    docker exec -e KAFKA_OPTS= ft-kafka /opt/kafka/bin/kafka-topics.sh \
        --bootstrap-server "${KAFKA_INTERNAL:-kafka:19092}" --delete --topic "$t" >/dev/null 2>&1 || true
  done
  for t in orders prices positions-by-symbol positions-by-account mv-by-symbol mv-by-account; do
    for _ in $(seq 1 60); do
      docker exec -e KAFKA_OPTS= ft-kafka /opt/kafka/bin/kafka-topics.sh \
          --bootstrap-server "${KAFKA_INTERNAL:-kafka:19092}" --list 2>/dev/null | grep -qx "$t" || break
      sleep 0.5
    done
  done
  docker compose -f "$COMPOSE" run --rm topics >/dev/null 2>&1

  for id in $(docker compose -f "$COMPOSE" exec -T jobmanager flink list -r 2>/dev/null \
              | grep -oE '[0-9a-f]{32}' || true); do
    docker compose -f "$COMPOSE" exec -T jobmanager flink cancel "$id" >/dev/null 2>&1 || true
  done
  PARALLELISM="$1" docker compose -f "$COMPOSE" run --rm submit >/dev/null 2>&1

  for _ in $(seq 1 60); do
    n=$(curl -sf http://localhost:8081/jobs 2>/dev/null \
        | jq -r '[.jobs[] | select(.status=="RUNNING")] | length' 2>/dev/null || echo 0)
    [ "${n:-0}" -ge 2 ] && break
    sleep 2
  done
}

run_case() {  # name parallelism trades/sec prices/sec
  local name="$1" par="$2" tps="$3" pps="$4"

  say ""
  say "=============================================================="
  say "$name   parallelism=$par  orders=${tps}/s  prices=${pps}/s"
  say "=============================================================="

  reset_and_submit "$par"

  TRADES_PER_SECOND="$tps" PRICES_PER_SECOND="$pps" \
    java -jar "$JAR" >/dev/null 2>&1 &
  local gen=$!
  # Let it reach steady state before measuring; rates ramp for a few seconds.
  sleep $(( RUN_SECONDS > MEASURE_SECONDS ? RUN_SECONDS - MEASURE_SECONDS : 20 ))

  local order_rate alloc_rate ckpt
  order_rate=$(prom 'sum(rate(flink_taskmanager_job_task_operator_numRecordsOut{operator_name="by_symbol"}[30s]))')
  alloc_rate=$(prom 'sum(rate(flink_taskmanager_job_task_operator_numRecordsOut{operator_name="split_by_allocation"}[30s]))')

  local lat
  lat=$(java -cp "$JAR" io.github.jimzucker.flinktraining.tools.MeasureLatency \
        "${BOOTSTRAP:-localhost:9092}" positions-by-symbol "$MEASURE_SECONDS" eventTime 2>/dev/null \
        | sed 's/positions-by-symbol *//')

  ckpt=$(prom 'max(flink_jobmanager_job_lastCheckpointDuration)')

  kill "$gen" 2>/dev/null || true
  wait "$gen" 2>/dev/null || true

  say ""
  say "  orders through the pipeline   $(printf '%.0f' "$order_rate")/sec"
  say "  allocations                   $(printf '%.0f' "$alloc_rate")/sec"
  say "  order latency                 ${lat}"
  say "  checkpoint duration           $(printf '%.0f' "$ckpt") ms"
}

if [ ! -f "$JAR" ]; then
  echo "build first:  mvn package -DskipTests" >&2
  exit 2
fi

say "scale test  ($(date -u +%Y-%m-%dT%H:%M:%SZ))"
say "each case runs for ${RUN_SECONDS}s with latency measured over ${MEASURE_SECONDS}s"

docker compose -f "$COMPOSE" up -d --wait \
  kafka jobmanager taskmanager prometheus grafana renderer >/dev/null 2>&1
for _ in $(seq 1 40); do
  s=$(curl -sf http://localhost:8081/overview 2>/dev/null | jq -r '."slots-total"' || echo 0)
  [ "${s:-0}" -ge 8 ] && break
  sleep 2
done

run_case "baseline"                     2 10   1000
run_case "case 1: orders to 1000/sec"   2 1000 1000
run_case "case 1 at parallelism 4"      4 1000 1000
run_case "case 2: price rate very high" 2 10   20000

say ""
say "=============================================================="
say "written to $OUT"
