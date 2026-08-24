#!/usr/bin/env bash
# Measures the pipeline's own throughput, by draining a backlog.
#
# Feeding it live does not measure it. The generator tops out around 3000
# orders/sec and its pacer under-delivers below that, so at the rates a live run
# reaches the pipeline is never saturated -- which is why raising parallelism
# changed nothing: there was no queue to work through.
#
# Filling the topic first and then starting the job removes the producer from the
# measurement entirely. What is left is how fast Flink drains a known backlog,
# which is the number parallelism should move.
set -uo pipefail

cd "$(dirname "$0")/.." || exit 1

COMPOSE="docker/compose.yml"
JAR=generators/target/generators.jar
BACKLOG="${BACKLOG:-120000}"
SAMPLES="${SAMPLES:-25}"   # x2s of sampling per case
OUT="${OUT:-docs/steps/step-10/catchup.txt}"

mkdir -p "$(dirname "$OUT")"
say() { echo "$@" | tee -a "$OUT"; }

records() {
  docker exec ft-kafka /opt/kafka/bin/kafka-get-offsets.sh \
      --bootstrap-server localhost:9092 --topic "$1" 2>/dev/null \
    | awk -F: '{s += $3} END {print s + 0}'
}

cancel_all() {
  for id in $(docker compose -f "$COMPOSE" exec -T jobmanager flink list -r 2>/dev/null \
              | grep -oE '[0-9a-f]{32}' || true); do
    docker compose -f "$COMPOSE" exec -T jobmanager flink cancel "$id" >/dev/null 2>&1 || true
  done
}

# Aborts if a topic will not go away. Proceeding regardless left the previous
# run's records in place, and the completion check then passed instantly against
# stale data -- a measurement that looked like an answer.
reset_topics() {
  local topics="orders prices positions-by-symbol positions-by-account mv-by-symbol mv-by-account"
  for t in $topics; do
    docker exec ft-kafka /opt/kafka/bin/kafka-topics.sh \
        --bootstrap-server localhost:9092 --delete --topic "$t" >/dev/null 2>&1 || true
  done
  for t in $topics; do
    local gone=0
    for _ in $(seq 1 60); do
      if ! docker exec ft-kafka /opt/kafka/bin/kafka-topics.sh \
             --bootstrap-server localhost:9092 --list 2>/dev/null | grep -qx "$t"; then
        gone=1
        break
      fi
      sleep 0.5
    done
    if [ "$gone" -ne 1 ]; then
      echo "topic $t would not delete; refusing to measure against stale data" >&2
      exit 3
    fi
  done
  docker compose -f "$COMPOSE" run --rm topics >/dev/null 2>&1
}

drain_at() {  # parallelism
  local par="$1"

  cancel_all
  reset_topics

  # Fill the topic with the job stopped, so the producer is not in the measurement.
  TRADES_PER_SECOND=5000 PRICES_PER_SECOND=2000 \
    MAX_TRADES="$BACKLOG" MAX_PRICES=$(( BACKLOG / 4 )) \
    java -jar "$JAR" >/dev/null 2>&1
  local queued
  queued=$(records orders)

  PARALLELISM="$par" docker compose -f "$COMPOSE" run --rm submit >/dev/null 2>&1

  # Sample the rate Flink actually achieves while it works through the backlog,
  # rather than timing to completion. Detecting completion needs the topic counts
  # to be trustworthy; a rate does not, and a peak sustained rate is the number
  # parallelism is supposed to move.
  # Sample for a fixed window and take the maximum. An early exit on a falling
  # rate looked sensible and was not: the rate dips while the job ramps up, so it
  # stopped before the peak and reported parallelism 2 as slower than 1.
  local peak=0 now
  for _ in $(seq 1 "$SAMPLES"); do
    sleep 2
    now=$(curl -sfG 'http://localhost:9090/api/v1/query' \
          --data-urlencode 'query=sum(rate(flink_taskmanager_job_task_operator_numRecordsOut{operator_name="by_symbol"}[10s]))' \
          | jq -r '.data.result[0].value[1] // "0"')
    now=${now%%.*}
    [ "${now:-0}" -gt "$peak" ] && peak=$now
  done

  say "  parallelism $par: backlog ${queued} orders, peak ${peak} orders/sec through the pipeline"
}

if [ ! -f "$JAR" ]; then
  echo "build first:  mvn package -DskipTests" >&2
  exit 2
fi

: > "$OUT"
say "catch-up throughput  ($(date -u +%Y-%m-%dT%H:%M:%SZ))"
say "backlog of ${BACKLOG} orders, drained with the producer stopped"
say ""
for par in 1 2 4; do
  drain_at "$par"
done
say ""
say "written to $OUT"
