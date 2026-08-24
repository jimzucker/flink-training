#!/usr/bin/env bash
# Measures the pipeline's own throughput at each parallelism, by draining a backlog.
#
# Feeding it live cannot answer the question on one machine. The generator and the
# job share eight cores, so pushing hard enough to saturate the job starves it:
# offering 315k orders/sec left the busiest task at 100% and drained only 37k/sec,
# and parallelism 2 beat parallelism 4 because more Flink threads on a full box
# only added contention. That measures the laptop, not the pipeline.
#
# Filling the topics first and then starting the job removes the producer from the
# measurement entirely. What is left is how fast Flink drains a known backlog with
# the cores to itself, which is the number parallelism should move.
#
#   BACKLOG=5000000 scripts/scale-catchup.sh
set -uo pipefail

cd "$(dirname "$0")/.." || exit 1

COMPOSE="docker/compose.yml"
JAR=generators/target/generators.jar
BACKLOG="${BACKLOG:-5000000}"
TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-600}"   # abort rather than hang if a case cannot drain
CASES="${CASES:-1 2 4}"
OUT="${OUT:-docs/steps/step-10/catchup.txt}"

INPUTS="orders prices"
OUTPUTS="positions-by-symbol positions-by-account mv-by-symbol mv-by-account"

mkdir -p "$(dirname "$OUT")"
say() { echo "$@" | tee -a "$OUT"; }

kafka() { local bin="$1"; shift; docker exec ft-kafka "/opt/kafka/bin/$bin" "$@"; }

records() {
  kafka kafka-get-offsets.sh --bootstrap-server "${KAFKA_INTERNAL:-kafka:19092}" --topic "$1" 2>/dev/null \
    | awk -F: '{s += $3} END {print s + 0}'
}

promq() {
  curl -sfG 'http://localhost:9090/api/v1/query' --data-urlencode "query=$1" \
    | jq -r '.data.result[0].value[1] // "0"' | cut -d. -f1
}

cancel_all() {
  for id in $(curl -sf http://localhost:8081/jobs | jq -r '.jobs[]|select(.status=="RUNNING")|.id'); do
    docker compose -f "$COMPOSE" exec -T jobmanager flink cancel "$id" >/dev/null 2>&1 || true
  done
}

# Aborts if a topic will not go away. Proceeding regardless left the previous
# run's records in place, and the completion check then passed instantly against
# stale data -- a measurement that looked like an answer.
delete_topics() {
  for t in $1; do
    kafka kafka-topics.sh --bootstrap-server "${KAFKA_INTERNAL:-kafka:19092}" --delete --topic "$t" >/dev/null 2>&1 || true
  done
  for t in $1; do
    local gone=0
    for _ in $(seq 1 60); do
      if ! kafka kafka-topics.sh --bootstrap-server "${KAFKA_INTERNAL:-kafka:19092}" --list 2>/dev/null | grep -qx "$t"; then
        gone=1; break
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
  delete_topics "$OUTPUTS"          # inputs are kept: every case drains the same backlog

  local queued
  queued=$(records orders)

  PARALLELISM="$par" docker compose -f "$COMPOSE" run --rm submit >/dev/null 2>&1
  local running
  running=$(curl -sf http://localhost:8081/jobs | jq -r '[.jobs[]|select(.status=="RUNNING")]|length')
  if [ "$running" != "2" ]; then
    echo "expected 2 running jobs at parallelism $par, got $running; refusing to report a rate" >&2
    exit 4
  fi

  # Time the drain to completion rather than sampling a window. A window has to be
  # sized against a rate that is not known yet -- at 8M orders every parallelism
  # emptied the backlog before the window closed, and all three reported nothing.
  # Draining a fixed backlog needs no such guess, and the ramp is the same in
  # every case.
  local t0 done_at busy_peak bp_peak out
  t0=$(date +%s); busy_peak=0; bp_peak=0; done_at=""
  while [ $(( $(date +%s) - t0 )) -lt "$TIMEOUT_SECONDS" ]; do
    out=$(promq 'sum(flink_taskmanager_job_task_operator_numRecordsOut{operator_name="by_symbol"})')
    local b p
    b=$(promq 'max(flink_taskmanager_job_task_busyTimeMsPerSecond)')
    p=$(promq 'max(flink_taskmanager_job_task_backPressuredTimeMsPerSecond)')
    [ "${b:-0}" -gt "$busy_peak" ] && busy_peak=$b
    [ "${p:-0}" -gt "$bp_peak" ] && bp_peak=$p
    if [ "${out:-0}" -ge "$queued" ]; then done_at=$(( $(date +%s) - t0 )); break; fi
    sleep 2
  done

  if [ -z "$done_at" ]; then
    say "  parallelism $par: did not drain ${queued} orders within ${TIMEOUT_SECONDS}s"
    return
  fi

  local ckpt
  ckpt=$(promq 'max(flink_jobmanager_job_lastCheckpointDuration)')
  printf "  parallelism %s: %4ss to drain %s orders = %8s orders/sec   peak busy %s%%   peak back-pressure %s%%   checkpoint %sms\n" \
    "$par" "$done_at" "$queued" "$(( queued / done_at ))" \
    "$(( busy_peak / 10 ))" "$(( bp_peak / 10 ))" "$ckpt" | tee -a "$OUT"
}

[ -f "$JAR" ] || { echo "build first:  mvn package -DskipTests" >&2; exit 2; }

: > "$OUT"
say "catch-up throughput  ($(date -u +%Y-%m-%dT%H:%M:%SZ))"

cancel_all
delete_topics "$INPUTS $OUTPUTS"

say "filling topics with the job stopped ..."
TRADES_PER_SECOND=1000000 PRICES_PER_SECOND=2000 \
  MAX_TRADES="$BACKLOG" MAX_PRICES=$(( BACKLOG / 100 )) \
  java -jar "$JAR" >/dev/null 2>&1
queued=$(records orders)
say "backlog of ${queued} orders, drained with the producer stopped"
say "each case is timed from job start to the last backlog record processed"
say ""

for par in $CASES; do
  drain_at "$par"
done
say ""
say "written to $OUT"
