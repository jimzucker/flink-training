#!/usr/bin/env bash
# Scales cores, partitions and parallelism together, which is the honest way to
# show what parallelism buys.
#
# Raising parallelism alone does nothing here, and step 10 established why: the
# ceiling is how fast one broker accepts writes, and Flink parallelism scales the
# work inside the job, not the broker outside it. Partitions are the broker-side
# half of the same dial -- a topic with one partition is one write stream no
# matter how many subtasks feed it -- so sizing them together is both the
# realistic deployment and the measurement that isolates the effect.
#
# Cores move with them for the same reason. Parallelism converts cores into
# throughput; it cannot conjure them. At the whole machine the TaskManager never
# ran out of cores, so raising parallelism only spread the same work over more
# threads. Budgeting 1, 2 and 4 cores alongside is exactly a KPU in Managed
# Service for Apache Flink -- one vCPU, bought together with the parallelism that
# uses it.
#
#   BACKLOG=4000000 scripts/scale-paired.sh
set -uo pipefail
cd "$(dirname "$0")/.." || exit 1

COMPOSE="docker/compose.yml"
JAR=generators/target/generators.jar
BACKLOG="${BACKLOG:-4000000}"
CASES="${CASES:-1 2 4}"
TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-900}"
OUT="${OUT:-docs/steps/step-10/paired.txt}"
TOPICS="orders prices positions-by-symbol positions-by-account mv-by-symbol mv-by-account"

mkdir -p "$(dirname "$OUT")"
say() { echo "$@" | tee -a "$OUT"; }
kafka() { local bin="$1"; shift; docker exec -e KAFKA_OPTS= ft-kafka "/opt/kafka/bin/$bin" "$@"; }
BOOT="${KAFKA_INTERNAL:-kafka:19092}"

records() {
  kafka kafka-get-offsets.sh --bootstrap-server "$BOOT" --topic "$1" 2>/dev/null \
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
recreate_topics() {  # partitions
  for t in $TOPICS; do
    kafka kafka-topics.sh --bootstrap-server "$BOOT" --delete --topic "$t" >/dev/null 2>&1 || true
  done
  for t in $TOPICS; do
    local gone=0
    for _ in $(seq 1 120); do
      if ! kafka kafka-topics.sh --bootstrap-server "$BOOT" --list 2>/dev/null | grep -qx "$t"; then
        gone=1; break
      fi
      sleep 0.5
    done
    [ "$gone" -eq 1 ] || { echo "topic $t would not delete; refusing to measure against stale data" >&2; exit 3; }
  done
  PARTITIONS="$1" docker compose -f "$COMPOSE" run --rm topics >/dev/null 2>&1
}

[ -f "$JAR" ] || { echo "build first:  mvn package -DskipTests" >&2; exit 2; }

: > "$OUT"
say "cores, partitions and parallelism raised together  ($(date -u +%Y-%m-%dT%H:%M:%SZ))"
say "backlog of ${BACKLOG} orders per case, drained with the producer stopped"
say ""

for n in $CASES; do
  cancel_all
  recreate_topics "$n"

  # Recreated rather than restarted: a CPU limit is fixed when the container is
  # created, so restarting keeps the old one.
  TASKMANAGER_CPUS="$n" docker compose -f "$COMPOSE" up -d --force-recreate --wait taskmanager >/dev/null 2>&1
  sleep 5

  # Filled fresh for each case, because the partition count differs.
  TRADES_PER_SECOND=1000000 PRICES_PER_SECOND=2000 \
    MAX_TRADES="$BACKLOG" MAX_PRICES=$(( BACKLOG / 100 )) \
    java -jar "$JAR" >/dev/null 2>&1
  queued=$(records orders)

  PARALLELISM="$n" docker compose -f "$COMPOSE" run --rm submit >/dev/null 2>&1
  running=$(curl -sf http://localhost:8081/jobs | jq -r '[.jobs[]|select(.status=="RUNNING")]|length')
  [ "$running" = "2" ] || { echo "expected 2 running jobs at parallelism $n, got $running" >&2; exit 4; }

  # The drain is detected by a counter reaching the backlog size, so a counter
  # left over from the previous case would end this one instantly. Cancelled jobs
  # stop being exported and Prometheus marks them stale within a scrape, but that
  # is a race worth closing rather than trusting: refuse to time a case whose
  # counter has not started from below the backlog.
  start_out=$(promq 'sum(flink_taskmanager_job_task_operator_numRecordsOut{operator_name="by_symbol"})')
  if [ "${start_out:-0}" -ge "$queued" ]; then
    echo "counter started at ${start_out} for a backlog of ${queued}: stale metrics, refusing to time this case" >&2
    exit 5
  fi

  t0=$(date +%s); done_at=""; bp_peak=0
  while [ $(( $(date +%s) - t0 )) -lt "$TIMEOUT_SECONDS" ]; do
    out=$(promq 'sum(flink_taskmanager_job_task_operator_numRecordsOut{operator_name="by_symbol"})')
    p=$(promq 'max(flink_taskmanager_job_task_backPressuredTimeMsPerSecond)')
    [ "${p:-0}" -gt "$bp_peak" ] && bp_peak=$p
    if [ "${out:-0}" -ge "$queued" ]; then done_at=$(( $(date +%s) - t0 )); break; fi
    sleep 2
  done

  if [ -z "$done_at" ]; then
    say "  ${n} partitions, parallelism ${n}: did not drain ${queued} within ${TIMEOUT_SECONDS}s"
    continue
  fi
  printf "  %2s cores, %2s partitions, parallelism %-2s: %4ss to drain %s = %8s orders/sec   peak back-pressure %s%%\n" \
    "$n" "$n" "$n" "$done_at" "$queued" "$(( queued / done_at ))" "$(( bp_peak / 10 ))" | tee -a "$OUT"
done

say ""
say "written to $OUT"
