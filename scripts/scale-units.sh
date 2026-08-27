#!/usr/bin/env bash
# Shows what parallelism buys, and where it stops buying.
#
# A unit is one core and one degree of parallelism, bought together -- which is
# what a KPU is in Managed Service for Apache Flink. Raising parallelism without
# cores only spreads the same work over more threads, which is why step 10 could
# never show scaling: on one machine Flink already had every core it could use
# and the broker was the ceiling.
#
# Shrinking Flink is far cheaper than overwhelming Kafka. Step 11 needed 88
# million records, 32 vCPUs and three brokers to make Flink the constraint; this
# does it on a laptop by making Flink small instead.
#
#   scripts/scale-units.sh                 # 1, 2, 4, 8 units
#   UNITS="1 2 4" BACKLOG=25000000 scripts/scale-units.sh
#
# On AWS, point it at the remote stack:
#   COMPOSE=docker/compose.aws.yml BOOTSTRAP_SERVERS=b-1...:9092 scripts/scale-units.sh
set -uo pipefail
cd "$(dirname "$0")/.." || exit 1

COMPOSE="${COMPOSE:-docker/compose.yml}"
UNITS="${UNITS:-1 2 4 8}"
BACKLOG="${BACKLOG:-50000000}"
PARTITIONS="${PARTITIONS:-8}"
WARMUP_SECONDS="${WARMUP_SECONDS:-180}"
WINDOW_SECONDS="${WINDOW_SECONDS:-60}"
CHECKPOINT_INTERVAL_MS="${CHECKPOINT_INTERVAL_MS:-5000}"
OUT="${OUT:-docs/steps/step-12/units.txt}"
SINKS="positions-by-symbol positions-by-account mv-by-symbol mv-by-account"

mkdir -p "$(dirname "$OUT")"
say() { echo "$@" | tee -a "$OUT"; }

# Locally the broker is a container to exec into; on MSK there is nothing to exec
# into, so the CLI comes from a throwaway container instead.
if docker ps --format '{{.Names}}' 2>/dev/null | grep -qx ft-kafka; then
  BOOT="${BOOTSTRAP_SERVERS:-kafka:19092}"
  kcli() { local b="$1"; shift; docker exec -e KAFKA_OPTS= ft-kafka "/opt/kafka/bin/$b" "$@"; }
else
  : "${BOOTSTRAP_SERVERS:?set BOOTSTRAP_SERVERS when there is no local broker}"
  BOOT="$BOOTSTRAP_SERVERS"
  kcli() { local b="$1"; shift; docker run --rm --network host apache/kafka:3.9.2 "/opt/kafka/bin/$b" "$@"; }
fi

count() {
  kcli kafka-get-offsets.sh --bootstrap-server "$BOOT" --topic "$1" 2>/dev/null \
    | awk -F: '{s += $3} END {print s + 0}'
}
cancel_all() {
  for id in $(curl -sf http://localhost:8081/jobs | grep -oE '[0-9a-f]{32}'); do
    docker compose -f "$COMPOSE" exec -T jobmanager flink cancel "$id" >/dev/null 2>&1
  done
}
recreate() {
  for t in $1; do kcli kafka-topics.sh --bootstrap-server "$BOOT" --delete --topic "$t" >/dev/null 2>&1; done
  for t in $1; do
    for _ in $(seq 1 120); do
      kcli kafka-topics.sh --bootstrap-server "$BOOT" --list 2>/dev/null | grep -qx "$t" || break
      sleep 1
    done
  done
  PARTITIONS="$PARTITIONS" docker compose -f "$COMPOSE" run --rm topics >/dev/null 2>&1 || \
    for t in $1; do
      kcli kafka-topics.sh --bootstrap-server "$BOOT" --create --if-not-exists \
        --topic "$t" --partitions "$PARTITIONS" --replication-factor 1 >/dev/null 2>&1
    done
}

: > "$OUT"
say "what a unit of parallelism buys  ($(date -u +%Y-%m-%dT%H:%M:%SZ))"
say "a unit is one core and one degree of parallelism, bought together"
say "$PARTITIONS partitions, checkpoint ${CHECKPOINT_INTERVAL_MS}ms, ${WINDOW_SECONDS}s window after ${WARMUP_SECONDS}s warm-up"
say ""

cancel_all
recreate "orders prices $SINKS"
say "-- filling $BACKLOG orders with the job stopped"
BOOTSTRAP_SERVERS="$BOOT" GENERATOR_THREADS=4 COMPRESSION_TYPE=lz4 BATCH_SIZE=262144 \
  TRADES_PER_SECOND=4000000 PRICES_PER_SECOND=200000 \
  MAX_TRADES="$BACKLOG" MAX_PRICES=$(( BACKLOG / 100 )) \
  java -jar generators/target/generators.jar >/dev/null 2>&1
queued=$(count orders)
say "   $queued orders queued"
say ""
printf "  %6s %14s %10s\n" units "orders/sec" "vs prev" | tee -a "$OUT"

prev=0
for n in $UNITS; do
  cancel_all
  recreate "$SINKS"

  export TASKMANAGER_CPUS="$n" PARALLELISM="$n" CHECKPOINT_INTERVAL_MS
  # Two jobs run, each wanting its own slots.
  export TASK_SLOTS=$(( n * 2 ))
  docker compose -f "$COMPOSE" up -d --force-recreate --wait taskmanager >/dev/null 2>&1

  # Refuse to report a number for a limit that was not applied. Setting
  # TASKMANAGER_CPUS as a prefix on one command is not enough: any later compose
  # call that cannot see it computes a different desired config and silently
  # recreates the container without the limit.
  applied=$(docker inspect ft-taskmanager --format '{{.HostConfig.NanoCpus}}' 2>/dev/null)
  want=$(( n * 1000000000 ))
  if [ "${applied:-0}" != "$want" ] && [ "$COMPOSE" = "docker/compose.yml" ]; then
    say "  ${n}: NanoCpus=${applied}, expected ${want} -- refusing to report"
    continue
  fi

  docker compose -f "$COMPOSE" run --rm submit >/dev/null 2>&1
  running=$(curl -sf http://localhost:8081/jobs | grep -c '"status":"RUNNING"')
  [ "$running" -ge 1 ] || { say "  ${n}: no running job -- refusing to report"; continue; }

  sleep "$WARMUP_SECONDS"
  a=$(count positions-by-symbol)
  sleep "$WINDOW_SECONDS"
  b=$(count positions-by-symbol)

  # A window that runs past the end of the backlog measures the silence after it.
  if [ "$b" -ge "$queued" ]; then
    say "  ${n}: backlog exhausted inside the window -- raise BACKLOG"
    continue
  fi

  rate=$(( (b - a) / WINDOW_SECONDS ))
  if [ "$prev" -gt 0 ]; then
    ratio=$(awk -v r="$rate" -v p="$prev" 'BEGIN { printf "%.2fx", r/p }')
  else
    ratio="-"
  fi
  printf "  %6s %14s %10s\n" "$n" "$rate" "$ratio" | tee -a "$OUT"
  prev=$rate
done

say ""
say "written to $OUT"
