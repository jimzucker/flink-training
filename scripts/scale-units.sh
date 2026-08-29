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
# Held constant across every case, and at least the largest unit count. Varying
# it with the units would change two things at once: a flat step could then be a
# broker at its limit or simply too few partitions to read from, and the run
# could not tell you which. Eight costs nothing at one unit, where a single
# subtask reads all eight.
PARTITIONS="${PARTITIONS:-8}"
WARMUP_SECONDS="${WARMUP_SECONDS:-180}"
WINDOW_SECONDS="${WINDOW_SECONDS:-60}"
CHECKPOINT_INTERVAL_MS="${CHECKPOINT_INTERVAL_MS:-5000}"
OUT="${OUT:-docs/steps/step-12/units.txt}"
SINKS="positions-by-symbol positions-by-account mv-by-symbol mv-by-account"
RUN_ID="${RUN_ID:-$(date +%s)}"

mkdir -p "$(dirname "$OUT")"
say() { echo "$@" | tee -a "$OUT"; }

# Locally the broker is a container to exec into; on MSK there is nothing to exec
# into, so the CLI comes from a throwaway container instead.
# Two addresses, not one. The CLI runs inside a container on the compose network
# and reaches the broker by service name; the generator runs on this host and
# cannot resolve that name at all. Using one for both fills nothing and reports
# "0 orders queued" without an error anywhere.
if docker ps --format '{{.Names}}' 2>/dev/null | grep -qx ft-kafka; then
  BOOT="${BOOTSTRAP_SERVERS:-kafka:19092}"          # from inside the network
  HOST_BOOT="${HOST_BOOTSTRAP_SERVERS:-localhost:9092}"   # from this host
  kcli() { local b="$1"; shift; docker exec -e KAFKA_OPTS= ft-kafka "/opt/kafka/bin/$b" "$@"; }
else
  : "${BOOTSTRAP_SERVERS:?set BOOTSTRAP_SERVERS when there is no local broker}"
  BOOT="$BOOTSTRAP_SERVERS"
  HOST_BOOT="${HOST_BOOTSTRAP_SERVERS:-$BOOTSTRAP_SERVERS}"
  kcli() { local b="$1"; shift; docker run --rm --network host apache/kafka:3.9.2 "/opt/kafka/bin/$b" "$@"; }
fi

count() {
  kcli kafka-get-offsets.sh --bootstrap-server "$BOOT" --topic "$1" 2>/dev/null \
    | awk -F: '{s += $3} END {print s + 0}'
}
# Cancels, then confirms. Killing the script that started a run does not cancel
# the jobs it submitted -- they keep their slots, and the next submission finds
# none free, never schedules its tasks, and aborts every checkpoint with "not all
# required tasks are running". Nothing is written and nothing says why, so the
# symptom reads as a slow cold start and invites a longer warm-up that cannot
# help. Leaving even one job behind poisons every case after it.
cancel_all() {
  for id in $(curl -sf http://localhost:8081/jobs | grep -oE '[0-9a-f]{32}'); do
    docker compose -f "$COMPOSE" exec -T jobmanager flink cancel "$id" >/dev/null 2>&1
  done
  for _ in $(seq 1 30); do
    local n
    # grep -c prints 0 and exits non-zero when nothing matches, so "|| echo 0"
    # appends a second line and the count becomes "0\n0" -- which fails the
    # numeric test and reports a busy cluster on an empty one.
    n=$(curl -sf http://localhost:8081/jobs 2>/dev/null | grep -o '"status":"RUNNING"' | wc -l | tr -d ' ')
    [ "${n:-0}" -eq 0 ] && return 0
    sleep 2
  done
  say "  jobs still running after cancel; refusing to measure against a busy cluster"
  exit 9
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
BOOTSTRAP_SERVERS="$HOST_BOOT" GENERATOR_THREADS=4 COMPRESSION_TYPE=lz4 BATCH_SIZE=262144 \
  TRADES_PER_SECOND=4000000 PRICES_PER_SECOND=200000 \
  MAX_TRADES="$BACKLOG" MAX_PRICES=$(( BACKLOG / 100 )) \
  java -jar generators/target/generators.jar >/dev/null 2>&1
queued=$(count orders)
say "   $queued orders queued"
if [ "$queued" -lt "$BACKLOG" ]; then
  say "   expected $BACKLOG; the fill did not complete, so nothing below would mean anything"
  exit 3
fi
say ""
# Each row carries what it cost, so a flat step can be attributed rather than
# guessed at: whether Flink ran out of cores, or the broker did, or neither and
# the job simply queued.
printf "  %6s %13s %9s %8s %8s %9s\n" \
  units "orders/sec" "vs prev" "flink" "broker" "back-pr" | tee -a "$OUT"
printf "  %6s %13s %9s %8s %8s %9s\n" "" "" "" "cores" "cores" "essure" | tee -a "$OUT"

prev=0
for n in $UNITS; do
  cancel_all
  recreate "$SINKS"

  # A fresh transactional namespace per case. Flink must fence every lingering
  # transaction under a sink's prefix before that sink can start, and a benchmark
  # restarting the same job dozens of times against one cluster leaves all of
  # them behind: 98,677 ids had accumulated on MSK, and a sink took 470s to reach
  # RUNNING instead of 31s. It reads as an unexplained cold start that grows with
  # every run, and no amount of extra warm-up touches it.
  export TRANSACTIONAL_ID_SCOPE="u${n}-${RUN_ID}"
  export TASKMANAGER_CPUS="$n" PARALLELISM="$n" CHECKPOINT_INTERVAL_MS
  # Two jobs run, each wanting its own slots.
  export TASK_SLOTS=$(( n * 2 ))
  docker compose -f "$COMPOSE" up -d --wait prometheus >/dev/null 2>&1
  docker compose -f "$COMPOSE" up -d --force-recreate --wait taskmanager >/dev/null 2>&1

  # Refuse to report a number for a limit that was not applied. Setting
  # TASKMANAGER_CPUS as a prefix on one command is not enough: any later compose
  # call that cannot see it computes a different desired config and silently
  # recreates the container without the limit.
  applied=$(docker inspect ft-taskmanager --format '{{.HostConfig.NanoCpus}}' 2>/dev/null)
  want=$(( n * 1000000000 ))
  # Checked wherever the run happens. Limiting this to the local compose file let
  # an AWS run report four numbers taken with no CPU limit at all.
  if [ "${applied:-0}" != "$want" ]; then
    say "  ${n}: NanoCpus=${applied}, expected ${want} -- refusing to report"
    say "     stopping here: the remaining cases would fail the same way"
    exit 5
  fi

  docker compose -f "$COMPOSE" run --rm submit >/dev/null 2>&1
  running=$(curl -sf http://localhost:8081/jobs | grep -c '"status":"RUNNING"')
  [ "$running" -ge 1 ] || { say "  ${n}: no running job -- refusing to report"; exit 6; }

  sleep "$WARMUP_SECONDS"
  a=$(count positions-by-symbol)
  sleep "$WINDOW_SECONDS"
  b=$(count positions-by-symbol)

  # A window that runs past the end of the backlog measures the silence after it.
  if [ "$b" -ge "$queued" ]; then
    say "  ${n}: backlog exhausted inside the window -- raise BACKLOG"
    say "     stopping here: every later case is faster and would exhaust it too"
    exit 7
  fi

  rate=$(( (b - a) / WINDOW_SECONDS ))
  if [ "$rate" -le 0 ]; then
    say "  ${n}: no progress in the window -- the warm-up was too short to clear the cold start"
    say "     stopping here rather than repeating it three more times"
    exit 8
  fi

  promq() {
    curl -sfG 'http://localhost:9090/api/v1/query' --data-urlencode "query=$1" 2>/dev/null \
      | grep -o '"value":\[[0-9.]*,"[0-9.e+-]*"' | sed 's/.*,"//' | head -1
  }
  fmt() { awk -v v="${1:-0}" 'BEGIN { printf "%.2f", v }'; }
  flink_cores=$(fmt "$(promq 'rate(flink_taskmanager_Status_JVM_CPU_Time[2m]) / 1e9')")
  broker_cores=$(fmt "$(promq 'ft:broker_cpu_cores')")
  backpressure=$(fmt "$(promq 'max(flink_taskmanager_job_task_backPressuredTimeMsPerSecond) / 10')")
  if [ "$prev" -gt 0 ]; then
    ratio=$(awk -v r="$rate" -v p="$prev" 'BEGIN { printf "%.2fx", r/p }')
  else
    ratio="-"
  fi
  printf "  %6s %13s %9s %8s %8s %8s%%\n" \
    "$n" "$rate" "$ratio" "$flink_cores" "$broker_cores" "$backpressure" | tee -a "$OUT"
  prev=$rate
done

say ""
say "written to $OUT"
