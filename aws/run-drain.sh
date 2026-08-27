#!/usr/bin/env bash
# Runs on the client instance: fills a backlog on MSK, then times Flink draining
# it. The local counterpart is scripts/scale-catchup.sh; this one differs in two
# ways that matter.
#
# Kafka's CLI comes from a throwaway container rather than the broker's, because
# there is no broker container here -- MSK is a managed service and there is
# nothing to `docker exec` into.
#
# And completion is detected from topic offsets rather than from a Flink metric.
# Sink 3 emits exactly one position per order, so the drain is done when
# positions-by-symbol holds as many records as orders. That needs no Prometheus
# and cannot be fooled by a counter left behind by a previous job, which is a
# failure the local script had to grow a guard against.
set -uo pipefail
cd "$(dirname "$0")/.." || exit 1

: "${BOOTSTRAP_SERVERS:?set to the MSK bootstrap brokers}"
PARALLELISM="${PARALLELISM:-4}"
# Two jobs run, each wanting PARALLELISM slots, so the task manager needs twice
# the parallelism. Locally eight slots fitted two jobs at parallelism four
# exactly, and carrying that number forward to parallelism 16 gave one job all
# sixteen slots and left the other unable to schedule: both reported RUNNING,
# nothing progressed, and the machine sat 99% idle with no exception anywhere.
TASK_SLOTS="${TASK_SLOTS:-$(( PARALLELISM * 2 ))}"
BACKLOG="${BACKLOG:-88678174}"
PARTITIONS="${PARTITIONS:-4}"
# Exactly-once opens a fresh transactional producer per sink subtask per
# checkpoint, so the registrations the broker must service scale with parallelism
# times checkpoint frequency. At parallelism 16 over four sinks a one-second
# interval asks a remote MSK coordinator for 64 InitProducerId round trips every
# second: 101 of 112 checkpoints failed, transactions never committed, and the
# pipeline stopped dead at 106,355 records with the machine 99% idle and no
# exception logged anywhere. One second was tuned against a broker on localhost
# at parallelism 2.
CHECKPOINT_INTERVAL_MS="${CHECKPOINT_INTERVAL_MS:-10000}"
REPLICATION="${REPLICATION_FACTOR:-1}"
TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-3600}"
COMPOSE="docker/compose.aws.yml"
TOPICS="orders prices positions-by-symbol positions-by-account mv-by-symbol mv-by-account"
OUT="${OUT:-docs/steps/step-11/aws-drain-p${PARALLELISM}.txt}"

mkdir -p "$(dirname "$OUT")"
say() { echo "$@" | tee -a "$OUT"; }

kcli() {
  local bin="$1"; shift
  docker run --rm --network host apache/kafka:3.9.2 "/opt/kafka/bin/$bin" "$@"
}
records() {
  kcli kafka-get-offsets.sh --bootstrap-server "$BOOTSTRAP_SERVERS" --topic "$1" 2>/dev/null \
    | awk -F: '{s += $3} END {print s + 0}'
}

: > "$OUT"
say "AWS drain  ($(date -u +%Y-%m-%dT%H:%M:%SZ))"
say "brokers: $BOOTSTRAP_SERVERS"
say "parallelism $PARALLELISM, $TASK_SLOTS slots, $PARTITIONS partitions, replication $REPLICATION, checkpoint ${CHECKPOINT_INTERVAL_MS}ms"

docker compose -f "$COMPOSE" down --remove-orphans >/dev/null 2>&1

# With the backlog kept, only the sinks are reset -- recreating the input topics
# would delete the very orders being reused, which reads as "reusing the 0 orders
# already in the topic" and then waits forever.
if [ "${SKIP_FILL:-0}" = "1" ]; then
  RECREATE="positions-by-symbol positions-by-account mv-by-symbol mv-by-account"
  say "-- recreating the sink topics, keeping the backlog"
else
  RECREATE="$TOPICS"
  say "-- recreating topics"
fi
for t in $RECREATE; do
  kcli kafka-topics.sh --bootstrap-server "$BOOTSTRAP_SERVERS" --delete --topic "$t" >/dev/null 2>&1
done
for t in $RECREATE; do
  for _ in $(seq 1 120); do
    kcli kafka-topics.sh --bootstrap-server "$BOOTSTRAP_SERVERS" --list 2>/dev/null | grep -qx "$t" || break
    sleep 1
  done
done
for t in $RECREATE; do
  kcli kafka-topics.sh --bootstrap-server "$BOOTSTRAP_SERVERS" --create --if-not-exists \
    --topic "$t" --partitions "$PARTITIONS" --replication-factor "$REPLICATION" >/dev/null 2>&1
done

if [ "${SKIP_FILL:-0}" = "1" ]; then
  queued=$(records orders)
  say "-- reusing the $queued orders already in the topic"
else
say "-- filling $BACKLOG orders with the job stopped"
fill_start=$(date +%s)
# Prices are filled at full speed, not at the demo's 2000/sec. The fill blocks
# until both streams finish, and on a fast machine the orders complete in about a
# minute while 886,781 prices paced at 2000/sec take seven and a half -- so the
# stream that contributes least to the measurement became the critical path, with
# the box sitting 99.6% idle waiting for it. Pacing belongs in the demo, not in
# the setup for a throughput run.
BOOTSTRAP_SERVERS="$BOOTSTRAP_SERVERS" GENERATOR_THREADS=4 COMPRESSION_TYPE=lz4 \
  BATCH_SIZE=262144 TRADES_PER_SECOND=4000000 \
  PRICES_PER_SECOND="${FILL_PRICES_PER_SECOND:-200000}" \
  MAX_TRADES="$BACKLOG" MAX_PRICES=$(( BACKLOG / 100 )) \
  java -jar generators/target/generators.jar >/dev/null 2>&1
queued=$(records orders)
say "   filled $queued orders in $(( $(date +%s) - fill_start ))s"
fi

say "-- starting Flink at parallelism $PARALLELISM"
BOOTSTRAP_SERVERS="$BOOTSTRAP_SERVERS" PARALLELISM="$PARALLELISM" \
  docker compose -f "$COMPOSE" up -d --wait jobmanager taskmanager prometheus grafana >/dev/null 2>&1
BOOTSTRAP_SERVERS="$BOOTSTRAP_SERVERS" PARALLELISM="$PARALLELISM" TASK_SLOTS="$TASK_SLOTS" \
  CHECKPOINT_INTERVAL_MS="$CHECKPOINT_INTERVAL_MS" \
  docker compose -f "$COMPOSE" run --rm submit >/dev/null 2>&1
running=$(curl -sf http://localhost:8081/jobs | grep -o '"status":"RUNNING"' | wc -l | tr -d ' ')
if [ "$running" != "2" ]; then
  say "   expected 2 running jobs, got $running; refusing to report a number"
  exit 4
fi

say ""
printf "  %6s %14s %14s %10s\n" t consumed remaining "orders/s" | tee -a "$OUT"
t0=$(date +%s); last=0; lt=$t0
while :; do
  sleep 20
  now=$(date +%s); el=$(( now - t0 )); d=$(( now - lt )); [ "$d" -lt 1 ] && d=1
  done_count=$(records positions-by-symbol)
  printf "  %5ss %14s %14s %10s\n" "$el" "$done_count" "$(( queued - done_count ))" \
    "$(( (done_count - last) / d ))" | tee -a "$OUT"
  last=$done_count; lt=$now
  if [ "$done_count" -ge "$queued" ]; then
    say ""
    say "  DRAINED $queued orders in ${el}s = $(( queued / el )) orders/sec"
    say "  allocations $(( (queued / el) * 4 ))/sec   records written $(( (queued / el) * 5 ))/sec"
    break
  fi
  if [ "$el" -gt "$TIMEOUT_SECONDS" ]; then
    say "  did not drain within ${TIMEOUT_SECONDS}s; $(( queued - done_count )) remaining"
    break
  fi
done
say ""
say "written to $OUT"
