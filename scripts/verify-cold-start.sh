#!/usr/bin/env bash
# Brings everything down and up again, and checks it came up green on its own.
#
# This is the deliverable in one command: nothing built by hand, no jobs
# submitted by hand, no data started by hand. The checks below are the ones that
# would otherwise be done by squinting at `docker compose ps` and hoping.
set -uo pipefail

cd "$(dirname "$0")/.." || exit 1

COMPOSE="docker/compose.yml"
SETTLE_SECONDS="${SETTLE_SECONDS:-45}"
FAILURES=0

check() {  # label expected actual
  if [ "$2" = "$3" ]; then
    printf "  %-44s %-14s OK\n" "$1" "$3"
  else
    printf "  %-44s %-14s EXPECTED %s\n" "$1" "$3" "$2"
    FAILURES=$((FAILURES + 1))
  fi
}

echo "tearing everything down"
docker compose -f "$COMPOSE" down -v --remove-orphans >/dev/null 2>&1

echo "one command, from nothing"
START=$(date +%s)
if ! docker compose -f "$COMPOSE" up -d --build >/dev/null 2>&1; then
  echo "  up failed" >&2
  exit 1
fi
echo "  came up in $(( $(date +%s) - START ))s; settling for ${SETTLE_SECONDS}s"
sleep "$SETTLE_SECONDS"

echo
echo "services"
for svc in ft-kafka ft-jobmanager ft-taskmanager ft-prometheus ft-grafana ft-generators; do
  status=$(docker inspect "$svc" --format '{{.State.Status}}' 2>/dev/null || echo missing)
  check "$svc" "running" "$status"
done

# A container that exits and is restarted by Docker looks alive in `ps` while
# doing nothing. The generator did exactly that, exiting immediately whenever it
# was run unbounded -- which is how the demo runs it.
RESTARTS=$(docker inspect ft-generators --format '{{.RestartCount}}' 2>/dev/null || echo "?")
check "generator restarts" "0" "$RESTARTS"

echo
echo "jobs"
RUNNING=$(curl -sf http://localhost:8081/jobs 2>/dev/null \
          | jq '[.jobs[] | select(.status=="RUNNING")] | length' 2>/dev/null || echo 0)
check "flink jobs running" "2" "$RUNNING"

echo
echo "data reaching every topic"
for pair in orders:1 prices:1 positions-by-symbol:1 positions-by-account:1; do
  topic="${pair%%:*}"
  n=$(docker exec ft-kafka /opt/kafka/bin/kafka-get-offsets.sh \
        --bootstrap-server localhost:9092 --topic "$topic" 2>/dev/null \
      | awk -F: '{s += $3} END {print (s > 0) ? 1 : 0}')
  check "$topic has records" "1" "${n:-0}"
done

echo
echo "the dashboard"
if curl -sf "http://admin:admin@localhost:3000/api/health" >/dev/null 2>&1; then
  ./scripts/verify-dashboard.sh | tail -n +1 | sed 's/^/  /'
  [ "${PIPESTATUS[0]:-0}" -eq 0 ] || FAILURES=$((FAILURES + 1))
else
  check "grafana reachable" "yes" "no"
fi

echo
if [ "$FAILURES" -eq 0 ]; then
  echo "cold start came up green"
else
  echo "$FAILURES cold-start check(s) failed"
fi
exit "$FAILURES"
