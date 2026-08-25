#!/usr/bin/env bash
# Kills a task manager mid-run and checks the sinks are still exactly right.
#
# Exactly-once is a claim about what happens when something fails. Without a
# failure the setting is untested, and at-least-once would look identical.
set -euo pipefail

cd "$(dirname "$0")/.."

COMPOSE="docker/compose.yml"
CONTAINER="${CONTAINER:-ft-kafka}"
TRADES="${TRADES:-100}"
PRICES="${PRICES:-400}"

committed() {  # topic want
  docker exec -e KAFKA_OPTS= "$CONTAINER" /opt/kafka/bin/kafka-console-consumer.sh \
      --bootstrap-server "${KAFKA_INTERNAL:-kafka:19092}" --topic "$1" \
      --isolation-level read_committed --from-beginning \
      --max-messages "$2" --timeout-ms 120000 2>/dev/null | grep -c . || true
}

echo "== preparing a clean run =="
WITH_PIPELINE=1 TRADES="$TRADES" PRICES="$PRICES" ./scripts/verify-run.sh >/dev/null
echo "   baseline run is green"

echo
echo "== rerunning, killing the task manager mid-flight =="
for t in orders prices positions-by-symbol positions-by-account; do
  docker exec -e KAFKA_OPTS= "$CONTAINER" /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server "${KAFKA_INTERNAL:-kafka:19092}" --delete --topic "$t" >/dev/null 2>&1 || true
done
for t in orders prices positions-by-symbol positions-by-account; do
  for _ in $(seq 1 60); do
    docker exec -e KAFKA_OPTS= "$CONTAINER" /opt/kafka/bin/kafka-topics.sh \
        --bootstrap-server "${KAFKA_INTERNAL:-kafka:19092}" --list 2>/dev/null | grep -qx "$t" || break
    sleep 0.5
  done
done
docker compose -f "$COMPOSE" run --rm topics >/dev/null 2>&1

for id in $(docker compose -f "$COMPOSE" exec -T jobmanager flink list -r 2>/dev/null \
            | grep -oE '[0-9a-f]{32}' || true); do
  docker compose -f "$COMPOSE" exec -T jobmanager flink cancel "$id" >/dev/null 2>&1 || true
done
docker compose -f "$COMPOSE" --profile submit run --rm submit >/dev/null 2>&1

SEED=42 START_EPOCH_MILLIS=1700000000000 MAX_TRADES="$TRADES" MAX_PRICES="$PRICES" \
  java -jar generators/target/generators.jar >/dev/null 2>&1 &
GEN=$!

sleep 5
echo "   killing ft-taskmanager"
docker kill ft-taskmanager >/dev/null
wait "$GEN" || true

# Bring it back explicitly rather than relying on the restart policy: a
# container created before that policy existed will not honour it, and the test
# would then be measuring a stopped cluster rather than a recovery.
echo "   restarting the task manager"
docker compose -f "$COMPOSE" up -d taskmanager >/dev/null 2>&1
for _ in $(seq 1 40); do
  slots=$(curl -sf http://localhost:8081/overview 2>/dev/null | jq -r '."slots-total"' || echo 0)
  [ "${slots:-0}" -ge 1 ] && break
  sleep 3
done
echo "   generator finished; waiting for the job to recover"

for _ in $(seq 1 60); do
  running=$(curl -sf "http://localhost:8081/jobs" 2>/dev/null \
            | jq -r '[.jobs[] | select(.status=="RUNNING")] | length' || echo 0)
  [ "${running:-0}" -ge 1 ] && break
  sleep 3
done
echo "   running jobs after recovery: ${running:-0}"

echo
echo "== results after the failure =="
S3=$(committed positions-by-symbol "$TRADES")
S4=$(committed positions-by-account "$((TRADES * 4))")
echo "   sink 3: ${S3}  (expect exactly ${TRADES})"
echo "   sink 4: ${S4}  (expect exactly $((TRADES * 4)))"

EXPECT_ORDERS="$TRADES" EXPECT_PRICES="$PRICES" CHECK_POSITIONS=1 ./scripts/verify-topics.sh
