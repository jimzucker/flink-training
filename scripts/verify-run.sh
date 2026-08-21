#!/usr/bin/env bash
# Produces a verifiable run and checks it, end to end, with no tolerances.
#
# Resets the input topics, emits an exact number of records in replay mode, then
# asserts the expected-output table against what actually landed. Two invocations
# produce byte-identical topics.
set -euo pipefail

cd "$(dirname "$0")/.."

COMPOSE="docker/compose.yml"
CONTAINER="${CONTAINER:-ft-kafka}"
TRADES="${TRADES:-100}"
PRICES="${PRICES:-400}"
SEED="${SEED:-42}"
START_EPOCH_MILLIS="${START_EPOCH_MILLIS:-1700000000000}"
JAR="generators/target/generators.jar"
# 1 when the positions job should be submitted and sinks 3 and 4 checked.
WITH_PIPELINE="${WITH_PIPELINE:-1}"

if [ ! -f "$JAR" ]; then
  echo "build first:  mvn package -DskipTests" >&2
  exit 2
fi
if [ "$WITH_PIPELINE" = "1" ] && [ ! -f jobs/target/jobs.jar ]; then
  echo "build first:  mvn package -DskipTests" >&2
  exit 2
fi

# Topic deletion is asynchronous. Recreating before it completes leaves the old
# records in place, which turns an exact count check into a race -- and a check
# that races is worse than no check, because it fails for the wrong reason.
topic_exists() {
  docker exec "$CONTAINER" /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server localhost:9092 --list 2>/dev/null | grep -qx "$1"
}

RESET_TOPICS="orders prices"
if [ "$WITH_PIPELINE" = "1" ]; then
  RESET_TOPICS="$RESET_TOPICS positions-by-symbol positions-by-account"
fi

echo "resetting topics: $RESET_TOPICS"
for t in $RESET_TOPICS; do
  docker exec "$CONTAINER" /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server localhost:9092 --delete --topic "$t" >/dev/null 2>&1 || true
done
for t in $RESET_TOPICS; do
  for _ in $(seq 1 60); do
    topic_exists "$t" || break
    sleep 0.5
  done
  if topic_exists "$t"; then
    echo "topic $t still present after delete; aborting rather than verifying stale data" >&2
    exit 3
  fi
done
docker compose -f "$COMPOSE" run --rm topics >/dev/null 2>&1

if [ "$WITH_PIPELINE" = "1" ]; then
  # Restart the job so it reads the freshly created topics from the beginning
  # and carries no state from a previous run.
  echo "restarting the positions job"
  for id in $(docker compose -f "$COMPOSE" exec -T jobmanager \
                flink list -r 2>/dev/null | grep -oE '[0-9a-f]{32}' || true); do
    docker compose -f "$COMPOSE" exec -T jobmanager flink cancel "$id" >/dev/null 2>&1 || true
  done
  docker compose -f "$COMPOSE" --profile submit run --rm submit >/dev/null 2>&1
  echo "  submitted"
fi

echo "emitting exactly ${TRADES} trades and ${PRICES} prices (seed ${SEED}, replay)"
SEED="$SEED" \
START_EPOCH_MILLIS="$START_EPOCH_MILLIS" \
MAX_TRADES="$TRADES" \
MAX_PRICES="$PRICES" \
  java -jar "$JAR" 2>&1 | grep -E "starting|universe|stopped" || true

if [ "$WITH_PIPELINE" = "1" ]; then
  # Wait for the job to drain the input rather than sleeping a guessed interval.
  echo "waiting for sinks 3 and 4 to catch up"
  for _ in $(seq 1 60); do
    n=$(docker exec "$CONTAINER" /opt/kafka/bin/kafka-console-consumer.sh \
          --bootstrap-server localhost:9092 --topic positions-by-account \
          --from-beginning --timeout-ms 5000 2>/dev/null | grep -c . || true)
    [ "$n" -ge "$((TRADES * 4))" ] && break
    sleep 2
  done
fi

echo
EXPECT_ORDERS="$TRADES" EXPECT_PRICES="$PRICES" CHECK_POSITIONS="$WITH_PIPELINE" \
  ./scripts/verify-topics.sh
