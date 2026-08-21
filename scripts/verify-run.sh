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

if [ ! -f "$JAR" ]; then
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

echo "resetting input topics"
for t in orders prices; do
  docker exec "$CONTAINER" /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server localhost:9092 --delete --topic "$t" >/dev/null 2>&1 || true
done
for t in orders prices; do
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

echo "emitting exactly ${TRADES} trades and ${PRICES} prices (seed ${SEED}, replay)"
SEED="$SEED" \
START_EPOCH_MILLIS="$START_EPOCH_MILLIS" \
MAX_TRADES="$TRADES" \
MAX_PRICES="$PRICES" \
  java -jar "$JAR" 2>&1 | grep -E "starting|universe|stopped" || true

echo
EXPECT_ORDERS="$TRADES" EXPECT_PRICES="$PRICES" ./scripts/verify-topics.sh
