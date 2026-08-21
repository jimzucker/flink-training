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
# Defaults are chosen so the one-minute windows actually close. Event time
# advances a second per trade while records are emitted as fast as the pacer
# allows, so 200 trades span 200 seconds of event time in a couple of real
# seconds and cross three minute boundaries. Prices step 250ms each so their
# event time covers the same span -- a join advances at its slower input, and a
# price stream that only reached second 2 would hold every window shut.
TRADES="${TRADES:-200}"
PRICES="${PRICES:-800}"
TRADE_EVENT_TIME_STEP_MS="${TRADE_EVENT_TIME_STEP_MS:-1000}"
PRICE_EVENT_TIME_STEP_MS="${PRICE_EVENT_TIME_STEP_MS:-250}"
EMIT_TRADES_PER_SECOND="${EMIT_TRADES_PER_SECOND:-100}"
EMIT_PRICES_PER_SECOND="${EMIT_PRICES_PER_SECOND:-400}"
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
  RESET_TOPICS="$RESET_TOPICS positions-by-symbol positions-by-account mv-by-symbol mv-by-account"
fi

# Minute boundaries strictly inside the event-time span the run covers. The
# watermark reaches the last event time minus a millisecond, so a boundary
# exactly at the end does not close.
LAST_EVENT_TIME=$((START_EPOCH_MILLIS + (TRADES - 1) * TRADE_EVENT_TIME_STEP_MS))
WINDOWS=0
b=$(( (START_EPOCH_MILLIS / 60000) * 60000 + 60000 ))
while [ "$b" -le "$((LAST_EVENT_TIME - 1))" ]; do
  WINDOWS=$((WINDOWS + 1))
  b=$((b + 60000))
done

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
TRADE_EVENT_TIME_STEP_MS="$TRADE_EVENT_TIME_STEP_MS" \
PRICE_EVENT_TIME_STEP_MS="$PRICE_EVENT_TIME_STEP_MS" \
TRADES_PER_SECOND="$EMIT_TRADES_PER_SECOND" \
PRICES_PER_SECOND="$EMIT_PRICES_PER_SECOND" \
  java -jar "$JAR" 2>&1 | grep -E "starting|universe|stopped" || true

# Waits until a topic holds at least `want` committed records, and reports how
# many it saw.
#
# End offsets cannot be used for this. The sinks write transactionally, and every
# committed transaction appends a marker that advances the offset without being a
# record -- so offsets over-count, and a wait keyed on them can finish before the
# data is there. Consuming with read_committed counts records, and stopping at
# `want` means the read returns as soon as the target is reached rather than
# waiting out a timeout.
committed_count() {  # topic want
  docker exec "$CONTAINER" /opt/kafka/bin/kafka-console-consumer.sh \
      --bootstrap-server localhost:9092 --topic "$1" \
      --isolation-level read_committed --from-beginning \
      --max-messages "$2" --timeout-ms "${CATCHUP_TIMEOUT_MS:-90000}" 2>/dev/null \
    | grep -c . || true
}

if [ "$WITH_PIPELINE" = "1" ]; then
  # Under exactly-once a record is not readable until its checkpoint commits, so
  # this waits for a checkpoint as much as for the processing.
  echo "waiting for sinks 3 and 4 to commit"
  s3=$(committed_count positions-by-symbol "$TRADES")
  s4=$(committed_count positions-by-account "$((TRADES * 4))")
  echo "  sink 3: ${s3}/${TRADES}   sink 4: ${s4}/$((TRADES * 4))"

  echo "waiting for sinks 5 and 6 (expecting ${WINDOWS} windows)"
  s5=$(committed_count mv-by-symbol "$((4 * WINDOWS))")
  s6=$(committed_count mv-by-account "$((16 * WINDOWS))")
  echo "  sink 5: ${s5}/$((4 * WINDOWS))   sink 6: ${s6}/$((16 * WINDOWS))"
fi

echo
EXPECT_ORDERS="$TRADES" EXPECT_PRICES="$PRICES" \
CHECK_POSITIONS="$WITH_PIPELINE" CHECK_MARKET_VALUE="$WITH_PIPELINE" WINDOWS="$WINDOWS" \
  ./scripts/verify-topics.sh
