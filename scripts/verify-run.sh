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
# The generator runs exactly as it does for the demo: wall-clock event times,
# paced in real time, at the demo rates. Nothing about the clock is simulated.
#
# Only the window is shortened. Closing three one-minute windows would need three
# and a half minutes of running, and a check that slow stops being run. The
# window length is a runtime parameter, so the test shortens it to ten seconds
# and exercises the same code on the same clock. The demo keeps its minute.
TRADES="${TRADES:-400}"
PRICES="${PRICES:-40000}"
TRADES_PER_SECOND="${TRADES_PER_SECOND:-10}"
PRICES_PER_SECOND="${PRICES_PER_SECOND:-1000}"
VERIFY_WINDOW_MS="${VERIFY_WINDOW_MS:-10000}"
MIN_WINDOWS="${MIN_WINDOWS:-3}"
SEED="${SEED:-42}"
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
  docker exec -e KAFKA_OPTS= "$CONTAINER" /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server "${KAFKA_INTERNAL:-kafka:19092}" --list 2>/dev/null | grep -qx "$1"
}

RESET_TOPICS="orders prices"
if [ "$WITH_PIPELINE" = "1" ]; then
  RESET_TOPICS="$RESET_TOPICS positions-by-symbol positions-by-account mv-by-symbol mv-by-account"
fi



echo "resetting topics: $RESET_TOPICS"
for t in $RESET_TOPICS; do
  docker exec -e KAFKA_OPTS= "$CONTAINER" /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server "${KAFKA_INTERNAL:-kafka:19092}" --delete --topic "$t" >/dev/null 2>&1 || true
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
  WINDOW_MS="$VERIFY_WINDOW_MS" \
    docker compose -f "$COMPOSE" --profile submit run --rm submit >/dev/null 2>&1
  echo "  submitted (window ${VERIFY_WINDOW_MS}ms; the demo uses 60000ms)"
fi

echo "emitting exactly ${TRADES} trades and ${PRICES} prices at the demo rates"
echo "  wall-clock event times, ${TRADES_PER_SECOND}/sec -> about $((TRADES / TRADES_PER_SECOND))s"
SEED="$SEED" \
MAX_TRADES="$TRADES" \
MAX_PRICES="$PRICES" \
TRADES_PER_SECOND="$TRADES_PER_SECOND" \
PRICES_PER_SECOND="$PRICES_PER_SECOND" \
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
  local want="$2"
  for _ in $(seq 1 "${CATCHUP_ATTEMPTS:-40}"); do
    local n
    n=$(java -cp generators/target/generators.jar \
          io.github.jimzucker.flinktraining.tools.TopicDump \
          "${BOOTSTRAP:-localhost:9092}" target/catchup "$1" --deadline-seconds 30 2>/dev/null \
        | awk '{print $2}')
    n=${n:-0}
    if [ "$n" -ge "$want" ]; then
      echo "$n"
      return 0
    fi
    sleep 3
  done
  echo "${n:-0}"
}

if [ "$WITH_PIPELINE" = "1" ]; then
  # Under exactly-once a record is not readable until its checkpoint commits, so
  # this waits for a checkpoint as much as for the processing.
  echo "waiting for sinks 3 and 4 to commit"
  s3=$(committed_count positions-by-symbol "$TRADES")
  s4=$(committed_count positions-by-account "$((TRADES * 4))")
  echo "  sink 3: ${s3}/${TRADES}   sink 4: ${s4}/$((TRADES * 4))"

  # How many windows close depends on where the run starts relative to a window
  # boundary, which is a property of running on a real clock rather than a
  # simulated one. The count is read from the data; everything inside a window
  # stays exact.
  echo "waiting for sinks 5 and 6 (at least ${MIN_WINDOWS} windows)"
  s5=$(committed_count mv-by-symbol "$((4 * MIN_WINDOWS))")
  s6=$(committed_count mv-by-account "$((16 * MIN_WINDOWS))")
  echo "  sink 5: ${s5}   sink 6: ${s6}"
fi

echo
EXPECT_ORDERS="$TRADES" EXPECT_PRICES="$PRICES" \
CHECK_POSITIONS="$WITH_PIPELINE" CHECK_MARKET_VALUE="$WITH_PIPELINE" \
MIN_WINDOWS="$MIN_WINDOWS" WINDOW_MS="$VERIFY_WINDOW_MS" \
  ./scripts/verify-topics.sh
