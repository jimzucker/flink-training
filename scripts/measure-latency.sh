#!/usr/bin/env bash
# Measures how old a record is when it becomes readable.
#
# Two numbers matter and they are not the same. The operators publish their own
# processing latency, which is what the pipeline took. This measures what a
# consumer waits for, which under exactly-once also includes the checkpoint that
# had to commit before the record became readable. Reporting only the first would
# flatter the result.
set -euo pipefail

cd "$(dirname "$0")/.." || exit 1

JAR=generators/target/generators.jar
BOOTSTRAP="${BOOTSTRAP:-localhost:9092}"
SECONDS_TO_WATCH="${SECONDS_TO_WATCH:-30}"

if [ ! -f "$JAR" ]; then
  echo "build first:  mvn package -DskipTests" >&2
  exit 2
fi

echo "watching for ${SECONDS_TO_WATCH}s -- age of each record when it became readable"
echo

echo "orders (what the pipeline is fed)"
java -cp "$JAR" io.github.jimzucker.flinktraining.tools.MeasureLatency \
  "$BOOTSTRAP" orders "$SECONDS_TO_WATCH" eventTime | sed 's/^/  /' || true

echo
echo "positions -- trade created to position readable, the order latency"
for topic in positions-by-symbol positions-by-account; do
  java -cp "$JAR" io.github.jimzucker.flinktraining.tools.MeasureLatency \
    "$BOOTSTRAP" "$topic" "$SECONDS_TO_WATCH" eventTime | sed 's/^/  /' || true
done

cat <<'EOF'

  Two costs are inside the position figures and both are deliberate.

  A record is not readable until the checkpoint that produced it commits, so the
  checkpoint interval is a floor: a position is a running sum, and a record
  replayed after a failure would be a wrong number rather than a duplicate.

  Market value is not measured here. Its delay is the window itself, which is the
  specification rather than a cost.
EOF
