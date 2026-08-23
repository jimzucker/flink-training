#!/usr/bin/env bash
# Brings up everything for the live demo and leaves it running.
#
# The window is shortened to ten seconds here. The requirements specify one
# minute and the job defaults to it; ten is used for the demo purely so the
# market value sinks say something within the first few seconds rather than
# after a minute of dead air. Say so when presenting -- it is a presentation
# choice, not a difference in the calculation.
set -euo pipefail

cd "$(dirname "$0")/.." || exit 1

COMPOSE="docker/compose.yml"
DEMO_WINDOW_MS="${DEMO_WINDOW_MS:-10000}"
PARALLELISM="${PARALLELISM:-2}"

if [ ! -f jobs/target/jobs.jar ] || [ ! -f generators/target/generators.jar ]; then
  echo "build first:  source scripts/env.sh && mvn package -DskipTests" >&2
  exit 2
fi

echo "starting the stack"
docker compose -f "$COMPOSE" up -d --wait \
  kafka jobmanager taskmanager prometheus grafana renderer >/dev/null

for _ in $(seq 1 40); do
  slots=$(curl -sf http://localhost:8081/overview 2>/dev/null | jq -r '."slots-total"' || echo 0)
  [ "${slots:-0}" -ge 1 ] && break
  sleep 2
done

docker compose -f "$COMPOSE" run --rm topics >/dev/null 2>&1
echo "  topics ready"

for id in $(docker compose -f "$COMPOSE" exec -T jobmanager flink list -r 2>/dev/null \
            | grep -oE '[0-9a-f]{32}' || true); do
  docker compose -f "$COMPOSE" exec -T jobmanager flink cancel "$id" >/dev/null 2>&1 || true
done

WINDOW_MS="$DEMO_WINDOW_MS" PARALLELISM="$PARALLELISM" \
  docker compose -f "$COMPOSE" --profile submit run --rm submit >/dev/null 2>&1
echo "  both jobs submitted (window ${DEMO_WINDOW_MS}ms, parallelism ${PARALLELISM})"

cat <<EOF

  ready. open these, in this order:

    the pipeline diagram      docs/design/pipeline.svg
    the dashboard             http://localhost:3000/d/flink-training/block-trade-pipeline
    the Flink UI              http://localhost:8081

  check the dashboard actually draws before anyone is watching.

  then start the data:

    java -jar generators/target/generators.jar

  and to explain any number on it:

    ./scripts/verify-run.sh
    ./scripts/trace-trade.sh <tradeId>

EOF
