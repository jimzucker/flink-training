#!/usr/bin/env bash
# Brings the whole demo up and tells you what to open.
#
# Everything comes from one `docker compose up`: the jars are built inside
# Docker, both jobs are submitted, and the generators start producing. Nothing
# has to be built or launched by hand, because "did you build first?" is not a
# step anyone should have to remember in front of an audience.
set -euo pipefail

cd "$(dirname "$0")/.." || exit 1

COMPOSE="docker/compose.yml"

echo "bringing the stack up (building on first run)"
docker compose -f "$COMPOSE" up -d --build

for _ in $(seq 1 60); do
  running=$(curl -sf http://localhost:8081/jobs 2>/dev/null \
            | jq -r '[.jobs[] | select(.status=="RUNNING")] | length' 2>/dev/null || echo 0)
  [ "${running:-0}" -ge 2 ] && break
  sleep 2
done
echo "  ${running:-0} of 2 jobs running"

cat <<EOF

  open these, in this order:

    the pipeline diagram      docs/design/pipeline.svg
    the dashboard             http://localhost:3000/d/flink-training/block-trade-pipeline
    the Flink UI              http://localhost:8081

  check the dashboard actually draws before anyone is watching.

  the generators are already producing at 10 trades/sec and 1000 prices/sec.
  the window is 10s here rather than the specified minute, so the market value
  sinks say something while people are watching -- say so when you reach them.

  to explain any number on the dashboard:

    ./scripts/verify-run.sh
    ./scripts/trace-trade.sh <tradeId>

  to stop:

    docker compose -f $COMPOSE down -v

EOF
