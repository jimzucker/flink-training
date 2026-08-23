#!/usr/bin/env bash
# Starts the generators in a stack brought up with GENERATOR_START=manual.
#
# The container is already running and idle, so this begins producing without
# recreating anything -- the graphs go from flat to flowing on cue.
set -euo pipefail

cd "$(dirname "$0")/.." || exit 1

CONTAINER="${CONTAINER:-ft-generators}"

if ! docker inspect "$CONTAINER" >/dev/null 2>&1; then
  echo "$CONTAINER is not running -- bring the stack up first" >&2
  exit 1
fi

if docker exec "$CONTAINER" pgrep -f "java -jar" >/dev/null 2>&1; then
  echo "generators are already producing"
  exit 0
fi

echo "starting the generators"
docker exec -d "$CONTAINER" java -jar /app/generators.jar
sleep 2
if docker exec "$CONTAINER" pgrep -f "java -jar" >/dev/null 2>&1; then
  echo "  producing"
else
  echo "  failed to start; check: docker logs $CONTAINER" >&2
  exit 1
fi
