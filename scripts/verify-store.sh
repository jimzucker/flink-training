#!/usr/bin/env bash
# Proves the Redis store holds exactly the latest committed value of every key
# on the four output topics, and no other keys. No tolerances.
#
# Needs the stack up with the store profile, and the generators stopped so the
# topics stop growing -- the check waits for them to be quiet, then compares:
#
#   docker compose -f docker/compose.yml --profile store up -d
#   ...run load...
#   docker stop ft-generators
#   scripts/verify-store.sh
set -euo pipefail
cd "$(dirname "$0")/.." || exit 1

JAR=generators/target/generators.jar
if [ ! -f "$JAR" ]; then
  echo "build first:  mvn package -DskipTests" >&2
  exit 2
fi

exec java -cp "$JAR" io.github.jimzucker.flinktraining.store.VerifyStore \
  "${BOOTSTRAP:-localhost:9092}" "${REDIS_HOST:-localhost}" "${REDIS_PORT:-6379}" "${QUIET_SECONDS:-15}"
