#!/usr/bin/env bash
# Decides when the generators start.
#
# Auto is convenient but takes away the most persuasive moment in the demo:
# graphs going from flat to flowing while people watch. Manual keeps that and
# costs one command. A delay gives both -- start the stack, talk through the
# design, and have data arrive on cue.
set -euo pipefail

MODE="${GENERATOR_START:-auto}"
JAR=/app/generators.jar

case "$MODE" in
  auto)
    echo "generators: starting now (GENERATOR_START=auto)"
    exec java -jar "$JAR"
    ;;
  manual)
    echo "generators: idle, waiting to be started (GENERATOR_START=manual)"
    echo "generators: start them with  ./scripts/start-generators.sh"
    # Stays up and idle so it can be triggered without recreating the container.
    exec tail -f /dev/null
    ;;
  ''|*[!0-9]*)
    echo "generators: GENERATOR_START must be auto, manual, or a number of minutes; got '$MODE'" >&2
    exit 2
    ;;
  *)
    echo "generators: starting in ${MODE} minute(s) (GENERATOR_START=${MODE})"
    sleep $(( MODE * 60 ))
    echo "generators: starting now"
    exec java -jar "$JAR"
    ;;
esac
