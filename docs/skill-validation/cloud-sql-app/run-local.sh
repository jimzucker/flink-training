#!/bin/sh
# The laptop side of the cloud SQL app: the steps the cloud's "local first" gate reads.
# Stops at the first step that does not pass.
B=$(cd "$(dirname "$0")" && pwd)
cd "$B"
. ~/code/GitHub/flink-training/scripts/env.sh >/dev/null 2>&1
export DOCKER_CONFIG=$HOME/.docker-nohelper PIPELINE_JSON=$B/pipeline.json
H=$HOME/.claude/skills/scalable-flink-skill/harness/prove.py
for step in ${STEPS:-up preflight completeness tinyproof}; do
  echo "=== $step $(date +%H:%M:%S)"
  python3 -u $H $step
  rc=$?
  echo "=== $step rc=$rc $(date +%H:%M:%S)"
  [ $rc -ne 0 ] && { echo "STOPPED at $step"; exit $rc; }
done
echo "LOCAL PASS"
