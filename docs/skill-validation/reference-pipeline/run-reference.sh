#!/bin/sh
# Re-run the harness against a pipeline we already trust.
#
# WHY THIS EXISTS
#
# Testing a harness change used to mean a clean-room run: an agent reading the
# skill and building a whole Flink project from scratch, then debugging what it
# built. Runs 44, 45 and 46 cost 363,000, 559,000 and 294,000 tokens and two to
# three hours each, and the measurement itself was under an hour of that. Run
# 46's single largest cost was 25 minutes on a watermark bug in its own job,
# which taught us nothing about the harness.
#
# Two different questions were being bought together:
#
#   1. does the harness work?          <- this script, about an hour, no building
#   2. can an agent build from the     <- a clean room, and worth it only when
#      skill's prose?                     what the skill TEACHES has changed
#
# WHAT THIS IS
#
# Clean-room run 44's build: temperature readings in, one average per location
# per hour out. Its ten cases all measured, no ceilings, and its completeness
# passed both arms with no tolerances. Expected outcome is in README.md.
#
# USE
#
#   sh run-reference.sh [destination-directory]
#
# Default destination is /tmp/refpipe-<date>. Needs Java 17 on PATH (in this
# repo: `source scripts/env.sh`) and the skill installed at
# ~/.claude/skills/scalable-flink-skill.

set -e
HERE=$(cd "$(dirname "$0")" && pwd)
DEST=${1:-/tmp/refpipe-$(date +%Y%m%d-%H%M%S)}
SKILL=${SKILL_DIR:-$HOME/.claude/skills/scalable-flink-skill}

if [ ! -f "$SKILL/harness/prove.py" ]; then
    echo "no harness at $SKILL/harness/prove.py — set SKILL_DIR"
    exit 2
fi
if ! java -version 2>&1 | grep -q '"17'; then
    echo "java 17 is not on PATH. In this repo: source scripts/env.sh"
    java -version 2>&1 | head -1
    exit 2
fi

echo "copying the reference pipeline to $DEST"
mkdir -p "$DEST"
cp -R "$HERE/job" "$HERE/dashboard" "$HERE/ANSWERS.md" "$DEST/"
sed "s#{RUNDIR}#$DEST#g" "$HERE/pipeline.json" > "$DEST/pipeline.json"

echo "building the jar"
(cd "$DEST/job" && mvn -q -DskipTests package)
ls -l "$DEST/job/target/st44.jar"

echo "running the chain — this is the part that takes about an hour"
export DOCKER_CONFIG=${DOCKER_CONFIG:-$HOME/.docker-nohelper}
export PIPELINE_JSON="$DEST/pipeline.json"
cd "$DEST"
python3 "$SKILL/harness/prove.py" all

echo
echo "verdict: $(cat "$DEST/results/DONE" 2>/dev/null || echo 'no DONE file')"
echo "table:   $DEST/results/suite.txt"
echo "compare against the expected outcome in $HERE/README.md"
