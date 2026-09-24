#!/bin/sh
# one arm: cold broker, check the backlog, run the 4-core suite, keep its results
# usage: arm.sh <label>
set -e
LABEL=$1
EXP=$(cd "$(dirname "$0")" && pwd)
R=~/code/GitHub/flink-skill-test-47
export DOCKER_CONFIG=$HOME/.docker-nohelper PATH=/opt/homebrew/opt/openjdk@17/bin:$PATH PIPELINE_JSON=$R/pipeline.json
H=$HOME/.claude/skills/scalable-flink-skill/harness/prove.py
echo "[$(date +%H:%M:%S)] arm $LABEL: broker memory in pipeline.json = $(python3 -c "import json;print(json.load(open('$R/pipeline.json'))['caps']['kafkaMemory'])")"
cd "$R"
python3 "$H" up
echo "[$(date +%H:%M:%S)] restarting the broker so both arms start with a cold cache"
docker restart st47-kafka
until docker exec st47-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list >/dev/null 2>&1; do sleep 2; done
echo "[$(date +%H:%M:%S)] broker limit read back: $(docker inspect st47-kafka --format '{{.HostConfig.Memory}}') bytes"
N=$(docker exec st47-kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic orders --time -1 | awk -F: '{s+=$3} END {print s}')
echo "[$(date +%H:%M:%S)] orders backlog read back: $N records (expect 165000000)"
[ "$N" = "165000000" ] || { echo "backlog is not what the manifest says; stopping"; exit 3; }
python3 "$H" suite || true
cp "$R/results/suite.json" "$EXP/suite-$LABEL.json"
python3 "$H" report > "$EXP/report-$LABEL.txt" 2>&1 || true
echo "[$(date +%H:%M:%S)] arm $LABEL done"
