#!/usr/bin/env bash
# Creates the six topics from the pipeline design. Idempotent.
set -euo pipefail

BOOTSTRAP="${BOOTSTRAP:-kafka:19092,kafka2:19092,kafka3:19092}"
PARTITIONS="${PARTITIONS:-12}"
# Replication factor 1, as it was with a single broker: this cluster exists to
# raise write throughput on one machine, not to survive losing a broker, and
# replicating every write would spend the capacity the extra brokers bought.
REPLICATION="${REPLICATION_FACTOR:-1}"
KT=/opt/kafka/bin/kafka-topics.sh

# Numbered as on the design diagram.
TOPICS=(
  "orders"                 # 1  K: tradeId       V: BlockTrade
  "prices"                 # 2  K: symbol        V: Price
  "positions-by-symbol"    # 3  K: symbol        V: Position
  "positions-by-account"   # 4  K: acct/sub/sym  V: Position
  "mv-by-symbol"           # 5  K: symbol        V: MarketValue
  "mv-by-account"          # 6  K: acct/sub/sym  V: MarketValue
)

for t in "${TOPICS[@]}"; do
  if "$KT" --bootstrap-server "$BOOTSTRAP" --describe --topic "$t" >/dev/null 2>&1; then
    echo "exists   $t"
  else
    # --if-not-exists because describing and then creating is not atomic: two
    # runs racing each other both see it missing and the loser fails.
    "$KT" --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
          --topic "$t" --partitions "$PARTITIONS" --replication-factor "$REPLICATION" >/dev/null
    echo "created  $t (partitions=$PARTITIONS, replication=$REPLICATION)"
  fi
done

echo
"$KT" --bootstrap-server "$BOOTSTRAP" --list
