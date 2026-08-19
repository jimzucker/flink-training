#!/usr/bin/env bash
# Creates the six topics from the pipeline design. Idempotent.
set -euo pipefail

BOOTSTRAP="${BOOTSTRAP:-kafka:19092}"
PARTITIONS="${PARTITIONS:-4}"
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
    "$KT" --bootstrap-server "$BOOTSTRAP" --create \
          --topic "$t" --partitions "$PARTITIONS" --replication-factor 1 >/dev/null
    echo "created  $t (partitions=$PARTITIONS)"
  fi
done

echo
"$KT" --bootstrap-server "$BOOTSTRAP" --list
