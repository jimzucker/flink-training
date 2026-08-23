#!/usr/bin/env bash
# Follows one trade through all six topics.
#
# The requirements are blunt about this: you have to be able to explain the
# numbers, and if you cannot answer a question you take it as an action item to
# change the logging until you can. This is that, made permanent -- every record
# carries the trade that last moved it, so any figure on the dashboard can be
# walked back to the block trade that caused it.
set -uo pipefail

cd "$(dirname "$0")/.." || exit 1

TRADE="${1:-}"
DUMP_DIR="${DUMP_DIR:-target/topic-dump}"

if [ -z "$TRADE" ]; then
  echo "usage: $0 <tradeId>            e.g. $0 T000000042" >&2
  echo "       reads $DUMP_DIR, populated by scripts/verify-run.sh" >&2
  exit 2
fi
if [ ! -f "$DUMP_DIR/orders.jsonl" ]; then
  echo "no topic dump at $DUMP_DIR -- run scripts/verify-run.sh first" >&2
  exit 2
fi

hr() { printf '%s\n' "------------------------------------------------------------"; }

hr
echo "① orders — the block trade"
hr
jq -r --arg t "$TRADE" 'select(.tradeId == $t)
  | "  \(.tradeId)  \(.side) \(.quantity) \(.symbol)   eventTime=\(.eventTime)",
    "  allocations:", (.allocations[] | "    \(.account)/\(.subAccount)  \(.quantity)")' \
  "$DUMP_DIR/orders.jsonl"

SIDE=$(jq -r --arg t "$TRADE" 'select(.tradeId == $t) | .side' "$DUMP_DIR/orders.jsonl")
SYMBOL=$(jq -r --arg t "$TRADE" 'select(.tradeId == $t) | .symbol' "$DUMP_DIR/orders.jsonl")
QTY=$(jq -r --arg t "$TRADE" 'select(.tradeId == $t) | .quantity' "$DUMP_DIR/orders.jsonl")
if [ -z "$SYMBOL" ]; then
  echo "  no such trade in the dump" >&2
  exit 1
fi

echo
hr
echo "③ positions-by-symbol — one update, the whole block"
hr
jq -r --arg t "$TRADE" 'select(.lastTradeId == $t)
  | "  \(.key)  quantity=\(.quantity)  (update #\(.updateCount))"' \
  "$DUMP_DIR/positions-by-symbol.jsonl"
echo "  the block moved this key by $([ "$SIDE" = "BUY" ] && echo "+$QTY" || echo "-$QTY")"

echo
hr
echo "④ positions-by-account — one update per allocation"
hr
jq -r --arg t "$TRADE" 'select(.lastTradeId == $t)
  | "  \(.key)  quantity=\(.quantity)  (update #\(.updateCount))"' \
  "$DUMP_DIR/positions-by-account.jsonl" | sort

# The two aggregations answer the same question. Saying so out loud, with the
# arithmetic, is the point of the demo.
SYMBOL_QTY=$(jq -r --arg t "$TRADE" 'select(.lastTradeId == $t) | .quantity' \
             "$DUMP_DIR/positions-by-symbol.jsonl" | head -1)
ACCOUNT_SUM=$(jq -r --arg t "$TRADE" --arg s "$SYMBOL" \
              'select(.lastTradeId == $t and .symbol == $s) | .quantity' \
              "$DUMP_DIR/positions-by-account.jsonl" | paste -sd+ - | bc)
if [ "$SYMBOL_QTY" = "$ACCOUNT_SUM" ]; then
  echo "  the four accounts sum to ${ACCOUNT_SUM}, which is what sink 3 reports  OK"
else
  echo "  the four accounts sum to ${ACCOUNT_SUM}, but sink 3 reports ${SYMBOL_QTY}  MISMATCH"
fi

echo
hr
echo "⑤ / ⑥ market value — the windows this trade closed"
hr
for topic in mv-by-symbol mv-by-account; do
  if [ -f "$DUMP_DIR/$topic.jsonl" ]; then
    jq -r --arg t "$TRADE" --arg n "$topic" 'select(.lastTradeId == $t)
      | "  \($n)  \(.key)  \(.quantity) x \(.price) = \(.marketValue)   windowEnd=\(.windowEnd)"' \
      "$DUMP_DIR/$topic.jsonl" | sort
  fi
done
MV_SYMBOL=$(jq -r --arg t "$TRADE" 'select(.lastTradeId == $t) | .marketValue' \
            "$DUMP_DIR/mv-by-symbol.jsonl" 2>/dev/null | head -1)
MV_ACCOUNTS=$(jq -r --arg t "$TRADE" --arg s "$SYMBOL" \
              'select(.lastTradeId == $t and .symbol == $s) | .marketValue' \
              "$DUMP_DIR/mv-by-account.jsonl" 2>/dev/null | paste -sd+ - | bc)
if [ -n "$MV_SYMBOL" ] && [ -n "$MV_ACCOUNTS" ]; then
  echo
  if [ "$(echo "$MV_SYMBOL == $MV_ACCOUNTS" | bc)" = "1" ]; then
    echo "  the account market values sum to ${MV_ACCOUNTS}, which is what sink 5 reports  OK"
  else
    echo "  the account market values sum to ${MV_ACCOUNTS}, but sink 5 reports ${MV_SYMBOL}  MISMATCH"
  fi
fi

echo
echo "  a window names the trade that last moved its position, so any market"
echo "  value on the dashboard can be walked back to the block trade above."
