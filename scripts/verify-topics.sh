#!/usr/bin/env bash
# Reads what is actually on the topics and checks it against the assignment's
# expected-output table. Reports observed vs expected rather than just pass/fail,
# so a mismatch says what it saw.
set -uo pipefail

BOOTSTRAP="${BOOTSTRAP:-localhost:9092}"
CONTAINER="${CONTAINER:-ft-kafka}"
SECONDS_RUN="${SECONDS_RUN:-10}"
FAILURES=0

consume() {  # topic max_messages
  docker exec "$CONTAINER" /opt/kafka/bin/kafka-console-consumer.sh \
      --bootstrap-server localhost:9092 --topic "$1" \
      --from-beginning --max-messages "$2" --timeout-ms 15000 2>/dev/null
}

check() {  # label expected actual
  if [ "$2" = "$3" ]; then
    printf "  %-46s %-10s OK\n" "$1" "$3"
  else
    printf "  %-46s %-10s EXPECTED %s\n" "$1" "$3" "$2"
    FAILURES=$((FAILURES + 1))
  fi
}

ORDERS_EXPECTED=$((10 * SECONDS_RUN))
echo "orders  (expecting ${ORDERS_EXPECTED} over ${SECONDS_RUN}s at 10/sec)"
ORDERS=$(consume orders "$ORDERS_EXPECTED")

check "record count"                 "$ORDERS_EXPECTED" "$(echo "$ORDERS" | grep -c .)"
check "unique symbols (sink 3 keys)" "4"  "$(echo "$ORDERS" | jq -r '.symbol' | sort -u | grep -c .)"
check "allocations (sink 4 rate)"    "$((ORDERS_EXPECTED * 4))" \
      "$(echo "$ORDERS" | jq -r '.allocations | length' | paste -sd+ - | bc)"
check "unique account keys (sink 4)" "16" \
      "$(echo "$ORDERS" | jq -r '.symbol as $s | .allocations[] | "\(.account)/\(.subAccount)/\($s)"' | sort -u | grep -c .)"
check "allocations sum to block qty" "0" \
      "$(echo "$ORDERS" | jq -r 'select(([.allocations[].quantity] | add) != .quantity) | .tradeId' | grep -c .)"
check "sides present"                "2"  "$(echo "$ORDERS" | jq -r '.side' | sort -u | grep -c .)"
check "duplicate tradeIds"           "0" \
      "$(echo "$ORDERS" | jq -r '.tradeId' | sort | uniq -d | grep -c .)"

echo "  buy/sell mix: $(echo "$ORDERS" | jq -r '.side' | sort | uniq -c | tr '\n' ' ')"

PRICES_EXPECTED=$((4 * SECONDS_RUN))
echo
echo "prices  (expecting ${PRICES_EXPECTED} over ${SECONDS_RUN}s at 1 tick/sec x 4 symbols)"
PRICES=$(consume prices "$PRICES_EXPECTED")

check "record count"                 "$PRICES_EXPECTED" "$(echo "$PRICES" | grep -c .)"
check "unique symbols"               "4"  "$(echo "$PRICES" | jq -r '.symbol' | sort -u | grep -c .)"
check "non-positive prices"          "0"  "$(echo "$PRICES" | jq -r 'select(.price <= 0) | .symbol' | grep -c .)"
check "prices off a quarter"         "0"  "$(echo "$PRICES" | jq -r 'select((.price * 4 | floor) != (.price * 4)) | .symbol' | grep -c .)"

echo
if [ "$FAILURES" -eq 0 ]; then
  echo "all checks passed"
else
  echo "$FAILURES check(s) failed"
fi
exit "$FAILURES"
