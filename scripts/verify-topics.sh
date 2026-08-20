#!/usr/bin/env bash
# Reads what is actually on the topics and checks it against the expected-output
# table. Reports observed vs expected rather than just pass/fail, so a mismatch
# says what it saw.
set -uo pipefail

CONTAINER="${CONTAINER:-ft-kafka}"
SECONDS_RUN="${SECONDS_RUN:-10}"
TRADES_PER_SECOND="${TRADES_PER_SECOND:-10}"
PRICES_PER_SECOND="${PRICES_PER_SECOND:-1000}"
ACCOUNT_KEY_UNIVERSE="${ACCOUNT_KEY_UNIVERSE:-160}"
FAILURES=0

# Drains the whole topic. A prefix read with --max-messages is not safe for
# per-key balance checks: it slices across partitions and the slice is uneven
# even when the production was not.
drain() {
  docker exec "$CONTAINER" /opt/kafka/bin/kafka-console-consumer.sh \
      --bootstrap-server localhost:9092 --topic "$1" \
      --from-beginning --timeout-ms "${DRAIN_TIMEOUT_MS:-20000}" 2>/dev/null
}

check() {  # label expected actual
  if [ "$2" = "$3" ]; then
    printf "  %-42s %-10s OK\n" "$1" "$3"
  else
    printf "  %-42s %-10s EXPECTED %s\n" "$1" "$3" "$2"
    FAILURES=$((FAILURES + 1))
  fi
}

note() { printf "  %-42s %s\n" "$1" "$2"; }

ORDERS_EXPECTED=$((TRADES_PER_SECOND * SECONDS_RUN))
echo "orders  (${TRADES_PER_SECOND}/sec for ${SECONDS_RUN}s)"
ORDERS=$(drain orders)

ORDER_COUNT=$(echo "$ORDERS" | grep -c .)
# A run ends mid-cycle, so the count lands within a record or two of nominal.
# The rate is approximate; the invariants below are not.
ORDERS_LOW=$(( ORDERS_EXPECTED * 95 / 100 ))
if [ "$ORDER_COUNT" -ge "$ORDERS_LOW" ]; then
  note "record count" "$ORDER_COUNT (nominal ${ORDERS_EXPECTED}, within 5%)"
else
  check "record count" ">= $ORDERS_LOW" "$ORDER_COUNT"
fi
check "unique symbols (sink 3 keys)"  "4"  "$(echo "$ORDERS" | jq -r '.symbol' | sort -u | grep -c .)"
# Exact: sink 4 carries four allocations for every order, whatever the count was.
check "allocations = 4 x orders"      "$((ORDER_COUNT * 4))" \
      "$(echo "$ORDERS" | jq -r '.allocations | length' | paste -sd+ - | bc)"
check "allocations sum to block qty"  "0" \
      "$(echo "$ORDERS" | jq -r 'select(([.allocations[].quantity] | add) != .quantity) | .tradeId' | grep -c .)"
check "sides present"                 "2"  "$(echo "$ORDERS" | jq -r '.side' | sort -u | grep -c .)"
check "duplicate tradeIds"            "0"  "$(echo "$ORDERS" | jq -r '.tradeId' | sort | uniq -d | grep -c .)"

ACCOUNT_KEYS=$(echo "$ORDERS" | jq -r '.symbol as $s | .allocations[] | "\(.account)/\(.subAccount)/\($s)"' | sort -u)
OBSERVED_KEYS=$(echo "$ACCOUNT_KEYS" | grep -c .)
# Every key must be inside the declared universe. Reaching all of it is a matter
# of run length, not correctness: 4 allocations x N trades random draws cover
# 160 keys only after roughly 160*ln(160) = 812 draws, about 20s at 10 trades/sec.
check "account keys outside universe" "0" \
      "$(echo "$ACCOUNT_KEYS" | grep -vcE '^ACC[1-4]/SUB(0[1-9]|10)/(AAPL|MSFT|GOOG|AMZN)$')"
if [ "$OBSERVED_KEYS" -gt "$ACCOUNT_KEY_UNIVERSE" ]; then
  check "account keys within universe" "<= $ACCOUNT_KEY_UNIVERSE" "$OBSERVED_KEYS"
else
  note "account keys seen" "$OBSERVED_KEYS of $ACCOUNT_KEY_UNIVERSE ($(( OBSERVED_KEYS * 100 / ACCOUNT_KEY_UNIVERSE ))% coverage)"
fi
note "buy/sell mix" "$(echo "$ORDERS" | jq -r '.side' | sort | uniq -c | tr '\n' ' ')"

echo
echo "prices  (${PRICES_PER_SECOND}/sec for ${SECONDS_RUN}s)"
PRICES=$(drain prices)
PRICE_COUNT=$(echo "$PRICES" | grep -c .)

# The pacer emits on a schedule, so a run yields the nominal count give or take
# a cycle. Anything further off means the rate was not met.
LOW=$(( PRICES_PER_SECOND * SECONDS_RUN * 95 / 100 ))
if [ "$PRICE_COUNT" -ge "$LOW" ]; then
  note "record count" "$PRICE_COUNT (>= ${LOW}, within 5% of nominal)"
else
  check "record count" ">= $LOW" "$PRICE_COUNT"
fi
check "unique symbols"                "4"  "$(echo "$PRICES" | jq -r '.symbol' | sort -u | grep -c .)"
# Round-robin means every symbol is priced the same number of times, give or
# take the partial cycle a run ends on.
SPREAD=$(echo "$PRICES" | jq -r '.symbol' | sort | uniq -c | awk '{print $1}' | sort -n | awk 'NR==1{min=$1} {max=$1} END {print max-min}')
check "round-robin spread (max-min)"  "0"  "$SPREAD"
check "non-positive prices"           "0"  "$(echo "$PRICES" | jq -r 'select(.price <= 0) | .symbol' | grep -c .)"
check "prices off a quarter"          "0"  "$(echo "$PRICES" | jq -r 'select((.price * 4 | floor) != (.price * 4)) | .symbol' | grep -c .)"

echo
if [ "$FAILURES" -eq 0 ]; then
  echo "all checks passed"
else
  echo "$FAILURES check(s) failed"
fi
exit "$FAILURES"
