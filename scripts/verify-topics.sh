#!/usr/bin/env bash
# Checks what is actually on the topics against the expected-output table.
#
# Every check here is exact. That is only possible because the run being verified
# is bounded by record count rather than by elapsed time: "emit 100 trades"
# always emits 100, whereas "run for ten seconds" lands on 100 or 101 depending
# on where the stop falls between pacer ticks. Verifying a duration-bounded run
# would force a tolerance on every count, and a tolerance is a place a real
# fault can hide.
#
# Use scripts/verify-run.sh to produce a run this can verify.
set -uo pipefail

CONTAINER="${CONTAINER:-ft-kafka}"
EXPECT_ORDERS="${EXPECT_ORDERS:-100}"
EXPECT_PRICES="${EXPECT_PRICES:-400}"
SYMBOLS="${SYMBOLS:-4}"
ACCOUNT_KEYS="${ACCOUNT_KEYS:-16}"
FAILURES=0

if [ $((EXPECT_PRICES % SYMBOLS)) -ne 0 ]; then
  echo "EXPECT_PRICES ($EXPECT_PRICES) must be a multiple of SYMBOLS ($SYMBOLS)," >&2
  echo "or the round-robin ends mid-cycle and per-symbol counts cannot be exact." >&2
  exit 2
fi

# Drains the whole topic. A prefix read with --max-messages is not safe: it
# slices across partitions, and the slice is uneven even when production was not.
drain() {
  docker exec "$CONTAINER" /opt/kafka/bin/kafka-console-consumer.sh \
      --bootstrap-server localhost:9092 --topic "$1" \
      --from-beginning --timeout-ms "${DRAIN_TIMEOUT_MS:-20000}" 2>/dev/null
}

check() {  # label expected actual
  if [ "$2" = "$3" ]; then
    printf "  %-40s %-12s OK\n" "$1" "$3"
  else
    printf "  %-40s %-12s EXPECTED %s\n" "$1" "$3" "$2"
    FAILURES=$((FAILURES + 1))
  fi
}

echo "orders  (expecting exactly ${EXPECT_ORDERS})"
ORDERS=$(drain orders)

check "record count"                 "$EXPECT_ORDERS" "$(echo "$ORDERS" | grep -c .)"
check "unique symbols (sink 3 keys)" "$SYMBOLS" "$(echo "$ORDERS" | jq -r '.symbol' | sort -u | grep -c .)"
check "allocations (sink 4 rate)"    "$((EXPECT_ORDERS * 4))" \
      "$(echo "$ORDERS" | jq -r '.allocations | length' | paste -sd+ - | bc)"
check "allocations sum to block qty" "0" \
      "$(echo "$ORDERS" | jq -r 'select(([.allocations[].quantity] | add) != .quantity) | .tradeId' | grep -c .)"
check "unique account keys (sink 4)" "$ACCOUNT_KEYS" \
      "$(echo "$ORDERS" | jq -r '.symbol as $s | .allocations[] | "\(.account)/\(.subAccount)/\($s)"' | sort -u | grep -c .)"
check "malformed account keys"       "0" \
      "$(echo "$ORDERS" | jq -r '.symbol as $s | .allocations[] | "\(.account)/\(.subAccount)/\($s)"' \
         | grep -vcE '^ACC[1-4]/SUB1/(AAPL|MSFT|GOOG|AMZN)$')"
check "sides present"                "2"  "$(echo "$ORDERS" | jq -r '.side' | sort -u | grep -c .)"
check "duplicate tradeIds"           "0"  "$(echo "$ORDERS" | jq -r '.tradeId' | sort | uniq -d | grep -c .)"
check "missing tradeIds"             "0" \
      "$(comm -23 <(seq 0 $((EXPECT_ORDERS - 1)) | xargs -I{} printf 'T%09d\n' {} | sort) \
                  <(echo "$ORDERS" | jq -r '.tradeId' | sort) | grep -c .)"
printf "  %-40s %s\n" "buy/sell mix" "$(echo "$ORDERS" | jq -r '.side' | sort | uniq -c | tr '\n' ' ')"

echo
echo "prices  (expecting exactly ${EXPECT_PRICES})"
PRICES=$(drain prices)

check "record count"                 "$EXPECT_PRICES" "$(echo "$PRICES" | grep -c .)"
check "unique symbols"               "$SYMBOLS" "$(echo "$PRICES" | jq -r '.symbol' | sort -u | grep -c .)"
# Exact: a whole number of round-robin cycles gives every symbol the same count.
check "prices per symbol"            "$((EXPECT_PRICES / SYMBOLS))" \
      "$(echo "$PRICES" | jq -r '.symbol' | sort | uniq -c | awk '{print $1}' | sort -u | paste -sd, -)"
check "non-positive prices"          "0"  "$(echo "$PRICES" | jq -r 'select(.price <= 0) | .symbol' | grep -c .)"
check "prices off a quarter"         "0"  "$(echo "$PRICES" | jq -r 'select((.price * 4 | floor) != (.price * 4)) | .symbol' | grep -c .)"

echo
if [ "$FAILURES" -eq 0 ]; then
  echo "all checks passed, with no tolerances"
else
  echo "$FAILURES check(s) failed"
fi
exit "$FAILURES"
