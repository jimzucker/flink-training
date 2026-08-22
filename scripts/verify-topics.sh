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
MIN_WINDOWS="${MIN_WINDOWS:-3}"
WINDOW_MS="${WINDOW_MS:-10000}"
FAILURES=0

if [ $((EXPECT_PRICES % SYMBOLS)) -ne 0 ]; then
  echo "EXPECT_PRICES ($EXPECT_PRICES) must be a multiple of SYMBOLS ($SYMBOLS)," >&2
  echo "or the round-robin ends mid-cycle and per-symbol counts cannot be exact." >&2
  exit 2
fi

# Topics are dumped once, up front, by a reader that knows the end offsets and
# reports when it could not reach them. A console consumer per topic could not
# tell "empty" from "did not finish in time" -- both return zero records -- so a
# slow machine produced failures that looked exactly like missing data.
DUMP_DIR="${DUMP_DIR:-target/topic-dump}"

dump_topics() {
  local jar=generators/target/generators.jar
  if [ ! -f "$jar" ]; then
    echo "build first:  mvn package -DskipTests" >&2
    exit 2
  fi
  rm -rf "$DUMP_DIR"
  java -cp "$jar" io.github.jimzucker.flinktraining.tools.TopicDump \
      "${BOOTSTRAP:-localhost:9092}" "$DUMP_DIR" "$@" \
      --deadline-seconds "${DUMP_DEADLINE_SECONDS:-120}"
}

drain() {
  cat "$DUMP_DIR/$1.jsonl" 2>/dev/null
}

check() {  # label expected actual
  if [ "$2" = "$3" ]; then
    printf "  %-40s %-12s OK\n" "$1" "$3"
  else
    printf "  %-40s %-12s EXPECTED %s\n" "$1" "$3" "$2"
    FAILURES=$((FAILURES + 1))
  fi
}

TOPICS="orders prices"
[ "${CHECK_POSITIONS:-1}" = "1" ] && TOPICS="$TOPICS positions-by-symbol positions-by-account"
[ "${CHECK_MARKET_VALUE:-0}" = "1" ] && TOPICS="$TOPICS mv-by-symbol mv-by-account"

echo "reading topics"
# shellcheck disable=SC2086
if ! dump_topics $TOPICS; then
  echo
  echo "a topic could not be read to its end offsets; the checks below would be" >&2
  echo "reporting a read failure as missing data, so stopping here instead." >&2
  exit 4
fi
echo

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

# ---------------------------------------------------------------- sinks 3 and 4
# Only checked when the positions job has been running; the input-side checks
# above stand on their own.
if [ "${CHECK_POSITIONS:-1}" = "1" ]; then
  echo
  echo "positions-by-symbol  (sink 3: one update per trade)"
  S3=$(drain positions-by-symbol)
  check "record count"               "$EXPECT_ORDERS" "$(echo "$S3" | grep -c .)"
  check "unique keys"                "$SYMBOLS" "$(echo "$S3" | jq -r '.key' | sort -u | grep -c .)"
  check "updateCount runs 1..n per key" "0" \
        "$(echo "$S3" | jq -r '"\(.key) \(.updateCount)"' | sort -k1,1 -k2,2n \
           | awk '{ if ($1 != k) { k = $1; want = 1 } ; if ($2 != want) bad++; want++ } END { print bad + 0 }')"

  echo
  echo "positions-by-account (sink 4: one update per allocation)"
  S4=$(drain positions-by-account)
  check "record count"               "$((EXPECT_ORDERS * 4))" "$(echo "$S4" | grep -c .)"
  check "unique keys"                "$ACCOUNT_KEYS" "$(echo "$S4" | jq -r '.key' | sort -u | grep -c .)"
  check "malformed keys"             "0" \
        "$(echo "$S4" | jq -r '.key' | grep -vcE '^ACC[1-4]/SUB1/(AAPL|MSFT|GOOG|AMZN)$')"

  # The reconciliation that matters: the same question answered two ways must
  # give the same answer, or the dashboard shows two different truths.
  echo
  echo "reconciliation  (final position per symbol, aggregated both ways)"
  BY_SYMBOL=$(echo "$S3" | jq -r '"\(.key) \(.updateCount) \(.quantity)"' | sort -k1,1 -k2,2n \
              | awk '{a[$1]=$3} END {for (k in a) printf "%s %d\n", k, a[k]}' | sort)
  BY_ACCOUNT=$(echo "$S4" | jq -r '"\(.symbol) \(.key) \(.updateCount) \(.quantity)"' | sort -k2,2 -k3,3n \
               | awk '{last[$2]=$4; sym[$2]=$1} END {for (k in last) t[sym[k]]+=last[k]; for (s in t) printf "%s %d\n", s, t[s]}' | sort)
  echo "$BY_SYMBOL" | while read -r sym qty; do printf "  %-6s %10s\n" "$sym" "$qty"; done
  check "symbol totals match account totals" "" "$(diff <(echo "$BY_SYMBOL") <(echo "$BY_ACCOUNT") | grep -c . | sed 's/^0$//')"
fi

# ---------------------------------------------------------------- sinks 5 and 6
if [ "${CHECK_MARKET_VALUE:-0}" = "1" ]; then
  PRICES_JSON=$(drain prices)

  check_market_value() {  # topic label keys
    local topic="$1" label="$2" keys="$3"
    echo
    echo "${topic}  (${label}: one per key per window)"
    local MV
    MV=$(drain "$topic")

    # How many windows close depends on where the run started relative to a
    # boundary -- a property of running on a real clock. The count is taken from
    # the data and only floored; everything inside a window stays exact.
    local windows
    windows=$(echo "$MV" | jq -r '.windowEnd' | sort -u | grep -c .)
    if [ "$windows" -ge "$MIN_WINDOWS" ]; then
      printf "  %-40s %-12s OK\n" "windows closed" "$windows"
    else
      check "windows closed" ">= $MIN_WINDOWS" "$windows"
    fi
    check "unique keys"  "$keys" "$(echo "$MV" | jq -r '.key' | sort -u | grep -c .)"

    # A key starts emitting at the first boundary after it is first seen, so keys
    # that appear late have fewer windows than keys that appear early. Counting
    # keys x windows would call that a failure. What must hold is that once a key
    # starts, it never skips a window and never stops early -- a quiet key still
    # holds a position and must keep reporting it.
    check "keys with a gap in their windows" "0" \
          "$(echo "$MV" | jq -r '"\(.key) \(.windowEnd)"' | sort -u | sort -k1,1 -k2,2n \
             | awk -v w="$WINDOW_MS" '{ if ($1 != k) { k = $1; prev = $2 }
                                        else { if ($2 != prev + w) bad++; prev = $2 } }
                                      END { print bad + 0 }')"
    local last_window keys_in_last
    last_window=$(echo "$MV" | jq -r '.windowEnd' | sort -n | tail -1)
    keys_in_last=$(echo "$MV" | jq -r --argjson b "$last_window" \
                   'select(.windowEnd == $b) | .key' | sort -u | grep -c .)
    check "keys missing from the last window" "0" "$((keys - keys_in_last))"
    # Records are then exactly one per key per window it participated in.
    check "record count" \
          "$(echo "$MV" | jq -r '"\(.key) \(.windowEnd)"' | sort -u | grep -c .)" \
          "$(echo "$MV" | grep -c .)"
    # Exactly one emission per key per window: a key emitting twice, or skipping a
    # window, is invisible in a total count.
    check "duplicate key/window pairs" "0" \
          "$(echo "$MV" | jq -r '"\(.key) \(.windowEnd)"' | sort | uniq -d | grep -c .)"
    check "marketValue != quantity x price" "0" \
          "$(echo "$MV" | jq -r 'select((.quantity * (.price|tonumber)) != (.marketValue|tonumber)) | .key' | grep -c .)"

    # The strongest check available: the price each window closed against,
    # recomputed independently from the raw price topic.
    local bad=0
    for b in $(echo "$MV" | jq -r '.windowEnd' | sort -u); do
      while read -r sym price; do
        want=$(echo "$PRICES_JSON" | jq -r --arg s "$sym" --argjson b "$b" \
               'select(.symbol==$s and .eventTime < $b) | "\(.eventTime) \(.price)"' \
               | sort -n | tail -1 | awk '{print $2}')
        [ "$want" = "$price" ] || bad=$((bad + 1))
      done < <(echo "$MV" | jq -r --argjson b "$b" 'select(.windowEnd==$b) | "\(.symbol) \(.price)"' | sort -u)
    done
    check "price at close differs from the price topic" "0" "$bad"
  }

  check_market_value mv-by-symbol  "sink 5" "$SYMBOLS"
  check_market_value mv-by-account "sink 6" "$ACCOUNT_KEYS"
fi

echo
if [ "$FAILURES" -eq 0 ]; then
  echo "all checks passed, with no tolerances"
else
  echo "$FAILURES check(s) failed"
fi
exit "$FAILURES"
