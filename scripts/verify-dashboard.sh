#!/usr/bin/env bash
# Checks the dashboard is provisioned, well formed, backed by data, and renders.
#
# Every panel query names a Flink operator, and those names come from .name()
# calls in the jobs. Renaming an operator silently blanks a panel: nothing fails,
# no error is logged, and the dashboard just stops saying anything. That is only
# discovered by looking at it, which during a demo is too late.
set -uo pipefail

cd "$(dirname "$0")/.." || exit 1

GRAFANA="${GRAFANA:-http://admin:admin@localhost:3000}"
PROM="${PROM:-http://localhost:9090}"
DASHBOARD="${DASHBOARD:-flink-training}"
EXPECT_SYMBOL_KEYS="${EXPECT_SYMBOL_KEYS:-4}"
EXPECT_ACCOUNT_KEYS="${EXPECT_ACCOUNT_KEYS:-16}"
FAILURES=0

check() {  # label expected actual
  if [ "$2" = "$3" ]; then
    printf "  %-44s %-10s OK\n" "$1" "$3"
  else
    printf "  %-44s %-10s EXPECTED %s\n" "$1" "$3" "$2"
    FAILURES=$((FAILURES + 1))
  fi
}

note() { printf "  %-44s %s\n" "$1" "$2"; }

echo "dashboard"
DASH=$(curl -sf "$GRAFANA/api/dashboards/uid/$DASHBOARD" | jq '.dashboard')
if [ -z "$DASH" ]; then
  echo "  not provisioned" >&2
  exit 1
fi
note "title" "$(echo "$DASH" | jq -r '.title')"
check "panels present" "true" "$([ "$(echo "$DASH" | jq '.panels | length')" -gt 0 ] && echo true || echo false)"

# Row panels stop everything below them drawing, which is invisible in the JSON.
check "row panels" "0" "$(echo "$DASH" | jq '[.panels[] | select(.type=="row")] | length')"

# An unrecognised reducer id renders an empty panel rather than complaining.
check "unknown stat reducers" "0" \
      "$(echo "$DASH" | jq '[.panels[] | select(.type=="stat") | .options.reduceOptions.calcs[]
                             | select(. as $c | ["lastNotNull","last","mean","max","min","sum","count","first","firstNotNull"]
                                                | index($c) | not)] | length')"
check "targets with no query" "0" \
      "$(echo "$DASH" | jq '[.panels[] | select(.type!="text" and .type!="row") | .targets[]?
                             | select((.expr // "") == "")] | length')"

echo
echo "every panel query returns data"
MISSING=0
while read -r expr; do
  [ -z "$expr" ] && continue
  n=$(curl -sfG "$PROM/api/v1/query" --data-urlencode "query=$expr" | jq '.data.result | length')
  if [ "${n:-0}" -eq 0 ]; then
    printf "  NO DATA  %s\n" "$expr"
    MISSING=$((MISSING + 1))
  fi
done < <(echo "$DASH" | jq -r '.panels[] | select(.type!="text" and .type!="row") | .targets[]?.expr // empty' | sort -u)
check "panel queries returning nothing" "0" "$MISSING"

echo
echo "the key counts the expected-output table states"
for pair in "aggregate_by_symbol:$EXPECT_SYMBOL_KEYS" "aggregate_by_account:$EXPECT_ACCOUNT_KEYS" \
            "market_value_by_symbol:$EXPECT_SYMBOL_KEYS" "market_value_by_account:$EXPECT_ACCOUNT_KEYS"; do
  op="${pair%%:*}"; want="${pair##*:}"
  got=$(curl -sfG "$PROM/api/v1/query" \
        --data-urlencode "query=sum(flink_taskmanager_job_task_operator_activeKeys{operator_name=\"$op\"})" \
        | jq -r '.data.result[0].value[1] // "none"')
  check "$op unique keys" "$want" "$got"
done

echo
echo "renders"
TMP=$(mktemp -t dashboard).png
if curl -sf -o "$TMP" "$GRAFANA/render/d/$DASHBOARD/x?orgId=1&from=now-5m&to=now&width=1200&height=1000&kiosk"; then
  SIZE=$(wc -c < "$TMP" | tr -d ' ')
  # A blank dashboard still renders, but compresses to almost nothing.
  if [ "$SIZE" -gt 20000 ]; then
    note "rendered png" "${SIZE} bytes"
  else
    check "rendered png bytes" "> 20000" "$SIZE"
  fi
else
  check "render" "http 200" "failed"
fi
rm -f "$TMP"

echo
if [ "$FAILURES" -eq 0 ]; then
  echo "dashboard checks passed"
else
  echo "$FAILURES dashboard check(s) failed"
fi
exit "$FAILURES"
