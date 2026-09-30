#!/bin/sh
# One line per sample: time, load average, then the 8 busiest processes on the Mac.
OUT=$1
while :; do
  t=$(date +%s)
  la=$(sysctl -n vm.loadavg | tr -d '{}')
  top -l 2 -s 1 -o cpu -n 8 -stats cpu,command 2>/dev/null | awk -v t="$t" -v la="$la" '
    /^%CPU/ {n++; next} n==2 && NF>=2 {printf "%s|%s|%s %s\n", t, la, $1, substr($0, index($0,$2))}' >> "$OUT"
  sleep 4
done
