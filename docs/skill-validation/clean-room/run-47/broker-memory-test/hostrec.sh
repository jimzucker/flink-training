#!/bin/sh
# host swap and load every 10 s; stops when the file STOP appears beside it
D=$(dirname "$0")
while [ ! -f "$D/STOP" ]; do
  sw=$(sysctl -n vm.swapusage | sed -E 's/.*used = ([0-9.]+)M.*/\1/')
  ld=$(sysctl -n vm.loadavg | awk '{print $2}')
  fr=$(memory_pressure 2>/dev/null | awk -F': ' '/free percentage/{print $2}')
  echo "$(date +%s) $(date +%H:%M:%S) swapMB=$sw load1=$ld memfree=$fr" >> "$D/host.log"
  sleep 10
done
