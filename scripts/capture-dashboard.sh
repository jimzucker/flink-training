#!/usr/bin/env bash
# Renders the dashboard to a PNG, server-side.
#
# Grafana renders it itself rather than relying on a browser: the images for the
# final deck have to be reproducible, and a browser extension on the presenting
# machine is enough to stop panels drawing at all.
set -euo pipefail

cd "$(dirname "$0")/.."

OUT="${OUT:-docs/steps/step-06/dashboard.png}"
FROM="${FROM:-now-5m}"
TO="${TO:-now}"
WIDTH="${WIDTH:-1600}"
HEIGHT="${HEIGHT:-1400}"

mkdir -p "$(dirname "$OUT")"
curl -sf -o "$OUT" \
  "http://admin:admin@localhost:3000/render/d/flink-training/block-trade-pipeline?orgId=1&from=${FROM}&to=${TO}&width=${WIDTH}&height=${HEIGHT}&scale=1&kiosk"

echo "wrote $OUT ($(wc -c < "$OUT" | tr -d ' ') bytes)"
