#!/usr/bin/env python3
"""Draws the scaling chart used as the article's lead image.

Scripted rather than hand-made so the picture cannot drift from the numbers:
the bar heights are computed from ORDERS straight out of
docs/steps/step-12/units.txt, and the ideal-linear line is computed from the
one-unit result rather than drawn by eye.

  .venv/bin/python scripts/article-chart.py
"""
import os
from PIL import Image, ImageDraw, ImageFont

OUT = "docs/img/scaling-linear.png"
S = 2                                    # 2x for a crisp upload
W, H = 1200 * S, 627 * S                 # LinkedIn's 1.91:1

# from docs/steps/step-12/units.txt -- one laptop run, 8 partitions throughout
CASES = [(1, 30505), (2, 65721), (4, 129056)]
STEPS = ["2.15x", "1.96x"]               # measured, between consecutive bars

INK   = (0x14, 0x22, 0x2E)
BODY  = (0x3B, 0x4A, 0x57)
MUTED = (0x76, 0x85, 0x91)
BAR   = (0x1B, 0x6B, 0x8A)
BAR_L = (0x8F, 0xB8, 0xC9)
RULE  = (0xD8, 0xDF, 0xE4)
IDEAL = (0xA8, 0x4B, 0x2A)
BG    = (0xFF, 0xFF, 0xFF)

F = "/System/Library/Fonts/Helvetica.ttc"
def font(sz, bold=False):
    return ImageFont.truetype(F, sz * S, index=1 if bold else 0)

im = Image.new("RGB", (W, H), BG)
d = ImageDraw.Draw(im)

d.text((60 * S, 44 * S), "One core in, one core's worth out", font=font(38, True), fill=INK)
d.text((60 * S, 96 * S),
       "Block trades to positions on a laptop with a single Kafka broker.",
       font=font(19), fill=MUTED)

BASE = int(468 * S)                      # baseline y
TOP  = int(176 * S)                      # y for the tallest bar
PLOT = BASE - TOP
FULL = 140000.0
def h(v): return int(PLOT * (v / FULL))

d.line([(60 * S, BASE), (1140 * S, BASE)], fill=RULE, width=int(2 * S))

BW = int(150 * S)
XS = [int(x * S) for x in (170, 430, 690)]

# ideal-linear reference, computed from the one-core result
per_unit = CASES[0][1]
pts = [(XS[i] + BW // 2, BASE - h(per_unit * u)) for i, (u, _) in enumerate(CASES)]
x, seg = pts[0][0], 0
while seg < len(pts) - 1:
    (x0, y0), (x1, y1) = pts[seg], pts[seg + 1]
    n = 26
    for k in range(0, n, 2):
        a = k / n; b = min((k + 1) / n, 1.0)
        d.line([(x0 + (x1 - x0) * a, y0 + (y1 - y0) * a),
                (x0 + (x1 - x0) * b, y0 + (y1 - y0) * b)], fill=IDEAL, width=int(3 * S))
    seg += 1
# in the clear space left of where the line starts, not over a bar
cx, cy = pts[0][0] - 176 * S, pts[0][1] - 74 * S
d.text((cx, cy), "perfect linear,", font=font(16, True), fill=IDEAL)
d.text((cx, cy + 21 * S), "from the 1-core result", font=font(16), fill=IDEAL)

for i, (units, val) in enumerate(CASES):
    bh = h(val)
    x0 = XS[i]
    d.rounded_rectangle([x0, BASE - bh, x0 + BW, BASE],
                        radius=int(5 * S), fill=BAR if i == len(CASES) - 1 else BAR_L)
    lab = f"{val:,}"
    w = d.textlength(lab, font=font(30, True))
    d.text((x0 + (BW - w) / 2, BASE - bh - 46 * S), lab, font=font(30, True), fill=INK)

    t = f"{units} core" + ("s" if units > 1 else "")
    w = d.textlength(t, font=font(24, True))
    d.text((x0 + (BW - w) / 2, BASE + 18 * S), t, font=font(24, True), fill=INK)

    if i:                                 # multiplier between this bar and the last
        mx = (XS[i - 1] + BW + x0) / 2
        m = STEPS[i - 1]
        w = d.textlength(m, font=font(26, True))
        d.text((mx - w / 2, BASE - bh - 46 * S), m, font=font(26, True), fill=BAR)

d.text((60 * S, 556 * S),
       "4x the resources, 4.23x the throughput.  129,056 orders/sec is 645,280 records/sec written.",
       font=font(20, True), fill=BODY)
d.text((60 * S, 590 * S),
       "Eight partitions held constant; each case drains a 50,000,000-order backlog with the producer stopped.",
       font=font(16), fill=MUTED)

os.makedirs(os.path.dirname(OUT), exist_ok=True)
im.save(OUT)
print(f"  {OUT}  {W}x{H}")
