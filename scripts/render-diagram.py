#!/usr/bin/env python3
"""Renders docs/design/pipeline.svg to a PNG the deck can embed.

svglib draws this SVG faithfully except for one thing: the arrowheads are
applied through a CSS class (`.edge { marker-end:url(#arrow) }`) and svglib does
not resolve CSS-applied markers, so every edge comes out as a plain line. On a
flow diagram that is not a cosmetic loss.

So the markers are baked in first -- each edge gets an explicit polygon at its
end point, sized and positioned from the <marker> definition itself rather than
by eye. The edges are all orthogonal (M/H/V), which makes the direction of the
final segment unambiguous.

Then: svglib -> PDF (pure Python, no native cairo) -> sips -> PNG -> trim.
"""
import os
import re
import subprocess
import sys
import tempfile

SVG = "docs/design/pipeline.svg"
OUT = "docs/steps/step-13/pipeline.png"
SCALE = 3.0                      # raster at 3x so it stays crisp at slide width

# from the <marker> defs: viewBox 0 0 10 10, refX 9, refY 5, markerWidth 7,
# markerUnits defaults to strokeWidth, and .edge/.price are stroke-width 1.8
MARKER_W = 7 * 1.8               # 12.6 user units across the viewBox's 10
U = MARKER_W / 10.0
TIP = (10 - 9) * U               # how far the tip sits past the path end
BACK = 9 * U                     # how far the base sits behind it
HALF = 5 * U                     # half the base height


def parse_path(d):
    """Absolute M/H/V only -- returns (points, last_direction)."""
    toks = re.findall(r'([MHVLmhvl])|(-?\d+\.?\d*)', d)
    cmd, nums, pts, cur = None, [], [], None
    stream = []
    for c, n in toks:
        stream.append(('c', c) if c else ('n', float(n)))
    i = 0
    while i < len(stream):
        kind, val = stream[i]
        if kind == 'c':
            cmd = val
            i += 1
            continue
        if cmd == 'M':
            cur = (stream[i][1], stream[i + 1][1]); pts.append(cur); i += 2
        elif cmd == 'H':
            cur = (val, cur[1]); pts.append(cur); i += 1
        elif cmd == 'V':
            cur = (cur[0], val); pts.append(cur); i += 1
        elif cmd == 'L':
            cur = (stream[i][1], stream[i + 1][1]); pts.append(cur); i += 2
        else:
            i += 1
    if len(pts) < 2:
        return pts, None
    (x0, y0), (x1, y1) = pts[-2], pts[-1]
    if abs(x1 - x0) >= abs(y1 - y0):
        return pts, 'R' if x1 > x0 else 'L'
    return pts, 'D' if y1 > y0 else 'U'


def triangle(pt, direction, fill):
    x, y = pt
    if direction == 'R':
        p = [(x + TIP, y), (x - BACK, y - HALF), (x - BACK, y + HALF)]
    elif direction == 'L':
        p = [(x - TIP, y), (x + BACK, y - HALF), (x + BACK, y + HALF)]
    elif direction == 'D':
        p = [(x, y + TIP), (x - HALF, y - BACK), (x + HALF, y - BACK)]
    else:
        p = [(x, y - TIP), (x - HALF, y + BACK), (x + HALF, y + BACK)]
    pts = " ".join(f"{a:.2f},{b:.2f}" for a, b in p)
    return f'<polygon points="{pts}" fill="{fill}" stroke="none"/>'


def bake(svg):
    """Append an explicit arrowhead for every .edge / .price path."""
    heads, n = [], 0
    for m in re.finditer(r'<path\b[^>]*class="(edge|price)"[^>]*\bd="([^"]+)"[^>]*>', svg):
        cls, d = m.group(1), m.group(2)
        pts, direction = parse_path(d)
        if not direction:
            continue
        heads.append(triangle(pts[-1], direction, "#4a4a4a" if cls == "edge" else "#B07A12"))
        n += 1
    return svg.replace("</svg>", "\n  " + "\n  ".join(heads) + "\n</svg>"), n


def main():
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    os.chdir(root)
    svg = open(SVG, encoding="utf-8").read()
    baked, n = bake(svg)
    if not n:
        sys.exit("no .edge/.price paths found -- has the diagram changed?")

    from svglib.svglib import svg2rlg
    from reportlab.graphics import renderPDF
    from PIL import Image, ImageChops

    with tempfile.TemporaryDirectory() as tmp:
        tsvg = os.path.join(tmp, "baked.svg")
        open(tsvg, "w", encoding="utf-8").write(baked)
        drawing = svg2rlg(tsvg)
        drawing.scale(SCALE, SCALE)
        drawing.width *= SCALE
        drawing.height *= SCALE
        pdf = os.path.join(tmp, "d.pdf")
        renderPDF.drawToFile(drawing, pdf)
        png = os.path.join(tmp, "d.png")
        subprocess.run(["sips", "-s", "format", "png", pdf, "--out", png],
                       check=True, capture_output=True)
        im = Image.open(png).convert("RGB")
        bbox = ImageChops.difference(im, Image.new("RGB", im.size, (255, 255, 255))).getbbox()
        pad = 20
        box = (max(0, bbox[0] - pad), max(0, bbox[1] - pad),
               min(im.width, bbox[2] + pad), min(im.height, bbox[3] + pad))
        im.crop(box).save(OUT)
        w, h = im.crop(box).size
    print(f"  {OUT}  {w}x{h}  aspect {w/h:.2f}  ({n} arrowheads baked)")


if __name__ == "__main__":
    main()
