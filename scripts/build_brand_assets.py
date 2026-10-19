#!/usr/bin/env python3
"""Render the PNG brand assets from ui/brand/mark.svg.

The mark is the SOURCE; every PNG here is derived from it. They are committed
rather than built on deploy because the deploy has no browser and because a
missing favicon or og:image is a visible defect on every page, not a data
staleness issue that the next rebuild would fix.

Re-run this after editing the mark, and commit what it writes:

    python3 scripts/build_brand_assets.py

It needs a Chromium. It looks for one on PATH and in the usual Playwright
location; pass --chromium to point it somewhere else. If no browser is
available the script says so and writes nothing -- it never leaves a
half-rendered asset behind, because a truncated PNG published to the site
looks exactly like a working one in git.
"""
from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BRAND = ROOT / "ui" / "brand"
MARK = BRAND / "mark.svg"

CANDIDATES = [
    "/opt/pw-browsers/chromium-1194/chrome-linux/chrome",
    "chromium",
    "chromium-browser",
    "google-chrome",
]

BG = "#0e1512"
INK = "#eef1ec"
AMBER = "#e8a33d"

# (filename, width, height, body-builder). One page per asset, each sized
# exactly so the screenshot needs no cropping.
SQUARE = """
<div style="width:{size}px;height:{size}px">{mark}</div>
"""

# A maskable icon is cropped to whatever shape the launcher wants, and the
# platforms only guarantee the middle 80% of the width (a circle inscribed in
# it). The mark already carries its own rounded-rectangle background, so here it
# is inset to 60% on a full-bleed field: the bars survive a circular crop, and
# the corners that get cut are background.
MASKABLE = """
<div style="width:{size}px;height:{size}px;display:flex;align-items:center;justify-content:center">
  <div style="width:{inner}px;height:{inner}px">{mark}</div>
</div>
"""

OG = """
<div class="card">
  <div class="mark">{mark}</div>
  <h1>Elo<span>cast</span></h1>
  <p>Real time sports intelligence. Elo and opponent-strength ratings,
     playoff seeding and bracket odds for 46 seasons of college football.</p>
</div>
"""

PAGE = """<!DOCTYPE html><html><head><meta charset="utf-8">
<link href="https://fonts.googleapis.com/css2?family=Graduate&family=IBM+Plex+Sans:wght@400;500&display=swap" rel="stylesheet">
<style>
  html,body {{ margin:0; padding:0; background:{bg}; }}
  svg {{ display:block; width:100%; height:100%; }}
  .card {{ width:1200px; height:630px; box-sizing:border-box; padding:96px 110px;
           display:flex; flex-direction:column; justify-content:center; gap:28px; }}
  .mark {{ width:132px; height:132px; }}
  h1 {{ font-family:'Graduate',Georgia,serif; font-size:86px; line-height:1;
        color:{ink}; margin:0; letter-spacing:.02em; }}
  h1 span {{ color:{amber}; }}
  p {{ font-family:'IBM Plex Sans',system-ui,sans-serif; font-size:30px;
       line-height:1.45; color:#a8b3ac; margin:0; max-width:900px; }}
</style></head><body>{body}</body></html>
"""


def find_chromium(explicit=None):
    if explicit:
        return explicit if Path(explicit).exists() else None
    for candidate in CANDIDATES:
        if Path(candidate).exists():
            return candidate
        found = shutil.which(candidate)
        if found:
            return found
    return None


def shoot(chromium, html, out, width, height, scale=1):
    """Screenshot one page to `out`. Raises if the browser wrote nothing."""
    with tempfile.TemporaryDirectory() as tmp:
        page = Path(tmp) / "page.html"
        page.write_text(html, encoding="utf-8")
        subprocess.run(
            [chromium, "--headless", "--disable-gpu", "--no-sandbox",
             "--hide-scrollbars", f"--force-device-scale-factor={scale}",
             # Fonts come from Google Fonts: without a time budget the
             # screenshot can be taken before they load, and the asset
             # silently ships in a fallback face.
             "--virtual-time-budget=8000",
             f"--window-size={width},{height}",
             f"--screenshot={out}", str(page)],
            check=True, capture_output=True)
    if not out.exists() or out.stat().st_size == 0:
        raise RuntimeError(f"{chromium} wrote no bytes for {out.name}")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--chromium", help="path to a Chromium binary")
    args = parser.parse_args(argv)

    chromium = find_chromium(args.chromium)
    if not chromium:
        print("ERROR: no Chromium found. Pass --chromium /path/to/chrome.")
        return 2
    mark = MARK.read_text(encoding="utf-8")

    page = PAGE.format(bg=BG, ink=INK, amber=AMBER,
                       body=SQUARE.format(size=180, mark=mark))
    shoot(chromium, page, BRAND / "apple-touch-icon.png", 180, 180)

    page = PAGE.format(bg=BG, ink=INK, amber=AMBER,
                       body=SQUARE.format(size=32, mark=mark))
    shoot(chromium, page, BRAND / "icon-32.png", 32, 32)

    # The home-screen install icons. 192 and 512 are the two sizes Android
    # asks for by name, and the maskable one is what keeps the mark intact
    # inside a launcher's own silhouette.
    for size in (192, 512):
        page = PAGE.format(bg=BG, ink=INK, amber=AMBER,
                           body=SQUARE.format(size=size, mark=mark))
        shoot(chromium, page, BRAND / f"icon-{size}.png", size, size)

    page = PAGE.format(bg=BG, ink=INK, amber=AMBER,
                       body=MASKABLE.format(size=512, inner=round(512 * 0.6), mark=mark))
    shoot(chromium, page, BRAND / "icon-maskable-512.png", 512, 512)

    page = PAGE.format(bg=BG, ink=INK, amber=AMBER, body=OG.format(mark=mark))
    shoot(chromium, page, BRAND / "og-image.png", 1200, 630)

    for name in ("apple-touch-icon.png", "icon-32.png", "icon-192.png",
                 "icon-512.png", "icon-maskable-512.png", "og-image.png"):
        path = BRAND / name
        print(f"Wrote {path.relative_to(ROOT)} ({path.stat().st_size} bytes)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
