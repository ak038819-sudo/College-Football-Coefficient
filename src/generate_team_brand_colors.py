#!/usr/bin/env python3
"""Derive dark, readable scoreboard colors from the current team logo artwork.

The logo chip backgrounds serve a different purpose (contrast behind a logo),
so scoreboard bands use the most prominent chromatic logo color instead.
Run from the repository root after changing the logo manifest/assets.
"""
from __future__ import annotations

import colorsys
import json
from collections import Counter
from pathlib import Path

from PIL import Image

MANIFEST = Path("ui/logo_manifest.json")
OUTPUT = Path("ui/team_brand_colors.json")
OVERRIDES = {
    "Army": "#414529", "Navy": "#18314d", "Air Force": "#234567",
    "UCF": "#54472b", "Vanderbilt": "#554829", "Wake Forest": "#58472b",
    "Notre Dame": "#1d3859", "SMU": "#23436c", "Ole Miss": "#334a75",
}


def brand_color(path: Path) -> str:
    image = Image.open(path).convert("RGBA")
    image.thumbnail((100, 100))
    buckets: Counter[int] = Counter()
    for red, green, blue, alpha in image.get_flattened_data():
        hue, saturation, value = colorsys.rgb_to_hsv(red / 255, green / 255, blue / 255)
        if alpha < 160 or saturation < .27 or value < .16 or value > .94:
            continue
        buckets[int(hue * 36) % 36] += alpha
    if not buckets:
        return "#304250"
    dominant = buckets.most_common(1)[0][0] / 36
    red, green, blue = colorsys.hls_to_rgb(dominant, .22, .48)
    return f"#{round(red * 255):02x}{round(green * 255):02x}{round(blue * 255):02x}"


def main() -> None:
    entries = json.loads(MANIFEST.read_text(encoding="utf-8"))["teams"]
    colors = {}
    for name, eras in entries.items():
        current = next((era for era in eras if era["start"] <= 2026 and
                        (era["end"] is None or 2026 < era["end"])), eras[-1])
        path = Path("ui") / current["src"].split("?", 1)[0]
        colors[name] = OVERRIDES.get(name, brand_color(path))
    OUTPUT.write_text(json.dumps(colors, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")
    print(f"Wrote {len(colors)} team colors to {OUTPUT}")


if __name__ == "__main__":
    main()
