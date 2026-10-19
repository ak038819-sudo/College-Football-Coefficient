#!/usr/bin/env python3
"""
Static logo files (data foundation step).

Until now every logo was embedded in dashboard.html as base64 (about 7.4 MB of
an 8.4 MB page), so every visit downloaded every era of every logo. This
decodes the committed logo sets into individual image files that the browser
fetches only when a logo is actually on screen, plus a small manifest the page
embeds instead of the images.

Inputs (committed; produced by src/build_dated_logo_assets.py, and for the
non-FBS set by src/fetch_non_fbs_logos.py):
    ui/dated_logo_assets.json              {team_name: [{start, end, data, bg}, ...]}
    ui/dated_conference_logo_assets.json   {conference: [{start, end, data, bg}, ...]}
    ui/non_fbs_logo_assets.json            {opponent_name: [{start, end, data, bg}, ...]}

Outputs:
    ui/assets/logos/teams/<slug>/<start>-<end|now>.png
    ui/assets/logos/<kind>/<slug>/<start>-<end|now>-dark.png   (where the set has one)
    ui/assets/logos/conferences/<slug>/<start>-<end|now>.png
    ui/assets/logos/non_fbs/<slug>/<start>-<end|now>.png
    ui/logo_manifest.json   {"teams": {name: [{start, end, src, bg}]}, "conferences": {...},
                             "non_fbs": {...}}

A kind the page has no source file for is simply absent from the manifest; the
dashboard reads each section with a default of {}. The non-FBS marks are not
dated (ESPN's college CDN serves only today's logo), so each carries one entry
starting at year 0 -- "every season", since nothing reads that number as a date
-- and the dashboard lets a team's dated eras win wherever both hold a name.

The images are the exact bytes that were embedded (decoded, not re-encoded),
so nothing looks different. `src` is relative to ui/dashboard.html and carries
a short content hash (?v=...) so a changed image can never be served stale
from a browser or GitHub Pages cache. The output folder is rebuilt from
scratch each run, so a renamed or removed era can't leave an orphan file.
"""
from __future__ import annotations

import base64
import hashlib
import io
import json
import shutil
import sys
from pathlib import Path

from PIL import Image

sys.path.insert(0, str(Path(__file__).resolve().parent))
from export_dashboard_data import slugify  # noqa: E402  (same URL slugs as team pages)

REPO = Path(__file__).resolve().parent.parent
UI = REPO / "ui"
SOURCES = {"teams": UI / "dated_logo_assets.json",
           "conferences": UI / "dated_conference_logo_assets.json",
           "non_fbs": UI / "non_fbs_logo_assets.json"}
# The light-ink artwork for our own teams' CURRENT logo, fetched separately
# (src/fetch_team_dark_logos.py) because the dated set is rebuilt from a folder
# outside this repository. Keyed by team, carrying the era it belongs to: a
# redrawn logo gives the team a new era, and an overlay naming the old one is
# stale rather than wrong, so it is dropped and reported.
DARK_OVERLAYS = {"teams": UI / "dated_logo_dark_assets.json"}
OUT_ROOT = UI / "assets" / "logos"
MANIFEST = UI / "logo_manifest.json"
SHELL = UI / "dashboard_shell.html"
PNG_SIGNATURE = b"\x89PNG\r\n\x1a\n"

# The two page backgrounds a mark can land on, as --bg in ui/dashboard_shell.html.
# tests/test_logo_contrast.py holds these to the stylesheet, which is the only
# thing that makes them true.
DARK_BG = (0x0e, 0x15, 0x12)
LIGHT_BG = (0xf2, 0xf0, 0xe8)
# A mark is "dim" on a background when less than this share of its ink clears
# 3:1 against it. Mean luminance was tried first and reads a white mark with a
# navy outline as mid-grey, which is neither; the share of ink you can actually
# see is what decides whether the logo needs something behind it.
READABLE_CONTRAST = 3.0
READABLE_SHARE = 0.35
INK_ALPHA = 160


def _relative_luminance(rgb) -> float:
    def channel(value: int) -> float:
        v = value / 255
        return v / 12.92 if v <= 0.04045 else ((v + 0.055) / 1.055) ** 2.4
    r, g, b = (channel(v) for v in rgb)
    return 0.2126 * r + 0.7152 * g + 0.0722 * b


def _contrast(a: float, b: float) -> float:
    hi, lo = max(a, b), min(a, b)
    return (hi + 0.05) / (lo + 0.05)


def is_dim_on(raw: bytes, background) -> bool:
    """Would this mark disappear against that background?

    Sampled every other pixel in each direction, which is plenty on a 256px
    image and keeps a whole-set export to a second or so.
    """
    image = Image.open(io.BytesIO(raw)).convert("RGBA")
    pixels = image.load()
    bg = _relative_luminance(background)
    ink = readable = 0
    for y in range(0, image.height, 2):
        for x in range(0, image.width, 2):
            r, g, b, a = pixels[x, y]
            if a < INK_ALPHA:
                continue
            ink += 1
            if _contrast(_relative_luminance((r, g, b)), bg) >= READABLE_CONTRAST:
                readable += 1
    if not ink:
        return False
    return readable / ink < READABLE_SHARE


def decode_data_uri(uri: str) -> bytes:
    head, _, b64 = uri.partition(",")
    if not head.startswith("data:image/png;base64"):
        raise ValueError(f"expected a base64 PNG data URI, got {head[:40]!r}")
    raw = base64.b64decode(b64, validate=True)
    if not raw.startswith(PNG_SIGNATURE):
        raise ValueError("decoded logo is not a PNG")
    return raw


def export(sources: dict = SOURCES, out_root: Path = OUT_ROOT, manifest_path: Path = MANIFEST,
           url_prefix: str = "assets/logos", dark_overlays: dict = DARK_OVERLAYS) -> dict:
    """url_prefix: how dashboard.html (in ui/) addresses out_root; files land in out_root."""
    if out_root.exists():
        shutil.rmtree(out_root)
    manifest, stats, duplicates = {}, {}, []
    stale_overlays = []
    for kind, src_path in sources.items():
        if not src_path.exists():
            continue
        data = json.loads(src_path.read_text(encoding="utf-8"))
        overlay_path = (dark_overlays or {}).get(kind)
        overlay = (json.loads(overlay_path.read_text(encoding="utf-8"))
                   if overlay_path and overlay_path.exists() else {})
        for name, mark in overlay.items():
            entries = data.get(name) or []
            if not any((e["start"], e["end"]) == (mark["start"], mark["end"]) for e in entries):
                stale_overlays.append(f"{kind}: {name} {mark['start']}-{mark['end']}")
        kind_manifest, used_slugs, files = {}, {}, 0
        for name in sorted(data):
            slug = slugify(name)
            if slug in used_slugs:
                raise ValueError(f"{kind}: {name!r} and {used_slugs[slug]!r} would share folder {slug!r}")
            used_slugs[slug] = name
            entries, seen = [], {}
            for e in data[name]:
                era = (e["start"], e["end"])
                if era in seen:
                    # An exact repeat (same era, same image, same color) is dropped and
                    # reported; the page always used the first one anyway. Two DIFFERENT
                    # logos claiming one era is a real conflict: stop, don't guess.
                    if (seen[era]["data"], seen[era]["bg"], seen[era].get("dark")) != \
                            (e["data"], e["bg"], e.get("dark")):
                        raise ValueError(f"{kind}: {name!r} has two different logos for era {era}")
                    duplicates.append(f"{kind}: {name} {era[0]}-{era[1] if era[1] is not None else 'now'}")
                    continue
                seen[era] = e
                raw = decode_data_uri(e["data"])
                fname = f"{e['start']}-{e['end'] if e['end'] is not None else 'now'}.png"
                path = out_root / kind / slug / fname
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(raw)
                files += 1
                entry = {"start": e["start"], "end": e["end"],
                         "src": f"{url_prefix}/{kind}/{slug}/{fname}?v={hashlib.sha256(raw).hexdigest()[:10]}",
                         "bg": e["bg"]}
                # The provider id, where the source set carries one: it is how
                # the live scoreboard finds the mark of a school that has no
                # name in this project to look up (see the non-FBS set).
                if e.get("espn"):
                    entry["espn"] = e["espn"]
                # The light-ink artwork for dark pages. It is a second file
                # beside the colour mark rather than a second entry, because it
                # is the same era and the same logo -- only the page it is meant
                # to sit on differs. A set may carry it per entry (the non-FBS
                # one, fetched with the mark) or in an overlay keyed by era (our
                # own teams, whose set is rebuilt from outside this repository).
                dark_mark = overlay.get(name)
                dark_raw = None
                dark_uri = e.get("dark")
                if dark_uri is None and dark_mark and (dark_mark["start"], dark_mark["end"]) == era:
                    dark_uri = dark_mark["data"]
                if dark_uri:
                    dark_raw = decode_data_uri(dark_uri)
                    dark_name = f"{fname[:-len('.png')]}-dark.png"
                    (out_root / kind / slug / dark_name).write_bytes(dark_raw)
                    files += 1
                    entry["darkSrc"] = (f"{url_prefix}/{kind}/{slug}/{dark_name}"
                                        f"?v={hashlib.sha256(dark_raw).hexdigest()[:10]}")
                # Which page this mark would vanish on. The light theme always
                # shows the colour mark; the dark one shows the light-ink
                # artwork where there is one, so each is measured against the
                # background it will actually sit on. The page puts a quiet
                # plate behind a mark that is dim and nothing behind one that
                # is not -- it is what replaced a coloured disc behind all of
                # them.
                dim = [theme for theme, image, background in
                       (("dark", dark_raw if dark_uri else raw, DARK_BG),
                        ("light", raw, LIGHT_BG))
                       if is_dim_on(image, background)]
                if dim:
                    entry["dim"] = dim
                entries.append(entry)
            kind_manifest[name] = entries
        manifest[kind] = kind_manifest
        stats[kind] = {"names": len(kind_manifest), "files": files}
    manifest_path.write_text(json.dumps(manifest, separators=(",", ":"), sort_keys=True), encoding="utf-8")
    stats["duplicates_dropped"] = duplicates
    stats["stale_dark_overlays"] = stale_overlays
    return stats


def main() -> None:
    stats = export()
    size = MANIFEST.stat().st_size
    counts = ", ".join(f"{stats[kind]['files']} {kind} images ({stats[kind]['names']} names)"
                       for kind in SOURCES if kind in stats)
    print(f"Logo files: {counts}; manifest {size:,} bytes")
    if stats["stale_dark_overlays"]:
        print("  dark artwork dropped, the era it was fetched for is gone (re-run "
              "src/fetch_team_dark_logos.py): " + "; ".join(stats["stale_dark_overlays"]))
    if stats["duplicates_dropped"]:
        print("  exact duplicate logo entries skipped (same era, image and color): "
              + "; ".join(stats["duplicates_dropped"]))


if __name__ == "__main__":
    main()
