#!/usr/bin/env python3
"""
Static logo files (data foundation step).

Until now every logo was embedded in dashboard.html as base64 (about 7.4 MB of
an 8.4 MB page), so every visit downloaded every era of every logo. This
decodes the committed logo sets into individual image files that the browser
fetches only when a logo is actually on screen, plus a small manifest the page
embeds instead of the images.

Inputs (committed; produced by src/build_dated_logo_assets.py):
    ui/dated_logo_assets.json              {team_name: [{start, end, data, bg}, ...]}
    ui/dated_conference_logo_assets.json   {conference: [{start, end, data, bg}, ...]}

Outputs:
    ui/assets/logos/teams/<slug>/<start>-<end|now>.png
    ui/assets/logos/conferences/<slug>/<start>-<end|now>.png
    ui/logo_manifest.json   {"teams": {name: [{start, end, src, bg}]}, "conferences": {...}}

The images are the exact bytes that were embedded (decoded, not re-encoded),
so nothing looks different. `src` is relative to ui/dashboard.html and carries
a short content hash (?v=...) so a changed image can never be served stale
from a browser or GitHub Pages cache. The output folder is rebuilt from
scratch each run, so a renamed or removed era can't leave an orphan file.
"""
from __future__ import annotations

import base64
import hashlib
import json
import shutil
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from export_dashboard_data import slugify  # noqa: E402  (same URL slugs as team pages)

REPO = Path(__file__).resolve().parent.parent
UI = REPO / "ui"
SOURCES = {"teams": UI / "dated_logo_assets.json", "conferences": UI / "dated_conference_logo_assets.json"}
OUT_ROOT = UI / "assets" / "logos"
MANIFEST = UI / "logo_manifest.json"
PNG_SIGNATURE = b"\x89PNG\r\n\x1a\n"


def decode_data_uri(uri: str) -> bytes:
    head, _, b64 = uri.partition(",")
    if not head.startswith("data:image/png;base64"):
        raise ValueError(f"expected a base64 PNG data URI, got {head[:40]!r}")
    raw = base64.b64decode(b64, validate=True)
    if not raw.startswith(PNG_SIGNATURE):
        raise ValueError("decoded logo is not a PNG")
    return raw


def export(sources: dict = SOURCES, out_root: Path = OUT_ROOT, manifest_path: Path = MANIFEST,
           url_prefix: str = "assets/logos") -> dict:
    """url_prefix: how dashboard.html (in ui/) addresses out_root; files land in out_root."""
    if out_root.exists():
        shutil.rmtree(out_root)
    manifest, stats, duplicates = {}, {}, []
    for kind, src_path in sources.items():
        data = json.loads(src_path.read_text(encoding="utf-8")) if src_path.exists() else {}
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
                    if (seen[era]["data"], seen[era]["bg"]) != (e["data"], e["bg"]):
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
                entries.append({"start": e["start"], "end": e["end"],
                                "src": f"{url_prefix}/{kind}/{slug}/{fname}?v={hashlib.sha256(raw).hexdigest()[:10]}",
                                "bg": e["bg"]})
            kind_manifest[name] = entries
        manifest[kind] = kind_manifest
        stats[kind] = {"names": len(kind_manifest), "files": files}
    manifest_path.write_text(json.dumps(manifest, separators=(",", ":"), sort_keys=True), encoding="utf-8")
    stats["duplicates_dropped"] = duplicates
    return stats


def main() -> None:
    stats = export()
    size = MANIFEST.stat().st_size
    print(f"Logo files: {stats['teams']['files']} team images ({stats['teams']['names']} teams), "
          f"{stats['conferences']['files']} conference images ({stats['conferences']['names']} conferences); "
          f"manifest {size:,} bytes")
    if stats["duplicates_dropped"]:
        print("  exact duplicate logo entries skipped (same era, image and color): "
              + "; ".join(stats["duplicates_dropped"]))


if __name__ == "__main__":
    main()
