#!/usr/bin/env python3
"""
Builds ui/dashboard.html -- a single self-contained, publishable HTML
file with all ratings/standings/playoff/bracket data embedded inline.

Runs src/export_dashboard_data.py to regenerate ui/dashboard_data.json,
then injects it into ui/dashboard_shell.html (the page template) in
place of the __DATA_JSON__ placeholder.

ui/dated_logo_assets.json (team logos) and
ui/dated_conference_logo_assets.json (conference logos) are read as-is,
NOT regenerated here -- both are built from raw historical-PNG source
folders too large to commit to the repo (build them once with
src/build_dated_logo_assets.py --kind teams|conferences whenever that
source set changes; the resulting encoded JSON files ARE committed and
just get read here). If either is missing, falls back to an empty
object so the dashboard still builds -- every logo just falls back
further, to the monogram badge (the old static single-logo system for
both teams and conferences has been fully retired; there's no other
fallback tier anymore).

Usage:
    python build_dashboard.py --draw-seed 1
"""
from __future__ import annotations

import argparse
import hashlib
import subprocess
import sys
from pathlib import Path

SHELL_PATH = Path("ui/dashboard_shell.html")
DATA_PATH = Path("ui/dashboard_data.json")
DATED_LOGO_ASSETS_PATH = Path("ui/dated_logo_assets.json")
DATED_CONFERENCE_LOGO_ASSETS_PATH = Path("ui/dated_conference_logo_assets.json")
OUT_PATH = Path("ui/dashboard.html")
TEAM_PAGES_PATH = Path("ui/data/team_pages.js")


def _read_or_empty(path: Path) -> str:
    if path.exists():
        return path.read_text(encoding="utf-8")
    print(f"NOTE: {path} not found -- its logos will be empty for this build "
          f"(run src/build_dated_logo_assets.py once against the matching raw source folder)")
    return "{}"


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--db", default="db/league.db")
    p.add_argument("--draw-seed", type=int, default=1)
    p.add_argument("--sims", type=int, default=10000)
    p.add_argument("--temperature", type=float, default=6.0)
    args = p.parse_args()

    subprocess.run(
        [sys.executable, "src/export_dashboard_data.py", "--db", args.db,
         "--draw-seed", str(args.draw_seed), "--sims", str(args.sims),
         "--temperature", str(args.temperature), "--out", str(DATA_PATH)],
        check=True,
    )
    # Team-page data (Milestone 2): loaded by the dashboard only when a team
    # page opens. Its content hash is stamped into the page so a browser or
    # GitHub Pages cache can never pair a new dashboard with an old data file.
    subprocess.run(
        [sys.executable, "src/export_team_pages.py", "--db", args.db, "--out", str(TEAM_PAGES_PATH)],
        check=True,
    )
    team_pages_version = hashlib.sha256(TEAM_PAGES_PATH.read_bytes()).hexdigest()[:12]

    # Explicit UTF-8: on Windows the default would be the legacy cp1252 codepage.
    shell = SHELL_PATH.read_text(encoding="utf-8")
    data = DATA_PATH.read_text(encoding="utf-8")
    dated_logo_assets = _read_or_empty(DATED_LOGO_ASSETS_PATH)
    dated_conference_logo_assets = _read_or_empty(DATED_CONFERENCE_LOGO_ASSETS_PATH)

    final = (shell
             .replace("__DATA_JSON__", data)
             .replace("__DATED_LOGO_ASSETS_JSON__", dated_logo_assets)
             .replace("__DATED_CONFERENCE_LOGO_ASSETS_JSON__", dated_conference_logo_assets)
             .replace("__TEAM_PAGES_VERSION__", team_pages_version))
    OUT_PATH.write_text(final, encoding="utf-8")

    print(f"\nBuilt {OUT_PATH} ({OUT_PATH.stat().st_size:,} bytes)")
    print("Open it directly in a browser, or publish it wherever you host static pages.")


if __name__ == "__main__":
    main()
