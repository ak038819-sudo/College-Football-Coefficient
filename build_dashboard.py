#!/usr/bin/env python3
"""
Builds ui/dashboard.html -- a single self-contained, publishable HTML
file with all ratings/standings/playoff/bracket data embedded inline.

Runs src/export_dashboard_data.py to regenerate ui/dashboard_data.json,
then injects it into ui/dashboard_shell.html (the page template) in
place of the __DATA_JSON__ placeholder.

ui/dated_logo_assets.json (date-accurate historical team logos) is read
as-is, NOT regenerated here -- unlike ui/logo_assets.json (built fresh
every run from the small, committed ui/team_logos/ folder), the dated
logo set's source is a much larger (46MB raw) folder of historical PNGs
that isn't committed to the repo. Building it is a one-off step (run
src/build_dated_logo_assets.py --source "path/to/raw/logos" whenever
that source set changes) -- the resulting ~6MB encoded JSON IS committed
and just gets read here. If it doesn't exist yet, falls back to an
empty object so the dashboard still builds (every team logo just falls
back further, to the static logo_assets.json entry, then the monogram).

Usage:
    python build_dashboard.py --draw-seed 1
"""
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

SHELL_PATH = Path("ui/dashboard_shell.html")
DATA_PATH = Path("ui/dashboard_data.json")
LOGO_ASSETS_PATH = Path("ui/logo_assets.json")
DATED_LOGO_ASSETS_PATH = Path("ui/dated_logo_assets.json")
OUT_PATH = Path("ui/dashboard.html")


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
    subprocess.run(
        [sys.executable, "src/build_logo_assets.py", "--db", args.db, "--out", str(LOGO_ASSETS_PATH)],
        check=True,
    )

    shell = SHELL_PATH.read_text()
    data = DATA_PATH.read_text()
    logo_assets = LOGO_ASSETS_PATH.read_text()
    dated_logo_assets = DATED_LOGO_ASSETS_PATH.read_text() if DATED_LOGO_ASSETS_PATH.exists() else "{}"
    if not DATED_LOGO_ASSETS_PATH.exists():
        print(f"NOTE: {DATED_LOGO_ASSETS_PATH} not found -- dated logos will be empty for this build "
              "(run src/build_dated_logo_assets.py once against your raw historical-logo source folder)")

    final = (shell
             .replace("__DATA_JSON__", data)
             .replace("__LOGO_ASSETS_JSON__", logo_assets)
             .replace("__DATED_LOGO_ASSETS_JSON__", dated_logo_assets))
    OUT_PATH.write_text(final)

    print(f"\nBuilt {OUT_PATH} ({OUT_PATH.stat().st_size:,} bytes)")
    print("Open it directly in a browser, or publish it wherever you host static pages.")


if __name__ == "__main__":
    main()
