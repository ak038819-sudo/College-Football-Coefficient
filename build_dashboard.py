#!/usr/bin/env python3
"""
Builds ui/dashboard.html -- a single self-contained, publishable HTML
file with all ratings/standings/playoff/bracket data embedded inline.

Runs src/export_dashboard_data.py to regenerate ui/dashboard_data.json,
then injects it into ui/dashboard_shell.html (the page template) in
place of the __DATA_JSON__ placeholder.

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
OUT_PATH = Path("ui/dashboard.html")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--db", default="db/league.db")
    p.add_argument("--draw-seed", type=int, default=1)
    args = p.parse_args()

    subprocess.run(
        [sys.executable, "src/export_dashboard_data.py", "--db", args.db,
         "--draw-seed", str(args.draw_seed), "--out", str(DATA_PATH)],
        check=True,
    )
    subprocess.run(
        [sys.executable, "src/build_logo_assets.py", "--db", args.db, "--out", str(LOGO_ASSETS_PATH)],
        check=True,
    )

    shell = SHELL_PATH.read_text()
    data = DATA_PATH.read_text()
    logo_assets = LOGO_ASSETS_PATH.read_text()
    final = shell.replace("__DATA_JSON__", data).replace("__LOGO_ASSETS_JSON__", logo_assets)
    OUT_PATH.write_text(final)

    print(f"\nBuilt {OUT_PATH} ({OUT_PATH.stat().st_size:,} bytes)")
    print("Open it directly in a browser, or publish it wherever you host static pages.")


if __name__ == "__main__":
    main()
