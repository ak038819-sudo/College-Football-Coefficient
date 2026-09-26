#!/usr/bin/env python3
"""
Build ui/dashboard.html from ui/dashboard_shell.html and the static exports.
Ratings and manifest data are embedded; navigation, logos, team histories,
conference histories and season game files remain separate assets served beside
the page.

Normal build: regenerate exports from an existing league database, export logo
files from the committed dated-logo sources, and render the dashboard.
UI preview: use --reuse-exports to render only, without recomputing model data.
Run from the repository root.

Usage:
    python build_dashboard.py --draw-seed 1
    python build_dashboard.py --reuse-exports
"""
from __future__ import annotations

import argparse
import hashlib
import subprocess
import sys
from pathlib import Path

SHELL_PATH = Path("ui/dashboard_shell.html")
DATA_PATH = Path("ui/dashboard_data.json")
OUT_PATH = Path("ui/dashboard.html")
TEAM_PAGES_PATH = Path("ui/data/team_pages.js")
CONFERENCE_PAGES_PATH = Path("ui/data/conference_pages.js")
LOGO_MANIFEST_PATH = Path("ui/logo_manifest.json")
STATIC_MANIFEST_PATH = Path("ui/data/static_manifest.json")
NAVIGATION_PATH = Path("ui/navigation.js")
SEARCH_PATH = Path("ui/search.js")


def render_from_exports() -> None:
    """Render the template from existing exports, without touching model data."""
    required = [SHELL_PATH, DATA_PATH, TEAM_PAGES_PATH, CONFERENCE_PAGES_PATH, LOGO_MANIFEST_PATH,
                STATIC_MANIFEST_PATH, NAVIGATION_PATH, SEARCH_PATH]
    missing = [str(path) for path in required if not path.exists()]
    if missing:
        raise SystemExit("Missing dashboard inputs: " + ", ".join(missing) +
                         ". Run the normal build to generate exports first.")
    final = (SHELL_PATH.read_text(encoding="utf-8")
             .replace("__DATA_JSON__", DATA_PATH.read_text(encoding="utf-8"))
             .replace("__LOGO_MANIFEST_JSON__", LOGO_MANIFEST_PATH.read_text(encoding="utf-8"))
             .replace("__STATIC_MANIFEST_JSON__", STATIC_MANIFEST_PATH.read_text(encoding="utf-8"))
             .replace("__TEAM_PAGES_VERSION__", hashlib.sha256(TEAM_PAGES_PATH.read_bytes()).hexdigest()[:12])
             .replace("__CONFERENCE_PAGES_VERSION__", hashlib.sha256(CONFERENCE_PAGES_PATH.read_bytes()).hexdigest()[:12])
             .replace("__NAVIGATION_VERSION__", hashlib.sha256(NAVIGATION_PATH.read_bytes()).hexdigest()[:12])
             .replace("__SEARCH_VERSION__", hashlib.sha256(SEARCH_PATH.read_bytes()).hexdigest()[:12]))
    OUT_PATH.write_text(final, encoding="utf-8")
    print(f"\nBuilt {OUT_PATH} ({OUT_PATH.stat().st_size:,} bytes)")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--db", default="db/league.db")
    p.add_argument("--draw-seed", type=int, default=1)
    p.add_argument("--sims", type=int, default=10000)
    p.add_argument("--temperature", type=float, default=6.0)
    p.add_argument("--reuse-exports", action="store_true",
                   help="Preview UI changes using existing exports; do not rebuild the database or data.")
    args = p.parse_args()

    if args.reuse_exports:
        render_from_exports()
        return

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
    # Conference-page data (Milestone E): members, historical CoE, external
    # performance. Loaded only when a conference page opens, and content-hashed
    # into the page for the same cache-pairing reason as team_pages.js.
    subprocess.run(
        [sys.executable, "src/export_conference_pages.py", "--db", args.db, "--out", str(CONFERENCE_PAGES_PATH)],
        check=True,
    )
    # Logos as static files + a small manifest (data foundation step) instead of
    # ~7 MB of base64 embedded in the page.
    subprocess.run([sys.executable, "src/export_logo_files.py"], check=True)
    # Per-season game files + search index, loaded on demand (data foundation step).
    subprocess.run([sys.executable, "src/export_static_data.py", "--db", args.db], check=True)
    render_from_exports()
    print("Open ui/dashboard.html, or publish it together with its neighboring static assets.")


if __name__ == "__main__":
    main()
