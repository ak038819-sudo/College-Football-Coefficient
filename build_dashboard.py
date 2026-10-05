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
TEAM_BRAND_COLORS_PATH = Path("ui/team_brand_colors.json")
STATIC_MANIFEST_PATH = Path("ui/data/static_manifest.json")
NAVIGATION_PATH = Path("ui/navigation.js")
SEARCH_PATH = Path("ui/search.js")
STATS_PATH = Path("ui/stats.js")
GEOGRAPHY_PATH = Path("ui/geography.js")
LIVE_TEAMS_PATH = Path("ui/live_teams.js")


def render_from_exports() -> None:
    """Render the template from existing exports, without touching model data."""
    from src.export_team_identities import export
    export()
    required = [SHELL_PATH, DATA_PATH, TEAM_PAGES_PATH, CONFERENCE_PAGES_PATH, LOGO_MANIFEST_PATH,
                TEAM_BRAND_COLORS_PATH,
                STATIC_MANIFEST_PATH, NAVIGATION_PATH, SEARCH_PATH, STATS_PATH, GEOGRAPHY_PATH, LIVE_TEAMS_PATH]
    missing = [str(path) for path in required if not path.exists()]
    if missing:
        raise SystemExit("Missing dashboard inputs: " + ", ".join(missing) +
                         ". Run the normal build to generate exports first.")
    final = (SHELL_PATH.read_text(encoding="utf-8")
             .replace("__DATA_JSON__", DATA_PATH.read_text(encoding="utf-8"))
             .replace("__LOGO_MANIFEST_JSON__", LOGO_MANIFEST_PATH.read_text(encoding="utf-8"))
             .replace("__TEAM_BRAND_COLORS_JSON__", TEAM_BRAND_COLORS_PATH.read_text(encoding="utf-8"))
             .replace("__STATIC_MANIFEST_JSON__", STATIC_MANIFEST_PATH.read_text(encoding="utf-8"))
             .replace("__TEAM_PAGES_VERSION__", hashlib.sha256(TEAM_PAGES_PATH.read_bytes()).hexdigest()[:12])
             .replace("__CONFERENCE_PAGES_VERSION__", hashlib.sha256(CONFERENCE_PAGES_PATH.read_bytes()).hexdigest()[:12])
             .replace("__NAVIGATION_VERSION__", hashlib.sha256(NAVIGATION_PATH.read_bytes()).hexdigest()[:12])
             .replace("__LIVE_TEAMS_VERSION__", hashlib.sha256(LIVE_TEAMS_PATH.read_bytes()).hexdigest()[:12])
             .replace("__GEOGRAPHY_VERSION__", hashlib.sha256(GEOGRAPHY_PATH.read_bytes()).hexdigest()[:12])
             .replace("__STATS_VERSION__", hashlib.sha256(STATS_PATH.read_bytes()).hexdigest()[:12])
             .replace("__SEARCH_VERSION__", hashlib.sha256(SEARCH_PATH.read_bytes()).hexdigest()[:12]))
    OUT_PATH.write_text(final, encoding="utf-8")
    print(f"\nBuilt {OUT_PATH} ({OUT_PATH.stat().st_size:,} bytes)")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--db", default="db/league.db")
    p.add_argument("--draw-seed", type=int, default=1)
    p.add_argument("--sims", type=int, default=10000)
    # No default here on purpose. The bracket temperature is a fitted model
    # parameter that lives in config/model_config.json; a default repeated at
    # this layer would silently override the fitted one, which is exactly what
    # a hardcoded 6.0 here did after the fit landed. Left unset, the flag is
    # simply not passed and export_dashboard_data.py uses the fitted value.
    p.add_argument("--temperature", type=float, default=None)
    p.add_argument("--reuse-exports", action="store_true",
                   help="Preview UI changes using existing exports; do not rebuild the database or data.")
    args = p.parse_args()

    if args.reuse_exports:
        render_from_exports()
        return

    subprocess.run(
        [sys.executable, "src/export_dashboard_data.py", "--db", args.db,
         "--draw-seed", str(args.draw_seed), "--sims", str(args.sims),
         "--out", str(DATA_PATH)]
        + (["--temperature", str(args.temperature)] if args.temperature is not None else []),
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
    # Head-coach metrics against the Elo expectation (v0.1.1 phase 5). Here, not
    # in run_pipeline.py, because it reads elo_game_history: the pipeline runs
    # before build_elo.py, and a coach page exported ahead of the ratings would
    # show every season as unmeasured. Before export_static_data.py, which is
    # what exports the people pages that read these rows.
    subprocess.run([sys.executable, "src/build_coach_metrics.py", "--db", args.db], check=True)
    # Per-season game files + search index, loaded on demand (data foundation step).
    subprocess.run([sys.executable, "src/export_static_data.py", "--db", args.db], check=True)
    render_from_exports()
    print("Open ui/dashboard.html, or publish it together with its neighboring static assets.")


if __name__ == "__main__":
    main()
