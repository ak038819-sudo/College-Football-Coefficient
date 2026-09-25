#!/usr/bin/env python3
"""
Per-season game files + search index (data foundation step).

The foundation the Games section, game pages and global search build on:

  ui/data/games/<season>.js   every game of one season, completed AND scheduled,
                              oldest first, loaded only when that season is needed
  ui/data/search_index.js     teams (+ aliases), seasons and every game, compactly,
                              loaded on the first search
  ui/data/static_manifest.json  seasons, counts and content versions; embedded in
                              dashboard.html so the page knows what exists without
                              downloading it

Files are `window.__CFB__...=` scripts rather than bare JSON for the same reason as
team_pages.js: browsers block fetch() of local JSON when dashboard.html is opened
from disk, but a <script src> still works there and on GitHub Pages.

Every value is copied from an authoritative table; nothing is recomputed:
  completed games  pregame Elo, pregame expectation (home field included, exactly as
                   the Elo engine predicted), postgame Elo and change: elo_game_history.
                   Game CoE: hybrid_game_ratings (CoE 2.0; null before it exists).
  scheduled games  current ratings and prediction from predict_upcoming.build_upcoming
                   (the engine's own functions). No score, no postgame Elo, no Game CoE
                   -- a scheduled game can't have them.
Dates follow the database convention: the UTC date of CFBD's start time (a late
Saturday-night Eastern kickoff carries Sunday's date). Scheduled games also keep
their exact kickoff_utc so pages can show local time.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sqlite3
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from predict_upcoming import build_upcoming, elo_config  # noqa: E402

REPO = Path(__file__).resolve().parent.parent
OUT_DIR = REPO / "ui" / "data"
PHASE_CODE = {"regular": 0, "bowl": 1, "cfp": 2}
FIELDS = ["game_id", "week", "date", "kickoff_utc", "completed", "phase", "neutral", "ot",
          "home_id", "away_id", "home_score", "away_score",
          "home_pre_elo", "away_pre_elo", "p_home", "home_post_elo", "away_post_elo", "home_elo_change",
          "home_game_coe", "away_game_coe", "home_provisional", "away_provisional"]
SEARCH_GAME_FIELDS = ["game_id", "season", "home_id", "away_id"]


def _r(v, nd):
    return None if v is None else round(v, nd)


def build_season_payloads(conn: sqlite3.Connection, upcoming: dict) -> dict:
    """{season: payload}. Pure with respect to the database contents + the upcoming export."""
    elo = {(g, t): (pre, exp, chg, post) for g, t, pre, exp, chg, post in conn.execute(
        "SELECT game_id, team_id, pregame_elo, elo_expectation, elo_change, postgame_elo FROM elo_game_history")}
    has_hybrid = conn.execute("SELECT 1 FROM sqlite_master WHERE name='hybrid_game_ratings'").fetchone()
    coe = {(g, t): c for g, t, c in conn.execute("SELECT game_id, team_id, game_coe FROM hybrid_game_ratings")} \
        if has_hybrid else {}
    rows = defaultdict(list)

    for (gid, season, week, date, home, away, hs, as_, neutral, ot, phase) in conn.execute(
            """SELECT game_id, season_year, week, game_date, home_team_id, away_team_id, home_score, away_score,
                      neutral_site, went_ot, game_phase FROM games WHERE home_score IS NOT NULL"""):
        he, ae = elo.get((gid, home)), elo.get((gid, away))
        rows[season].append([
            gid, week, str(date)[:10], None, 1, PHASE_CODE.get(phase, 0), int(bool(neutral)), int(bool(ot)),
            home, away, hs, as_,
            _r(he[0], 1) if he else None, _r(ae[0], 1) if ae else None, _r(he[1], 4) if he else None,
            _r(he[3], 1) if he else None, _r(ae[3], 1) if ae else None, _r(he[2], 2) if he else None,
            _r(coe.get((gid, home)), 3), _r(coe.get((gid, away)), 3), 0, 0])

    for g in upcoming.get("games", []):
        gid, season, week, kickoff, _tbd, home, away, neutral, phase, h_elo, a_elo, p, h_prov, a_prov = g
        rows[season].append([
            gid, week, (kickoff or "1900-01-01")[:10], kickoff, 0, phase, neutral, 0, home, away, None, None,
            h_elo, a_elo, p, None, None, None, None, None, h_prov, a_prov])

    conf = defaultdict(dict)
    for tid, season, c in conn.execute("SELECT team_id, season_year, conference_real FROM team_membership_by_season"):
        conf[season][str(tid)] = c

    payloads = {}
    for season, rs in rows.items():
        rs.sort(key=lambda r: (r[2], r[3] or "", r[0]))            # date, kickoff, game_id
        teams = {str(t) for r in rs for t in (r[8], r[9])}
        payloads[season] = {"season": season, "fields": FIELDS,
                            "phase_codes": {v: k for k, v in PHASE_CODE.items()},
                            "game_coe_model": "CoE 2.0 (hybrid)",
                            "conferences": {t: conf[season].get(t) for t in sorted(teams, key=int)},
                            "games": rs}
    return payloads


def build_search_index(conn: sqlite3.Connection, payloads: dict) -> dict:
    names = dict(conn.execute("SELECT team_id, team_name FROM teams"))
    aliases = defaultdict(list)
    for alias, canonical in conn.execute("SELECT alias, team_name FROM team_aliases ORDER BY alias"):
        tid = next((t for t, n in names.items() if n == canonical), None)
        if tid is not None:
            aliases[tid].append(alias)
    from export_dashboard_data import slugify
    teams = [[tid, name, slugify(name), aliases.get(tid, [])] for tid, name in sorted(names.items(), key=lambda x: x[1])]
    games = [[r[0], season, r[8], r[9]] for season in sorted(payloads) for r in payloads[season]["games"]]
    return {"teams": teams, "seasons": sorted(payloads), "game_fields": SEARCH_GAME_FIELDS, "games": games}


def _write_js(path: Path, global_expr: str, payload) -> str:
    body = json.dumps(payload, separators=(",", ":"))
    path.write_text(f"{global_expr}={body};\n", encoding="utf-8")
    return hashlib.sha256(path.read_bytes()).hexdigest()[:10]


def export(conn: sqlite3.Connection, out_dir: Path = OUT_DIR) -> dict:
    payloads = build_season_payloads(conn, build_upcoming(conn, elo_config()))
    games_dir = out_dir / "games"
    if games_dir.exists():
        shutil.rmtree(games_dir)                                     # no orphan seasons
    games_dir.mkdir(parents=True)
    seasons = []
    for season in sorted(payloads):
        p = payloads[season]
        v = _write_js(games_dir / f"{season}.js", f"(window.__CFB_GAMES__=window.__CFB_GAMES__||{{}})[{season}]", p)
        seasons.append({"season": season, "completed": sum(r[4] for r in p["games"]),
                        "scheduled": sum(1 - r[4] for r in p["games"]), "src": f"data/games/{season}.js?v={v}"})
    sv = _write_js(out_dir / "search_index.js", "window.__CFB_SEARCH__", build_search_index(conn, payloads))
    manifest = {"seasons": seasons, "search_index": f"data/search_index.js?v={sv}", "game_fields": FIELDS}
    (out_dir / "static_manifest.json").write_text(json.dumps(manifest, separators=(",", ":")), encoding="utf-8")
    return manifest


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--db", default=str(REPO / "db" / "league.db"))
    args = p.parse_args()
    conn = sqlite3.connect(args.db)
    m = export(conn)
    conn.close()
    games_bytes = sum((OUT_DIR / "games" / f"{s['season']}.js").stat().st_size for s in m["seasons"])
    print(f"Season game files: {len(m['seasons'])} seasons, {sum(s['completed'] for s in m['seasons'])} completed + "
          f"{sum(s['scheduled'] for s in m['seasons'])} scheduled games ({games_bytes:,} bytes total); "
          f"search index {(OUT_DIR / 'search_index.js').stat().st_size:,} bytes")


if __name__ == "__main__":
    main()
