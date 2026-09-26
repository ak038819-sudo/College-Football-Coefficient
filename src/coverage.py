#!/usr/bin/env python3
"""
Data-coverage metadata (P1-08).

One place that answers "does this season have scores / kickoff times / advanced
stats / per-game efficiency / ratings, and if not, why?" -- so the site can say
"not collected for 1994" instead of rendering an absence as a zero.

Everything here is COUNTED from the database. The only hardcoded facts are the
source boundaries, and even those are read from the fetchers' own FIRST_SEASON
constants rather than restated, so a fetcher that changes its coverage window
cannot leave this module claiming the old one.

The result is embedded in ui/data/static_manifest.json, which the dashboard
already loads inline, so every page can check coverage synchronously without a
second request.
"""
from __future__ import annotations

import ast
import sqlite3
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent


def _first_season(module_name: str) -> int | None:
    """Read FIRST_SEASON out of a fetcher's syntax tree -- never imported (that
    would pull in its HTTP dependency) and never copied by hand."""
    path = REPO / "src" / f"{module_name}.py"
    if not path.exists():
        return None
    for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
        if (isinstance(node, ast.Assign) and len(node.targets) == 1
                and isinstance(node.targets[0], ast.Name) and node.targets[0].id == "FIRST_SEASON"):
            try:
                return int(ast.literal_eval(node.value))
            except ValueError:
                return None
    return None


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    return bool(conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)).fetchone())


def _counts(conn: sqlite3.Connection, sql: str) -> dict:
    return {int(season): int(n) for season, n in conn.execute(sql)}


# Each dimension the site can report on. `key` is what the UI looks up, `source`
# says where it comes from, and `absent` is the sentence shown when a season has
# none of it -- written once here rather than in each view.
DIMENSIONS = [
    {"key": "scores", "label": "Final scores",
     "source": "CollegeFootballData.com game results",
     "absent": "No completed games recorded for this season."},
    {"key": "kickoff", "label": "Kickoff times",
     "source": "CollegeFootballData.com start times",
     "absent": "Only the calendar date was collected for this season, not a clock time."},
    {"key": "team_advanced", "label": "Season efficiency",
     "source": "CollegeFootballData.com /stats/season/advanced",
     "absent": "Play-by-play efficiency was not collected for this season."},
    {"key": "game_advanced", "label": "Per-game efficiency",
     "source": "CollegeFootballData.com /stats/game/advanced",
     "absent": "Per-game efficiency has not been collected for this season."},
    {"key": "elo", "label": "Elo ratings",
     "source": "src/build_elo.py",
     "absent": "No Elo history for this season."},
    {"key": "coe2", "label": "CoE 2.0",
     "source": "src/build_hybrid_coefficients.py",
     "absent": "CoE 2.0 needs five prior seasons of CoE, which this season does not have."},
]


def build_coverage(conn: sqlite3.Connection) -> dict:
    """
    {"dimensions": [...], "seasons": {season: {key: [have, of]}}, "first": {key: season}}

    Each cell is [have, of]: how many of the season's units have that data, out
    of how many could. A view can therefore distinguish "none" from "some" from
    "all" without another query, which is what "support partial coverage without
    hiding the whole page" needs.
    """
    games = _counts(conn, "SELECT season_year, COUNT(*) FROM games GROUP BY 1")
    completed = _counts(conn, "SELECT season_year, COUNT(*) FROM games "
                              "WHERE home_score IS NOT NULL AND away_score IS NOT NULL GROUP BY 1")
    # Teams that actually played that season: the denominator for a per-team metric.
    teams = _counts(conn, """SELECT season_year, COUNT(*) FROM (
                                 SELECT season_year, home_team_id AS t FROM games
                                 UNION SELECT season_year, away_team_id FROM games) GROUP BY 1""")

    kickoff = {}
    if _table_exists(conn, "game_kickoffs"):
        cols = {r[1] for r in conn.execute("PRAGMA table_info(game_kickoffs)")}
        # A date-only row carries a midnight stamp that is not a time; it must not
        # be counted as kickoff coverage (see sql/kickoff_tables.sql).
        date_only = "k.date_only = 1" if "date_only" in cols else "0"
        kickoff = _counts(conn, f"""SELECT g.season_year, COUNT(*) FROM games g
                                    JOIN game_kickoffs k ON k.game_id = g.game_id
                                    WHERE k.kickoff_utc IS NOT NULL AND NOT ({date_only})
                                    GROUP BY 1""")

    # Restricted to teams that actually played that season. CFBD can carry a
    # team-season row for a program whose season our data has no games for --
    # New Mexico State in 2020, cancelled for COVID, is a real example -- and
    # counting it would make coverage read "128 of 127". The question this
    # fraction answers is "of the teams in this dataset for this season, how
    # many have stats?", so a row for a team we show no games for is not
    # coverage of anything.
    team_advanced = _counts(conn, """SELECT a.season_year, COUNT(*) FROM team_season_advanced a
                                     WHERE EXISTS (SELECT 1 FROM games g WHERE g.season_year = a.season_year
                                                   AND (g.home_team_id = a.team_id OR g.away_team_id = a.team_id))
                                     GROUP BY 1""") \
        if _table_exists(conn, "team_season_advanced") else {}
    game_advanced = _counts(conn, """SELECT season_year, COUNT(DISTINCT game_id) FROM game_team_advanced
                                     WHERE off_success_rate IS NOT NULL GROUP BY 1""") \
        if _table_exists(conn, "game_team_advanced") else {}
    elo = _counts(conn, """SELECT g.season_year, COUNT(DISTINCT e.game_id)
                           FROM elo_game_history e JOIN games g ON g.game_id = e.game_id GROUP BY 1""") \
        if _table_exists(conn, "elo_game_history") else {}
    coe2 = _counts(conn, """SELECT g.season_year, COUNT(DISTINCT h.game_id)
                            FROM hybrid_game_ratings h JOIN games g ON g.game_id = h.game_id GROUP BY 1""") \
        if _table_exists(conn, "hybrid_game_ratings") else {}

    seasons = {}
    for season in sorted(games):
        done, total, n_teams = completed.get(season, 0), games.get(season, 0), teams.get(season, 0)
        seasons[str(season)] = {
            "scores": [done, total],
            "kickoff": [kickoff.get(season, 0), total],
            "team_advanced": [team_advanced.get(season, 0), n_teams],
            "game_advanced": [game_advanced.get(season, 0), done],
            "elo": [elo.get(season, 0), done],
            "coe2": [coe2.get(season, 0), done],
        }

    # First season that actually has each dimension, alongside the source's own
    # documented start. They can differ: a source may cover a season this project
    # has simply not fetched yet, which is worth telling the reader apart.
    first = {}
    for d in DIMENSIONS:
        have = [int(s) for s, row in seasons.items() if row[d["key"]][0] > 0]
        first[d["key"]] = min(have) if have else None
    return {
        "dimensions": DIMENSIONS,
        "seasons": seasons,
        "first": first,
        "source_first": {"team_advanced": _first_season("fetch_cfbd_advanced"),
                         "game_advanced": _first_season("fetch_cfbd_game_advanced")},
    }


def main() -> None:
    import argparse
    import json
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--db", default=str(REPO / "db" / "league.db"))
    p.add_argument("--json", action="store_true", help="print the full payload instead of a table")
    args = p.parse_args()

    conn = sqlite3.connect(args.db)
    cov = build_coverage(conn)
    conn.close()

    if args.json:
        print(json.dumps(cov, indent=2))
        return
    keys = [d["key"] for d in cov["dimensions"]]
    print(f"{'season':>7}" + "".join(f"{k:>16}" for k in keys))
    for season, row in cov["seasons"].items():
        cells = []
        for k in keys:
            have, of = row[k]
            cells.append("-" if not of else f"{have}/{of}")
        print(f"{season:>7}" + "".join(f"{c:>16}" for c in cells))
    print("\nfirst season with data:", {k: v for k, v in cov["first"].items()})
    print("source coverage begins:", cov["source_first"])


if __name__ == "__main__":
    main()
