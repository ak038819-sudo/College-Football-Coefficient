#!/usr/bin/env python3
"""
Conference CoE 2.0 (CoE 2.0 rollup, spec section 16) -- built ALONGSIDE
CoE v1's own conference ratings (build_coefficients.py's
conference_ratings_by_season.csv, untouched) and alongside team-level
Game CoE 2.0 (build_hybrid_coefficients.py, hybrid_game_ratings).

Per spec: "Conference CoE asks: how has this conference performed
against the outside world?" -- so unlike team CoE (every game counts),
conference CoE only counts a team's EXTERNAL games: non-conference
regular-season games, plus ALL postseason games (bowls/playoffs), even
in the rare case a bowl happens to pair two teams from the same
conference -- postseason always counts as external per the spec,
regardless of the opponent's conference.

ConferenceCoE_{C,Y} = sum over teams i in C, external games g, of
Game CoE 2.0 (i, g). This is NOT wired into actual playoff bid
allocation (which still runs entirely on CoE v1, untouched) -- it's
built here for inspection/comparison purposes only, matching this
whole initiative's "everything additive, v1 keeps running" principle.

Usage:
    python src/build_conference_coe2.py [--db db/league.db]
"""
from __future__ import annotations

import argparse
import csv
import sqlite3
from collections import defaultdict
from pathlib import Path


SCHEMA = """
CREATE TABLE IF NOT EXISTS conference_coe2_by_season (
    conference TEXT NOT NULL,
    season_year INTEGER NOT NULL,
    conference_coe2 REAL NOT NULL,
    external_games_counted INTEGER NOT NULL,
    PRIMARY KEY (conference, season_year)
);
"""


def is_external_game(own_conf, opp_conf, game_phase: str) -> bool:
    """
    True if this game counts toward own_conf's Conference CoE 2.0: a
    non-conference regular-season game, or ANY postseason game (spec:
    ConferenceGameSet = Nonconference + Postseason -- postseason always
    counts as external, even in the rare case both teams share a
    conference, e.g. a same-conference bowl rematch).
    """
    is_postseason = game_phase != "regular"
    return is_postseason or opp_conf is None or opp_conf != own_conf


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--db", default="db/league.db")
    p.add_argument("--out", default="data/processed/conference_coe2_by_season.csv")
    args = p.parse_args()

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA)
    conn.execute("DELETE FROM conference_coe2_by_season")

    # Each team's own conference per season (for grouping) and, per game,
    # both teams' conferences (for the external-vs-conference test).
    membership = {
        (r["team_id"], r["season_year"]): r["conference_real"]
        for r in conn.execute("SELECT team_id, season_year, conference_real FROM team_membership_by_season")
    }

    rows = conn.execute(
        """
        SELECT hgr.team_id, hgr.game_coe, g.season_year, g.game_phase,
               g.home_team_id, g.away_team_id
        FROM hybrid_game_ratings hgr
        JOIN games g ON g.game_id = hgr.game_id
        WHERE hgr.game_coe IS NOT NULL
        """
    ).fetchall()

    conf_coe = defaultdict(float)
    conf_game_count = defaultdict(int)

    for r in rows:
        team_id, season = r["team_id"], r["season_year"]
        own_conf = membership.get((team_id, season))
        if own_conf is None:
            continue  # team has no conference this season (e.g. an Independent) -- no conference to credit

        opp_id = r["away_team_id"] if team_id == r["home_team_id"] else r["home_team_id"]
        opp_conf = membership.get((opp_id, season))

        if is_external_game(own_conf, opp_conf, r["game_phase"]):
            conf_coe[(own_conf, season)] += r["game_coe"]
            conf_game_count[(own_conf, season)] += 1

    conn.executemany(
        "INSERT INTO conference_coe2_by_season (conference, season_year, conference_coe2, external_games_counted) VALUES (?, ?, ?, ?)",
        [(conf, year, val, conf_game_count[(conf, year)]) for (conf, year), val in conf_coe.items()],
    )
    conn.commit()

    out_rows = sorted(
        [{"conference": c, "season_year": y, "conference_coe2": round(v, 6), "external_games_counted": conf_game_count[(c, y)]}
         for (c, y), v in conf_coe.items()],
        key=lambda r: (r["season_year"], -r["conference_coe2"]),
    )
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    with open(args.out, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["conference", "season_year", "conference_coe2", "external_games_counted"])
        w.writeheader()
        w.writerows(out_rows)

    conn.close()
    print(f"Wrote {len(out_rows)} conference-season CoE 2.0 rows to {args.out}")

    years = sorted(set(y for _, y in conf_coe))
    if years:
        latest = years[-1]
        print(f"\n{latest} conference CoE 2.0 ranking:")
        latest_rows = sorted([r for r in out_rows if r["season_year"] == latest], key=lambda r: -r["conference_coe2"])
        for i, r in enumerate(latest_rows, start=1):
            print(f"  {i}. {r['conference']:<20} {r['conference_coe2']:.3f}  ({r['external_games_counted']} external games)")


if __name__ == "__main__":
    main()
