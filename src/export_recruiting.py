#!/usr/bin/env python3
"""Publish the recruiting archive: class ranks, and the signees behind them.

WHY THIS EXISTS. `recruits` has held 81,262 signees since 2000 and
`recruiting_classes` 4,577 team classes, and the only thing either reached was
a hometown heat map on a COACH page, through `coach_recruits`. No team page
said what it signed, no board ranked the classes, and a signee's name was not
a link to the player he became -- although `recruits.athlete_id` is that join
and has been stored all along.

TWO PAYLOADS, BECAUSE THEY ARE READ DIFFERENTLY.

  * `classes.js` is every class this database can attribute to a team, all
    seasons at once. It is 93 KB, and one load serves the whole ranking board
    AND a team's season-by-season history, so neither costs a request per
    season.
  * `<season>.js` is one season's signees, keyed by team. 164 KB on average,
    and a reader loads one only when they open a class.

A CLASS RANK IS NATIONAL AND INCLUDES SCHOOLS THIS DATABASE DOES NOT CARRY.
1,148 of the 4,577 classes belong to Harvard, Eastern Washington, Northern
Iowa and the like, which load_recruiting.py deliberately stores with a NULL
team_id rather than dropping. They cannot appear on a board keyed by team, so
the board would read as "rank 30 is the 25th row" unless it says why. The
count travels in the payload for exactly that sentence; it is not computed in
the browser, where it would be a guess.

Usage:
    python src/export_recruiting.py --db db/league.db --out ui/data
"""
from __future__ import annotations

import argparse
import sqlite3
from collections import defaultdict
from pathlib import Path

# Pinned here, read by the browser from the payload itself, so a column added
# in the middle cannot silently shift every value one place to the left.
CLASS_FIELDS = ["team_id", "season_year", "class_rank", "points"]
SIGNEE_FIELDS = ["name", "position", "stars", "rating", "national_rank",
                 "athlete_id", "city", "state_province", "high_school"]


def build_classes(conn: sqlite3.Connection) -> dict:
    """Every attributable class, plus the per-season count of the ones that
    are not. Returns {} when the archive was never loaded."""
    if not conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' "
                        "AND name='recruiting_classes'").fetchone():
        return {}
    rows = [[int(team_id), int(season), rank, points]
            for team_id, season, rank, points in conn.execute(
                "SELECT team_id, season_year, class_rank, points FROM recruiting_classes "
                "WHERE team_id IS NOT NULL "
                # an unranked class sorts after the ranked ones, not before them
                "ORDER BY season_year, class_rank IS NULL, class_rank, team_id")]
    if not rows:
        return {}
    # The schools this database does not carry, by season. A board that ranks
    # 25 teams out of a national field of 30 has to be able to say so.
    unplaced = {int(season): int(n) for season, n in conn.execute(
        "SELECT season_year, COUNT(*) FROM recruiting_classes "
        "WHERE team_id IS NULL GROUP BY 1")}
    return {"fields": CLASS_FIELDS, "classes": rows, "unplaced": unplaced}


def build_signees(conn: sqlite3.Connection) -> dict:
    """{season: {team_id: [signee, ...]}}, each signee in SIGNEE_FIELDS order.

    A recruit with no team is left out here and only here: this payload answers
    "who did this team sign", and a recruit committed to a school this database
    does not carry is not an answer to it. The count of those is already on the
    classes payload, so nothing disappears without being accounted for.
    """
    if not conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' "
                        "AND name='recruits'").fetchone():
        return {}
    # An athlete_id is only a link if the player page it names exists. CFBD
    # carries ids for recruits this database has no player row for, and a name
    # that looks like a link and goes nowhere is worse than plain text.
    known = {int(r[0]) for r in conn.execute("SELECT player_id FROM players")} \
        if conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' "
                        "AND name='players'").fetchone() else set()
    out: dict[int, dict[int, list]] = defaultdict(lambda: defaultdict(list))
    for row in conn.execute(
            "SELECT season_year, committed_team_id, name, position, stars, rating, "
            "national_rank, athlete_id, city, state_province, high_school FROM recruits "
            "WHERE committed_team_id IS NOT NULL "
            "ORDER BY season_year, committed_team_id, national_rank, name"):
        season, team_id = int(row[0]), int(row[1])
        athlete = row[7]
        out[season][team_id].append([
            row[2], row[3], row[4],
            None if row[5] is None else round(float(row[5]), 4),
            row[6],
            int(athlete) if athlete is not None and int(athlete) in known else None,
            row[8], row[9], row[10]])
    return {season: {"season": season, "fields": SIGNEE_FIELDS,
                     "teams": {str(team): signees for team, signees in sorted(teams.items())}}
            for season, teams in out.items()}


def main(argv=None) -> None:
    import json
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default="db/league.db")
    parser.add_argument("--out", default="ui/data")
    args = parser.parse_args(argv)
    conn = sqlite3.connect(args.db)
    try:
        classes, signees = build_classes(conn), build_signees(conn)
    finally:
        conn.close()
    print(f"recruiting: {len(classes.get('classes', []))} classes, "
          f"{sum(len(t) for p in signees.values() for t in p['teams'].values())} signees "
          f"over {len(signees)} seasons")
    if args.out == "-":
        print(json.dumps({"classes": classes, "seasons": sorted(signees)})[:400])


if __name__ == "__main__":
    main()
