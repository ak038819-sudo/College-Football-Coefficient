#!/usr/bin/env python3
"""Load a committed season-statistics snapshot into player_season_stats.

The identity rule, unchanged from the roster loader: a row attaches to a person
by the source's athlete id or not at all. There is no contextual fallback here
and there should never be one. A roster row with no id can still be recognised
by name AND team AND season AND position, because a roster describes who was
present; a statistic describes what somebody DID, so attaching it to the wrong
person puts a number on a page under someone else's name. A page with no
statistics is better than that.

Measured against the real 2025 snapshot: of 141,627 rows, 85,768 are stored for
8,819 people. 40,291 carry an athlete id with no roster row in this database and
15,568 belong to a person here but name a school outside it -- a player who has
since moved to an FCS programme still appears in this feed. Not one row was lost
for want of an identity: every unknown athlete id in 2025 is at a school this
FBS-only database does not carry, which is why both counts are recorded as
unresolved rather than asserted.

Idempotent: the snapshot is the whole season, so the season's rows for this
source are replaced wholesale. A statistic CFBD has since corrected must change
rather than accumulate beside the old value.

Usage:
    python src/load_player_season_stats.py data/raw/player_season_stats/2025.json
"""
from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path

import person_identity as identity

SOURCE = "cfbd"
CONTEXT = "season stats"


def load_season_stats(conn: sqlite3.Connection, path: str | Path) -> dict:
    identity.apply_schema(conn)
    rows = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(rows, list):
        raise ValueError(f"{path}: season stats snapshot must be a list")

    seasons = {int(r["season_year"]) for r in rows if r.get("season_year") is not None}
    if not seasons:
        raise ValueError(f"{path}: no season_year on any row")

    stats = {"loaded": 0, "people": 0, "unknown_athlete": 0, "unknown_team": 0, "skipped": 0}
    # Resolution happens before anything is deleted, for the same reason the
    # roster loader stages its rows: a failed resolution must not be able to
    # leave the season with fewer statistics than it had.
    staged = []
    unresolved = []
    people = set()
    for row in rows:
        name = (row.get("name") or "").strip()
        category = (row.get("category") or "").strip()
        stat_type = (row.get("stat_type") or "").strip()
        season = row.get("season_year")
        if not name or not category or not stat_type or season is None:
            stats["skipped"] += 1
            continue
        season = int(season)
        external_id = row.get("athlete_id")
        player_id = (identity.find_by_external_id(conn, SOURCE, str(external_id))
                     if external_id not in (None, "") else None)
        if player_id is None:
            stats["unknown_athlete"] += 1
            unresolved.append((name, "athlete id with no roster row in this database",
                               season, row.get("team")))
            continue
        team_id = identity.resolve_team_id(conn, row.get("team") or "")
        if team_id is None:
            stats["unknown_team"] += 1
            unresolved.append((name, "unknown team", season, row.get("team")))
            continue
        staged.append((player_id, season, team_id, category, stat_type, row.get("stat"), SOURCE))
        people.add(player_id)

    for season in sorted(seasons):
        conn.execute("DELETE FROM player_season_stats WHERE season_year = ? AND source = ?",
                     (season, SOURCE))
        conn.execute("DELETE FROM person_unresolved WHERE entity = 'player' AND context = ? "
                     "AND season_year = ?", (CONTEXT, season))
    conn.executemany(
        "INSERT OR REPLACE INTO player_season_stats (player_id, season_year, team_id, category, "
        "stat_type, stat, source) VALUES (?, ?, ?, ?, ?, ?, ?)", staged)
    stats["loaded"] = len(staged)
    stats["people"] = len(people)

    # Recorded per person rather than per row: one player with ten statistics at
    # a school outside this database is one fact, and writing it ten times would
    # make the review list unreadable.
    for name, reason, season, team in {(u[0], u[1], u[2], u[3]) for u in unresolved}:
        identity.record_unresolved(conn, "player", SOURCE, CONTEXT, name, reason, season, team)

    conn.commit()
    return stats


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("snapshot")
    p.add_argument("--db", default="db/league.db")
    a = p.parse_args()
    conn = sqlite3.connect(a.db)
    conn.execute("PRAGMA foreign_keys = ON")
    stats = load_season_stats(conn, a.snapshot)
    conn.close()
    print(f"{a.snapshot}: {stats['loaded']} statistics for {stats['people']} people"
          + (f", {stats['unknown_athlete']} rows for athletes with no roster row here"
             if stats["unknown_athlete"] else "")
          + (f", {stats['unknown_team']} rows for schools outside this database"
             if stats["unknown_team"] else "")
          + (f", {stats['skipped']} unusable rows" if stats["skipped"] else ""))


if __name__ == "__main__":
    main()
