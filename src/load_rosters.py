#!/usr/bin/env python3
"""Load a committed roster snapshot into players / player_external_ids /
player_team_seasons.

Idempotent by construction. A CFBD athlete id already in player_external_ids
resolves to its existing player_id, and the season row's primary key is
(player_id, season_year, team_id) -- so re-running a season updates rows instead
of minting a second copy of anyone. Only the season being loaded is touched;
other seasons' rows are left alone, which is what preserves a transfer as two
team-seasons for one person.

What it refuses to do:
  - invent a team. A school this FBS-only database does not carry is counted and
    recorded in person_unresolved, not guessed at or created.
  - merge on a name. See person_identity for the resolution order.
  - fabricate bio values. Missing height, weight or hometown stays NULL; a zero
    is never written in place of an unknown.

Usage:
    python src/load_rosters.py data/raw/rosters/2026.json --db db/league.db
"""
from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path

import person_identity as identity

SOURCE = "cfbd"


def load_roster(conn: sqlite3.Connection, path: str | Path) -> dict:
    identity.apply_schema(conn)
    rows = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(rows, list):
        raise ValueError(f"{path}: roster snapshot must be a list")

    seasons = {int(r["season_year"]) for r in rows if r.get("season_year") is not None}
    if not seasons:
        raise ValueError(f"{path}: no season_year on any row")
    for season in seasons:
        conn.execute("DELETE FROM person_unresolved WHERE entity = 'player' AND context = 'roster' "
                     "AND season_year = ?", (season,))

    stats = {"loaded": 0, "created": 0, "matched": 0, "unknown_team": 0, "skipped": 0}

    # Resolution happens BEFORE the season's rows are deleted, and the rows to
    # write are staged until it is done. A row with no athlete id can only be
    # recognised by its existing player_team_seasons row (name + team + season +
    # position), so deleting first would hide the very evidence the match needs:
    # every run would then create another person and orphan the last one, which
    # is exactly what a reload of an id-less roster used to do.
    staged = []
    for row in rows:
        name = (row.get("name") or "").strip()
        season = row.get("season_year")
        if not name or season is None:
            stats["skipped"] += 1
            continue
        season = int(season)
        team_id = identity.resolve_team_id(conn, row.get("team"))
        if team_id is None:
            stats["unknown_team"] += 1
            identity.record_unresolved(conn, "player", SOURCE, "roster", name,
                                       "unknown team", season, row.get("team"))
            continue
        position = row.get("position") or None
        player_id, reason = identity.resolve_player(
            conn, SOURCE, name, external_id=row.get("athlete_id"), season_year=season,
            team_id=team_id, position=position, context="roster",
            first_name=row.get("first_name"), last_name=row.get("last_name"),
            hometown=row.get("hometown"), home_state=row.get("home_state"),
            height=row.get("height"), weight=row.get("weight"))
        if player_id is None:
            stats["skipped"] += 1
            continue
        stats["created" if reason.startswith("created") else "matched"] += 1
        staged.append((player_id, season, team_id, row.get("jersey"), position,
                       identity.class_year_label(row.get("class_year")), row.get("height"),
                       row.get("weight"), SOURCE, row.get("source_updated_at")))

    # This snapshot is the authority for the seasons it covers, so its own rows are
    # replaced wholesale. A player who left the roster between fetches must
    # disappear from that season, which an upsert alone would never achieve.
    for season in seasons:
        conn.execute("DELETE FROM player_team_seasons WHERE season_year = ? AND source = ?",
                     (season, SOURCE))
    for values in staged:
        conn.execute(
            "INSERT OR REPLACE INTO player_team_seasons (player_id, season_year, team_id, jersey, "
            "position, class_year, height, weight, source, source_updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", values)
        stats["loaded"] += 1

    identity.refresh_latest_seasons(conn)
    conn.commit()
    return stats


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("snapshot")
    p.add_argument("--db", default="db/league.db")
    a = p.parse_args()
    conn = sqlite3.connect(a.db)
    conn.execute("PRAGMA foreign_keys = ON")
    stats = load_roster(conn, a.snapshot)
    conn.close()
    print(f"{a.snapshot}: {stats['loaded']} player-seasons "
          f"({stats['created']} new people, {stats['matched']} existing)"
          + (f", {stats['unknown_team']} rows for schools outside this database"
             if stats["unknown_team"] else "")
          + (f", {stats['skipped']} unresolved" if stats["skipped"] else ""))


if __name__ == "__main__":
    main()
