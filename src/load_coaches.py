#!/usr/bin/env python3
"""Load the committed coaching snapshot into coaches / coach_external_ids /
coach_tenures.

CFBD's /coaches feed carries no coach identifier, so identity needs a key this
loader derives rather than one the source supplies:

    cfbd:<normalized name>|<hire date>

and, only when that key collides inside one snapshot, with the record's first
season appended. Two different people who share a name AND a hire date are
otherwise indistinguishable in this feed; appending the first season keeps them
two people, and the collision is recorded in person_unresolved so the pair can
be looked at rather than trusted. The key lives in coach_external_ids (not in
code) so a source with real coach ids can be added beside it later.

Head coaches only. Nothing here describes coordinators or assistants.

Idempotent: the key is deterministic, and coach_tenures is keyed on
(coach_id, team_id, season_year), so re-running the same snapshot updates the
same rows.

Usage:
    python src/load_coaches.py data/raw/coaches/coaches.json --db db/league.db
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import sqlite3
from pathlib import Path

import person_identity as identity

SOURCE = "cfbd"
ROLE = "head coach"


def coach_key(record: dict) -> str:
    return f"{identity.normalize_name(record.get('name', ''))}|{record.get('hire_date') or ''}"


def _now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _upsert_coach(conn: sqlite3.Connection, record: dict, external_id: str) -> int:
    row = conn.execute("SELECT coach_id FROM coach_external_ids WHERE source = ? AND external_id = ?",
                       (SOURCE, external_id)).fetchone()
    now = _now()
    first, last = identity.split_name(record.get("first_name"), record.get("last_name"),
                                      record.get("name", ""))
    if row:
        coach_id = int(row[0])
        conn.execute("UPDATE coaches SET display_name = ?, first_name = ?, last_name = ?, "
                     "hire_date = ?, updated_at = ? WHERE coach_id = ?",
                     (record["name"], first, last, record.get("hire_date"), now, coach_id))
        return coach_id
    cur = conn.execute("INSERT INTO coaches (display_name, first_name, last_name, hire_date, "
                       "created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
                       (record["name"], first, last, record.get("hire_date"), now, now))
    coach_id = int(cur.lastrowid)
    conn.execute("INSERT INTO coach_external_ids (coach_id, source, external_id, confidence, is_primary) "
                 "VALUES (?, ?, ?, ?, 1)", (coach_id, SOURCE, external_id, "derived key"))
    return coach_id


def load_coaches(conn: sqlite3.Connection, path: str | Path) -> dict:
    identity.apply_schema(conn)
    records = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(records, list):
        raise ValueError(f"{path}: coaching snapshot must be a list")

    # The snapshot is the whole coaching history, so its own rows are replaced
    # wholesale: a season CFBD has corrected must change, not accumulate.
    conn.execute("DELETE FROM coach_tenures WHERE source = ?", (SOURCE,))
    conn.execute("DELETE FROM person_unresolved WHERE entity = 'coach' AND context = 'coaches'")

    stats = {"coaches": 0, "tenures": 0, "unknown_team": 0, "collisions": 0, "skipped": 0}
    used: set[str] = set()
    for record in records:
        name = (record.get("name") or "").strip()
        seasons = record.get("seasons") or []
        if not name or not seasons:
            stats["skipped"] += 1
            continue
        key = coach_key(record)
        if key in used:
            # Same name and hire date as a record already loaded. Keeping them
            # separate is the conservative reading: merging would blend two
            # careers into one page, which no later correction could detect.
            first_season = min(int(s["year"]) for s in seasons if s.get("year") is not None)
            key = f"{key}|{first_season}"
            stats["collisions"] += 1
            identity.record_unresolved(conn, "coach", SOURCE, "coaches", name,
                                       "same name and hire date as another coach",
                                       first_season)
        used.add(key)
        coach_id = _upsert_coach(conn, {**record, "name": name}, f"{SOURCE}:{key}")
        stats["coaches"] += 1
        for season in seasons:
            year, school = season.get("year"), season.get("school")
            if year is None or not school:
                continue
            team_id = identity.resolve_team_id(conn, school)
            if team_id is None:
                stats["unknown_team"] += 1
                identity.record_unresolved(conn, "coach", SOURCE, "coaches", name,
                                           "unknown team", int(year), school)
                continue
            conn.execute(
                "INSERT OR REPLACE INTO coach_tenures (coach_id, team_id, season_year, role, games, "
                "wins, losses, ties, preseason_rank, postseason_rank, source, source_team) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (coach_id, team_id, int(year), ROLE, season.get("games"), season.get("wins"),
                 season.get("losses"), season.get("ties"), season.get("preseason_rank"),
                 season.get("postseason_rank"), SOURCE, school))
            stats["tenures"] += 1

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
    stats = load_coaches(conn, a.snapshot)
    conn.close()
    print(f"{a.snapshot}: {stats['coaches']} head coach"
          f"{'' if stats['coaches'] == 1 else 'es'}, {stats['tenures']} coach-seasons"
          + (f", {stats['unknown_team']} seasons at schools outside this database"
             if stats["unknown_team"] else "")
          + (f", {stats['collisions']} same-name collisions kept separate"
             if stats["collisions"] else ""))


if __name__ == "__main__":
    main()
