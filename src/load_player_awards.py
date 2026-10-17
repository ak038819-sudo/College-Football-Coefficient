#!/usr/bin/env python3
"""
Load data/raw/awards/<year>.json.gz into awards and player_awards.

Separate from the fetch on purpose. The fetch is a network call that should not
have to be repeated; what goes wrong here is IDENTITY -- turning "Travis Hunter,
Colorado" into one of this project's 99,813 people -- and that must be fixable
against the committed archive rather than against ESPN.

How a winner is resolved, in order:

  1. an ESPN athlete id this project has already linked to a person. The first
     successful contextual match LINKS the id, so every later run of every
     ingestion resolves that person by id instead of by name.
  2. name + team + season, through the same find_by_context the roster loader
     uses, which requires exactly ONE candidate. Two players of one name on one
     roster stay two people; the award is recorded unresolved rather than
     attached to a coin flip.

Nothing is created. An award winner with no roster row in this project is kept
as a name, with `resolution` saying why, and the page shows the name without a
link. That is the usual case before 2009, where the roster archive begins, and
for every coach-of-the-year award, whose winner is not a player at all.

Usage:
    python src/load_player_awards.py data/raw/awards/2024.json.gz --db db/league.db
"""
from __future__ import annotations

import argparse
import gzip
import json
import re
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import person_identity as pid

SCHEMA = Path(__file__).resolve().parent.parent / "sql" / "award_tables.sql"

# An award whose name says its winner is a coach. Their names resolve against
# the coaching tables, not the roster, so they are recorded with no player_id
# and a resolution that says so rather than being reported as a failed match.
COACH_AWARD = re.compile(r"\bcoach\b", re.IGNORECASE)


def apply_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA.read_text(encoding="utf-8"))


def award_key(name: str) -> str:
    """A stable key from an award's name.

    Derived from the name rather than ESPN's id so that a renumbering on their
    side cannot split one trophy into two rows, and so the key is readable in a
    URL and a test.
    """
    slug = re.sub(r"[^a-z0-9]+", "-", (name or "").strip().lower()).strip("-")
    return slug


def read_season(path: Path) -> dict:
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rt", encoding="utf-8") as fh:
        return json.load(fh)


def resolve_winner(conn: sqlite3.Connection, winner: dict, season: int,
                   is_coach_award: bool) -> tuple[int | None, int | None, str]:
    """(player_id, team_id, resolution) for one winner. Never creates a person."""
    name = (winner.get("display_name") or "").strip()
    espn_id = winner.get("espn_athlete_id")
    school = (winner.get("school") or "").strip()
    team_id = pid.resolve_team_id(conn, school) if school else None

    if not name:
        return None, team_id, "no name from the source"

    if espn_id:
        existing = pid.find_by_external_id(conn, "espn", str(espn_id))
        if existing is not None:
            pid.add_alias(conn, existing, name)
            return existing, team_id, "espn id"

    if is_coach_award:
        # Not a failed match: a coach is simply not in the player tables.
        return None, team_id, "coach award"
    if team_id is None:
        return None, None, "unknown team"

    matched, reason = pid.find_by_context(conn, name, season, team_id,
                                          winner.get("position"))
    if matched is None:
        pid.record_unresolved(conn, "player", "espn", "awards", name, reason, season)
        return None, team_id, reason
    pid.add_alias(conn, matched, name)
    if espn_id:
        # The contextual match is what earns the id its link, marked as such:
        # 'context' says this pairing came from a name, a team and a season,
        # not from a source that numbered the same person twice.
        pid.link_external_id(conn, matched, "espn", str(espn_id), "context")
    return matched, team_id, "context"


def load_awards(conn: sqlite3.Connection, path: str | Path) -> dict:
    """One season's file into the tables, replacing that season wholesale."""
    path = Path(path)
    payload = read_season(path)
    season = int(payload["season"])
    apply_schema(conn)
    cur = conn.cursor()
    # Replaced wholesale so a re-run after a correction cannot leave a stale
    # winner beside the right one.
    cur.execute("DELETE FROM player_awards WHERE season_year = ?", (season,))

    stats = {"awards": 0, "winners": 0, "linked": 0, "by_espn_id": 0,
             "coach_awards": 0, "unresolved": 0, "no_winner": 0}
    for award in payload.get("awards") or []:
        name = award.get("name")
        key = award_key(name)
        if not key:
            continue
        stats["awards"] += 1
        is_coach = bool(COACH_AWARD.search(name or ""))
        cur.execute(
            """INSERT INTO awards (award_key, name, description, espn_award_id,
                                   first_season, last_season)
                    VALUES (?, ?, ?, ?, ?, ?)
               ON CONFLICT(award_key) DO UPDATE SET
                    name = excluded.name,
                    description = COALESCE(excluded.description, awards.description),
                    espn_award_id = COALESCE(excluded.espn_award_id, awards.espn_award_id),
                    first_season = MIN(awards.first_season, excluded.first_season),
                    last_season = MAX(awards.last_season, excluded.last_season)""",
            (key, name, award.get("description"), award.get("espn_award_id"), season, season))

        winners = award.get("winners") or []
        if not winners:
            stats["no_winner"] += 1
            continue
        for winner in winners:
            player_id, team_id, resolution = resolve_winner(conn, winner, season, is_coach)
            stats["winners"] += 1
            if player_id is not None:
                stats["linked"] += 1
                if resolution == "espn id":
                    stats["by_espn_id"] += 1
            elif resolution == "coach award":
                stats["coach_awards"] += 1
            else:
                stats["unresolved"] += 1
            cur.execute(
                """INSERT OR REPLACE INTO player_awards
                       (season_year, award_key, winner_name, source_team, position,
                        team_id, player_id, espn_athlete_id, espn_team_id, resolution)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (season, key, (winner.get("display_name") or "").strip() or "(unnamed)",
                 winner.get("school"), winner.get("position"), team_id, player_id,
                 winner.get("espn_athlete_id"), winner.get("espn_team_id"), resolution))
    conn.commit()
    return stats


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("path", help="data/raw/awards/<year>.json.gz")
    ap.add_argument("--db", default="db/league.db")
    args = ap.parse_args()
    conn = sqlite3.connect(args.db)
    stats = load_awards(conn, args.path)
    print(f"{Path(args.path).name}: {stats['awards']} awards, {stats['winners']} winners, "
          f"{stats['linked']} linked to a person ({stats['by_espn_id']} by ESPN id), "
          f"{stats['coach_awards']} coach awards, {stats['unresolved']} unresolved, "
          f"{stats['no_winner']} awards with no winner recorded")


if __name__ == "__main__":
    main()
