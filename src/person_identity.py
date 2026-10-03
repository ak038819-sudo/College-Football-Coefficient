#!/usr/bin/env python3
"""Identity resolution for players and coaches.

The single rule this module exists to enforce: a person is never created or merged
because two names look alike. Resolution is tried in a fixed order, and anything
that does not clear a bar is recorded for review instead of guessed at.

  1. Source identifier. A CFBD athlete id already seen maps to its existing
     player_id. This is the only path that can match across teams and seasons,
     which is what makes transfers one person rather than two.
  2. Strong contextual match. With no source id, a row may attach to an existing
     person only on normalized name AND team AND season AND position, and only if
     exactly one person matches. Two candidates is a collision, not a tie-break.
  3. Nothing. The row is written to person_unresolved and no person is invented.

normalize_name exists for step 2 and for alias storage. It is deliberately NOT
an identity: 'Mike Williams' at two schools normalizes identically, which is why
step 2 also requires team, season and position, and refuses a double match.
"""
from __future__ import annotations

import datetime as dt
import re
import sqlite3
from pathlib import Path
from typing import Optional

SCHEMA = Path(__file__).resolve().parent.parent / "sql" / "person_tables.sql"

# Generational suffixes are decoration on a name, not part of the person: CFBD,
# box scores and school rosters disagree about whether to print them, and about
# whether to punctuate them ("Jr." / "Jr" / "JR").
SUFFIXES = {"jr", "sr", "ii", "iii", "iv", "v", "vi"}

CLASS_YEARS = {1: "FR", 2: "SO", 3: "JR", 4: "SR", 5: "GR"}


def apply_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA.read_text(encoding="utf-8"))


def normalize_name(name: str) -> str:
    """A comparison key: case, punctuation and generational suffixes removed.

    'D.J. Uiagalelei', 'DJ Uiagalelei' and 'D J Uiagalelei' share a key, as do
    'Jerome Gaillard Jr.' and 'Jerome Gaillard'. The key is for LOOKUP only --
    sharing one is never by itself a reason to treat two rows as one person.
    """
    cleaned = re.sub(r"[^a-z0-9 ]+", " ", (name or "").lower())
    parts = [p for p in cleaned.split() if p]
    while len(parts) > 1 and parts[-1] in SUFFIXES:
        parts.pop()
    return " ".join(parts)


def split_name(first: Optional[str], last: Optional[str], display: str) -> tuple[Optional[str], Optional[str]]:
    """Source-provided name parts, falling back to a split of the display name."""
    if first or last:
        return (first or None), (last or None)
    parts = (display or "").split()
    if len(parts) < 2:
        return None, None
    return parts[0], " ".join(parts[1:])


def class_year_label(value) -> Optional[str]:
    """CFBD sends class as 1-4 (sometimes 5); anything else is kept verbatim."""
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return CLASS_YEARS.get(value, str(value))
    text = str(value).strip()
    if text.isdigit():
        return CLASS_YEARS.get(int(text), text)
    return text or None


def _now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def record_unresolved(conn: sqlite3.Connection, entity: str, source: str, context: str,
                      display_name: str, reason: str, season_year: Optional[int] = None,
                      source_team: Optional[str] = None) -> None:
    conn.execute(
        "INSERT INTO person_unresolved (entity, source, context, display_name, season_year, "
        "source_team, reason, seen_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (entity, source, context, display_name, season_year, source_team, reason, _now()))


def add_alias(conn: sqlite3.Connection, player_id: int, raw_name: str) -> None:
    alias = normalize_name(raw_name)
    if not alias:
        return
    conn.execute("INSERT OR IGNORE INTO player_name_aliases (player_id, alias, raw_name) "
                 "VALUES (?, ?, ?)", (player_id, alias, raw_name))


def find_by_external_id(conn: sqlite3.Connection, source: str, external_id: str) -> Optional[int]:
    row = conn.execute("SELECT player_id FROM player_external_ids WHERE source = ? AND external_id = ?",
                       (source, str(external_id))).fetchone()
    return row[0] if row else None


def find_by_context(conn: sqlite3.Connection, display_name: str, season_year: int,
                    team_id: int, position: Optional[str]) -> tuple[Optional[int], str]:
    """Name + team + season + position, requiring exactly one candidate.

    Returns (player_id, reason). A second candidate returns (None, 'name collision'):
    two same-name players on one roster must stay two people until something
    stronger than a name separates them.
    """
    alias = normalize_name(display_name)
    if not alias:
        return None, "empty name"
    candidates = [r[0] for r in conn.execute(
        "SELECT DISTINCT s.player_id FROM player_team_seasons s "
        "JOIN player_name_aliases a ON a.player_id = s.player_id "
        "WHERE a.alias = ? AND s.season_year = ? AND s.team_id = ? "
        "AND (? IS NULL OR s.position IS NULL OR s.position = ?)",
        (alias, season_year, team_id, position, position))]
    if len(candidates) == 1:
        return candidates[0], "context"
    if len(candidates) > 1:
        return None, "name collision"
    return None, "no match"


def create_player(conn: sqlite3.Connection, display_name: str, first_name: Optional[str] = None,
                  last_name: Optional[str] = None, position: Optional[str] = None,
                  hometown: Optional[str] = None, home_state: Optional[str] = None,
                  height=None, weight=None) -> int:
    now = _now()
    cur = conn.execute(
        "INSERT INTO players (display_name, first_name, last_name, primary_position, hometown, "
        "home_state, height, weight, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (display_name, first_name, last_name, position, hometown, home_state, height, weight, now, now))
    player_id = int(cur.lastrowid)
    add_alias(conn, player_id, display_name)
    return player_id


def link_external_id(conn: sqlite3.Connection, player_id: int, source: str, external_id: str,
                     confidence: str, is_primary: bool = True) -> None:
    conn.execute(
        "INSERT OR IGNORE INTO player_external_ids (player_id, source, external_id, confidence, is_primary) "
        "VALUES (?, ?, ?, ?, ?)", (player_id, source, str(external_id), confidence, int(is_primary)))


def resolve_player(conn: sqlite3.Connection, source: str, display_name: str,
                   external_id=None, season_year: Optional[int] = None,
                   team_id: Optional[int] = None, position: Optional[str] = None,
                   context: str = "roster", create: bool = True,
                   **profile) -> tuple[Optional[int], str]:
    """One person for a source row, or (None, reason) with the row recorded for review.

    `create=False` is the box-score case: an archived line may only LINK to a person
    who already exists, never bring one into being from a name.
    """
    if external_id not in (None, ""):
        existing = find_by_external_id(conn, source, str(external_id))
        if existing is not None:
            add_alias(conn, existing, display_name)
            return existing, "external id"
        if not create:
            record_unresolved(conn, "player", source, context, display_name,
                              "unknown external id", season_year)
            return None, "unknown external id"
        first, last = split_name(profile.get("first_name"), profile.get("last_name"), display_name)
        player_id = create_player(conn, display_name, first, last, position,
                                  profile.get("hometown"), profile.get("home_state"),
                                  profile.get("height"), profile.get("weight"))
        link_external_id(conn, player_id, source, str(external_id), "id")
        return player_id, "created"

    if season_year is not None and team_id is not None:
        matched, reason = find_by_context(conn, display_name, season_year, team_id, position)
        if matched is not None:
            add_alias(conn, matched, display_name)
            return matched, reason
        if reason == "name collision" or not create:
            record_unresolved(conn, "player", source, context, display_name, reason, season_year)
            return None, reason

    if not create:
        record_unresolved(conn, "player", source, context, display_name, "no source id", season_year)
        return None, "no source id"

    # No source id and no existing contextual match: a new person with no external
    # identity. It is recorded as such so a later ID-bearing feed can be checked
    # against it rather than quietly creating a duplicate.
    first, last = split_name(profile.get("first_name"), profile.get("last_name"), display_name)
    player_id = create_player(conn, display_name, first, last, position,
                              profile.get("hometown"), profile.get("home_state"),
                              profile.get("height"), profile.get("weight"))
    record_unresolved(conn, "player", source, context, display_name, "created without source id",
                      season_year)
    return player_id, "created without source id"


def resolve_team_id(conn: sqlite3.Connection, raw_name: str) -> Optional[int]:
    """Canonical team_id for a source school string, through the project's aliases.

    None rather than an exception: roster and coaching feeds legitimately name
    schools this FBS-only database does not carry, and an opponent outside the
    league is not a data error.
    """
    name = (raw_name or "").strip()
    if not name:
        return None
    row = conn.execute("SELECT team_id FROM teams WHERE team_name = ?", (name,)).fetchone()
    if row:
        return int(row[0])
    row = conn.execute(
        "SELECT t.team_id FROM team_aliases a JOIN teams t ON t.team_name = a.team_name "
        "WHERE TRIM(a.alias) = TRIM(?) LIMIT 1", (name,)).fetchone()
    return int(row[0]) if row else None


def refresh_latest_seasons(conn: sqlite3.Connection) -> None:
    """Derived fields, recomputed from the season rows rather than tracked on write."""
    conn.execute("UPDATE players SET latest_season = ("
                 "SELECT MAX(season_year) FROM player_team_seasons s WHERE s.player_id = players.player_id)")
    conn.execute("UPDATE players SET primary_position = COALESCE(("
                 "SELECT s.position FROM player_team_seasons s WHERE s.player_id = players.player_id "
                 "AND s.position IS NOT NULL ORDER BY s.season_year DESC LIMIT 1), primary_position)")
    conn.execute("UPDATE coaches SET latest_season = ("
                 "SELECT MAX(season_year) FROM coach_tenures t WHERE t.coach_id = coaches.coach_id)")
